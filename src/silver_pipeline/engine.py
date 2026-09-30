"""
Engine genérica dirigida por contrato.

Um único código, sem regra hard-coded por fonte. Ele:
  1. lê a fonte bronze conforme `source` do contrato (adapter por formato);
  2. aplica as transformações declaradas (`transform`) coluna a coluna;
  3. faz cast para o tipo declarado;
  4. separa em VÁLIDO x QUARENTENA pelas regras hard (PK/nullable);
  5. deduplica pela chave declarada;
  6. aplica regras soft com política `on_fail` (quarantine/null/warn);
  7. escreve Silver + Silver_quarantine (Delta se disponível, senão Parquet).

Trocar a stack de escrita (Delta/Parquet) ou a forma de aplicar as funções
puras (UDF -> pandas_udf -> expressão nativa) não muda o contrato nem os
testes das funções puras.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from silver_pipeline import quality
from silver_pipeline.contract import Column, DataContract, OnFail, Transform

# --- Reason codes para quarentena ------------------------------------------
Q_NULL_PK = "null_primary_key"
Q_NOT_NULLABLE = "not_nullable_violation"
Q_RULE = "rule_violation"

# --- UDFs que embrulham as funções puras testadas --------------------------
_udf_clean_numeric = F.udf(quality.clean_numeric, T.DoubleType())
_udf_normalize_cpf = F.udf(quality.normalize_cpf, T.StringType())
_udf_valid_cpf = F.udf(quality.validate_cpf, T.BooleanType())
_udf_valid_email = F.udf(quality.is_valid_email, T.BooleanType())


def _udf_normalize_score(score_map):
    return F.udf(lambda v: quality.normalize_score(v, score_map), T.DoubleType())


@dataclass
class PipelineResult:
    name: str
    valid: DataFrame
    quarantine: DataFrame


def _parse_type(type_str: str) -> T.DataType:
    """Converte o tipo declarado no contrato para um DataType do Spark."""
    s = type_str.strip().lower()
    simple = {
        "string": T.StringType(),
        "long": T.LongType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "double": T.DoubleType(),
        "float": T.FloatType(),
        "boolean": T.BooleanType(),
        "bool": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
    }
    if s in simple:
        return simple[s]
    if s.startswith("decimal"):
        # decimal(p,s)
        inside = s[s.find("(") + 1 : s.find(")")]
        p, sc = (int(x) for x in inside.split(","))
        return T.DecimalType(p, sc)
    raise ValueError(f"tipo não suportado no contrato: {type_str}")


class ContractEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ----- 1. Leitura da fonte (adapter por formato) -----------------------
    def read_source(self, contract: DataContract) -> DataFrame:
        src = contract.source
        reader = self.spark.read
        if src.format == "json":
            # JSON linha-a-linha; lê tudo como string p/ controlar o cast na engine
            return reader.options(**src.options).json(src.path)
        if src.format == "csv":
            opts = {"header": "true", "inferSchema": "false"}
            opts.update({k: str(v) for k, v in src.options.items()})
            return reader.options(**opts).csv(src.path)
        raise ValueError(f"formato de fonte não suportado: {src.format}")

    # ----- 2. Transformações declaradas ------------------------------------
    def _apply_transform(self, df: DataFrame, col: Column) -> DataFrame:
        name = col.name
        if col.transform is None:
            return df
        if col.transform == Transform.clean_numeric:
            return df.withColumn(name, _udf_clean_numeric(F.col(name)))
        if col.transform == Transform.normalize_cpf:
            return df.withColumn(name, _udf_normalize_cpf(F.col(name)))
        if col.transform == Transform.normalize_score:
            return df.withColumn(name, _udf_normalize_score(col.score_map)(F.col(name)))
        if col.transform == Transform.parse_timestamp:
            if not col.formats:
                raise ValueError(f"coluna '{name}' usa parse_timestamp sem 'formats'")
            # tenta cada formato declarado, na ordem; primeiro que casar vence
            attempts = [F.to_timestamp(F.col(name), fmt) for fmt in col.formats]
            return df.withColumn(name, F.coalesce(*attempts))
        return df

    # ----- 3. Cast para o tipo declarado -----------------------------------
    def _cast_types(self, df: DataFrame, contract: DataContract) -> DataFrame:
        for col in contract.columns:
            # timestamp já foi resolvido no parse; evita recast destrutivo
            if col.transform == Transform.parse_timestamp:
                continue
            df = df.withColumn(col.name, F.col(col.name).cast(_parse_type(col.type)))
        return df

    # ----- 4. Regras hard: PK nula e nullable=false ------------------------
    def _split_hard(self, df: DataFrame, contract: DataContract) -> tuple[DataFrame, DataFrame]:
        reason = F.lit(None).cast("string")
        for pk in contract.primary_key:
            reason = F.when(F.col(pk).isNull(), F.lit(f"{Q_NULL_PK}:{pk}")).otherwise(reason)
        for col in contract.columns:
            if not col.nullable:
                reason = F.when(
                    reason.isNull() & F.col(col.name).isNull(),
                    F.lit(f"{Q_NOT_NULLABLE}:{col.name}"),
                ).otherwise(reason)
        tagged = df.withColumn("_quarantine_reason", reason)
        valid = tagged.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        quarantine = tagged.filter(F.col("_quarantine_reason").isNotNull())
        return valid, quarantine

    # ----- 5. Deduplicação pela chave declarada ----------------------------
    def _dedup(self, df: DataFrame, contract: DataContract) -> DataFrame:
        if not contract.dedup:
            return df
        d = contract.dedup
        order_col = d.order_by or d.keys[0]
        direction = F.col(order_col).desc() if d.keep == "last" else F.col(order_col).asc()
        w = Window.partitionBy(*d.keys).orderBy(direction)
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    # ----- 6. Regras soft com política on_fail -----------------------------
    def _apply_soft_rules(
        self, df: DataFrame, contract: DataContract
    ) -> tuple[DataFrame, DataFrame]:
        df = df.withColumn("_dq_flags", F.array().cast("array<string>"))
        soft_quarantine_reason = F.lit(None).cast("string")

        for col in contract.columns:
            r = col.rules
            checks: list[tuple[str, SparkColumn]] = []
            # cada check devolve TRUE quando a linha REPROVA
            if r.regex is not None:
                checks.append((f"regex:{col.name}",
                               F.col(col.name).isNotNull() & ~F.col(col.name).rlike(r.regex)))
            if r.valid_cpf:
                checks.append((f"invalid_cpf:{col.name}",
                               F.col(col.name).isNotNull() & ~_udf_valid_cpf(F.col(col.name))))
            if r.valid_email:
                checks.append((f"invalid_email:{col.name}",
                               F.col(col.name).isNotNull() & ~_udf_valid_email(F.col(col.name))))
            if r.min is not None:
                checks.append((f"below_min:{col.name}",
                               F.col(col.name).isNotNull() & (F.col(col.name) < F.lit(r.min))))
            if r.max is not None:
                checks.append((f"above_max:{col.name}",
                               F.col(col.name).isNotNull() & (F.col(col.name) > F.lit(r.max))))
            if r.unique:
                w = Window.partitionBy(col.name)
                dup = (F.col(col.name).isNotNull()) & (F.count("*").over(w) > 1)
                checks.append((f"duplicate:{col.name}", dup))

            for flag_name, failed in checks:
                if r.on_fail == OnFail.nullify:
                    # flag antes de anular: `failed` exige o valor não-nulo
                    df = df.withColumn(
                        "_dq_flags",
                        F.when(failed, F.array_union("_dq_flags", F.array(F.lit(flag_name))))
                        .otherwise(F.col("_dq_flags")),
                    )
                    df = df.withColumn(
                        col.name,
                        F.when(failed, F.lit(None)).otherwise(F.col(col.name)),
                    )
                elif r.on_fail == OnFail.warn:
                    df = df.withColumn(
                        "_dq_flags",
                        F.when(failed, F.array_union("_dq_flags", F.array(F.lit(flag_name))))
                        .otherwise(F.col("_dq_flags")),
                    )
                elif r.on_fail == OnFail.quarantine:
                    soft_quarantine_reason = F.when(
                        soft_quarantine_reason.isNull() & failed,
                        F.lit(f"{Q_RULE}:{flag_name}"),
                    ).otherwise(soft_quarantine_reason)

        tagged = df.withColumn("_quarantine_reason", soft_quarantine_reason)
        valid = tagged.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        quarantine = tagged.filter(F.col("_quarantine_reason").isNotNull())
        return valid, quarantine

    # ----- 7. Colunas derivadas + seleção final ----------------------------
    def _derive_and_select(self, df: DataFrame, contract: DataContract) -> DataFrame:
        for d in contract.derived:
            if d.expr == "to_date":
                df = df.withColumn(d.name, F.to_date(F.col(d.from_col)))
        select_cols = [c.name for c in contract.columns]
        select_cols += [d.name for d in contract.derived]
        select_cols += ["_dq_flags"]
        return df.select(*select_cols)

    # ----- Orquestração de um dataset --------------------------------------
    # O job chama process(); o caminho declarativo (dlt_pipeline.py) chama
    # prepare() e refine() separados, para aplicar as regras hard como
    # expectations entre as duas etapas. Nenhuma delas escreve nada.
    def process(self, contract: DataContract) -> PipelineResult:
        return self.refine(contract, self.prepare(contract))

    def prepare(self, contract: DataContract, df: DataFrame | None = None) -> DataFrame:
        """Etapas 1-3: leitura (se `df` não vier), colunas faltantes, transform e cast."""
        if df is None:
            df = self.read_source(contract)
        # garante que todas as colunas do contrato existem (fonte pode não trazer alguma)
        for col in contract.columns:
            if col.name not in df.columns:
                df = df.withColumn(col.name, F.lit(None).cast("string"))
        for col in contract.columns:
            df = self._apply_transform(df, col)
        return self._cast_types(df, contract)

    def refine(self, contract: DataContract, typed: DataFrame) -> PipelineResult:
        """Etapas 4-7 sobre o DataFrame tipado: hard, dedup, soft, derivadas."""
        valid, quarantine_hard = self._split_hard(typed, contract)
        valid = self._dedup(valid, contract)
        valid, quarantine_soft = self._apply_soft_rules(valid, contract)

        # alinhar schemas de quarentena (hard não tem _dq_flags)
        quarantine_hard = quarantine_hard.withColumn(
            "_dq_flags", F.array().cast("array<string>")
        )
        # ordena colunas iguais nos dois lados antes do union
        common = [c for c in quarantine_hard.columns if c in quarantine_soft.columns]
        quarantine = quarantine_hard.select(*common).unionByName(
            quarantine_soft.select(*common)
        )

        valid = self._derive_and_select(valid, contract)
        return PipelineResult(name=contract.name, valid=valid, quarantine=quarantine)


# --- Regras do contrato como expectations (SQL) -----------------------------
def hard_expectations(contract: DataContract) -> dict[str, str]:
    """PK e nullable:false -> {nome: constraint SQL}, mesmos critérios de _split_hard."""
    exps = {f"{Q_NULL_PK}_{pk}": f"`{pk}` IS NOT NULL" for pk in contract.primary_key}
    for col in contract.columns:
        if not col.nullable and col.name not in contract.primary_key:
            exps[f"{Q_NOT_NULLABLE}_{col.name}"] = f"`{col.name}` IS NOT NULL"
    return exps


def soft_rule_flags(contract: DataContract) -> list[tuple[str, OnFail]]:
    """Flags que _apply_soft_rules grava em _dq_flags (mesma convenção de nomes)."""
    flags: list[tuple[str, OnFail]] = []
    for col in contract.columns:
        r = col.rules
        names = []
        if r.regex is not None:
            names.append(f"regex:{col.name}")
        if r.valid_cpf:
            names.append(f"invalid_cpf:{col.name}")
        if r.valid_email:
            names.append(f"invalid_email:{col.name}")
        if r.min is not None:
            names.append(f"below_min:{col.name}")
        if r.max is not None:
            names.append(f"above_max:{col.name}")
        if r.unique:
            names.append(f"duplicate:{col.name}")
        flags += [(n, r.on_fail) for n in names]
    return flags


def soft_expectations(contract: DataContract) -> dict[str, str]:
    """Regras soft warn/nullify -> expectations sobre `_dq_flags`.

    A checagem em si (UDFs de CPF/e-mail, regex, min/max, unique) continua na
    engine; a expectation só lê a flag que ela gravou. Regras com
    on_fail=quarantine não aparecem: essas linhas já saíram da Silver.
    """
    return {
        flag.replace(":", "_"): f"NOT array_contains(_dq_flags, '{flag}')"
        for flag, on_fail in soft_rule_flags(contract)
        if on_fail != OnFail.quarantine
    }


# --- Escrita: Delta se disponível, senão Parquet ----------------------------
def _on_databricks(spark: SparkSession) -> bool:
    """True se rodando em um cluster Databricks (Delta é nativo)."""
    import os

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None) is not None
    except Exception:
        return False


def _delta_available(df: DataFrame) -> bool:
    """Delta é nativo no Databricks; local, só se a extensão estiver ativa."""
    if _on_databricks(df.sparkSession):
        return True
    try:
        ext = df.sparkSession.conf.get("spark.sql.extensions", "") or ""
        return "DeltaSparkSessionExtension" in ext
    except Exception:
        return False


def _is_table_target(output: str) -> bool:
    """True se `output` é um destino Unity Catalog 'catalog.schema'
    (sem separador de path), e não um diretório de arquivos."""
    return "/" not in output and "\\" not in output and "." in output


def write_output(result: PipelineResult, contract: DataContract, base: str) -> str:
    """Escreve Silver + quarentena.

    - base como 'catalog.schema' (ex.: main.silver) -> tabelas gerenciadas
      Delta no Unity Catalog via saveAsTable (caminho Databricks).
    - base como diretório (ex.: data/silver) -> arquivos Delta/Parquet por
      path (caminho local).
    """
    if _is_table_target(base):
        result.valid.sparkSession.sql(f"CREATE SCHEMA IF NOT EXISTS {base}")
        for df, suffix in ((result.valid, ""), (result.quarantine, "_quarantine")):
            writer = (
                df.write.mode("overwrite")
                .format("delta")
                .option("overwriteSchema", "true")
            )
            if suffix == "" and contract.partition_by:
                writer = writer.partitionBy(*contract.partition_by)
            writer.saveAsTable(f"{base}.{contract.name}{suffix}")
        return "delta (managed table)"

    fmt = "delta" if _delta_available(result.valid) else "parquet"
    silver = f"{base}/{contract.name}"
    writer = result.valid.write.mode("overwrite").format(fmt)
    if contract.partition_by:
        writer = writer.partitionBy(*contract.partition_by)
    writer.save(silver)
    result.quarantine.write.mode("overwrite").format(fmt).save(f"{silver}_quarantine")
    return fmt
