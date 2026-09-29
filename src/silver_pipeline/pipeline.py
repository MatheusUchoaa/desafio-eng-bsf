"""
Ponto de entrada da pipeline (entry point da wheel: `silver-pipeline`).

Roda a engine para cada contrato. Nenhuma lógica específica de fonte aqui.
Por padrão usa os contratos EMBUTIDOS no pacote (code + contratos versionados
juntos como um único artefato). `--contracts <glob>` sobrescreve.
"""
from __future__ import annotations

import argparse
import glob
import os
from importlib.resources import files

from pyspark.sql import SparkSession

from silver_pipeline.contract import DataContract
from silver_pipeline.engine import ContractEngine, write_output


def packaged_contracts() -> list[str]:
    """Caminhos dos contratos YAML embutidos no pacote."""
    root = files("silver_pipeline.contracts")
    return sorted(str(root / p.name) for p in root.iterdir() if p.name.endswith(".yaml"))


def build_spark(app_name: str = "silver-pipeline") -> SparkSession:
    databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    builder = SparkSession.builder.appName(app_name).config(
        "spark.ui.showConsoleProgress", "false"
    )
    if not databricks:
        # local: define master. No Databricks a sessão já existe e é reutilizada.
        builder = builder.master(os.environ.get("SPARK_MASTER", "local[*]"))

    # Delta local é opt-in (baixa JARs, requer rede). No Databricks é nativo.
    if not databricks and os.environ.get("ENABLE_DELTA") == "1":
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore

            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            return spark
        except Exception:
            pass

    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # aplica na sessão viva
    return spark


def run(
    contracts_glob: str | None = None,
    output_base: str = "data/silver",
    source_base: str | None = None,
) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    engine = ContractEngine(spark)

    paths = sorted(glob.glob(contracts_glob)) if contracts_glob else packaged_contracts()
    if not paths:
        raise SystemExit("nenhum contrato encontrado")

    for path in paths:
        contract = DataContract.from_yaml(path)  # valida o contrato no load
        if source_base:
            # reancora a fonte num Volume/diretório, mantendo o nome do arquivo
            fname = os.path.basename(contract.source.path)
            contract.source.path = f"{source_base.rstrip('/')}/{fname}"
        result = engine.process(contract)
        v = result.valid.count()
        q = result.quarantine.count()
        dest = write_output(result, contract, output_base)
        print(f"[{contract.name}] validas={v} quarentena={q} destino={dest}")

    # Em notebook/cluster Databricks a sessão é compartilhada — não encerrar.
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="silver-pipeline")
    ap.add_argument("--contracts", default=None,
                    help="glob de contratos (default: embutidos no pacote)")
    ap.add_argument("--output", default="data/silver",
                    help="diretório (local) ou catalog.schema (Unity Catalog)")
    ap.add_argument("--source-base", default=None,
                    help="Volume/diretório da Bronze (ex.: /Volumes/main/bronze/payments)")
    args = ap.parse_args(argv)
    run(args.contracts, args.output, args.source_base)


if __name__ == "__main__":
    main()
