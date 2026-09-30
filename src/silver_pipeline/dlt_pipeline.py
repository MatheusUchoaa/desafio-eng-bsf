"""
Caminho declarativo: Lakeflow Spark Declarative Pipelines (antigo DLT).

Paralelo ao job (`pipeline.py`, engine imperativa): lê os MESMOS contratos
embutidos na wheel e chama as MESMAS etapas da engine. O que muda é quem
orquestra e quem aplica as regras:

  <nome>_bronze      view     leitura da fonte do contrato no Volume
  <nome>_typed       view     transform + cast; PK/nullable:false -> expect_all_or_drop
  <nome>             MV       dedup + regras soft + derivadas; soft -> expect_all (warn)
  <nome>_quarantine  MV       linhas rejeitadas, com _quarantine_reason (igual ao job)

Este arquivo só roda dentro de um pipeline (o módulo `pyspark.pipelines` é do
runtime Databricks); a wheel com engine + contratos vem de environment.dependencies.
Parâmetro lido da configuração do pipeline: `silver_pipeline.bronze_volume`.
"""
from __future__ import annotations

import os

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from silver_pipeline.contract import DataContract
from silver_pipeline.engine import ContractEngine, hard_expectations, soft_expectations
from silver_pipeline.pipeline import packaged_contracts

spark = SparkSession.getActiveSession()
engine = ContractEngine(spark)
bronze_volume = spark.conf.get("silver_pipeline.bronze_volume")


def _expect(decorator, expectations: dict[str, str]):
    """Aplica o decorator de expectations só se o contrato declarar alguma regra."""
    return decorator(expectations) if expectations else (lambda f: f)


def _register(contract: DataContract) -> None:
    name = contract.name
    bronze, typed = f"{name}_bronze", f"{name}_typed"

    @dp.temporary_view(name=bronze, comment=f"Bronze bruta ({contract.source.path})")
    def _bronze():
        return engine.read_source(contract)

    @dp.temporary_view(name=typed, comment="transform + cast do contrato; regras hard")
    @_expect(dp.expect_all_or_drop, hard_expectations(contract))
    def _typed():
        return engine.prepare(contract, spark.read.table(bronze))

    @dp.materialized_view(
        name=name,
        comment=contract.description.strip(),
        partition_cols=contract.partition_by or None,
    )
    @_expect(dp.expect_all, soft_expectations(contract))
    def _silver():
        return engine.refine(contract, spark.read.table(typed)).valid

    @dp.materialized_view(
        name=f"{name}_quarantine",
        comment="Linhas rejeitadas pelo contrato, com _quarantine_reason",
    )
    def _quarantine():
        # recalcula a partir da Bronze: a view _typed já teve as linhas hard descartadas
        typed_all = engine.prepare(contract, spark.read.table(bronze))
        return engine.refine(contract, typed_all).quarantine


for _path in packaged_contracts():
    _contract = DataContract.from_yaml(_path)  # valida o contrato no load
    # reancora a fonte no Volume, mantendo o nome do arquivo (igual a pipeline.run)
    _fname = os.path.basename(_contract.source.path)
    _contract.source.path = f"{bronze_volume.rstrip('/')}/{_fname}"
    _register(_contract)
