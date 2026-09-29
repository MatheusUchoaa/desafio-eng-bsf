"""
Testes de integração da engine, rodando Spark local sobre os dados reais.
Cobrem os edge cases exigidos: timestamp multi-formato, amount malformado,
PK nula, duplicata e score em escala diferente.

Pulam automaticamente se pyspark não estiver instalado.
"""
import os
from importlib.resources import files

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from silver_pipeline.contract import DataContract  # noqa: E402
from silver_pipeline.engine import ContractEngine, _is_table_target  # noqa: E402

# raiz do repo (tests/ fica na raiz) -> onde estão os dados de exemplo
ROOT = os.path.join(os.path.dirname(__file__), "..")
BRONZE = os.path.join(ROOT, "data", "bronze")


def test_target_detection():
    assert _is_table_target("main.silver") is True
    assert _is_table_target("data/silver") is False
    assert _is_table_target("/Volumes/main/bronze") is False


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _process(spark, contract_file):
    path = str(files("silver_pipeline.contracts") / contract_file)
    contract = DataContract.from_yaml(path)
    # reancora a fonte nos dados de exemplo do repo
    contract.source.path = os.path.join(BRONZE, os.path.basename(contract.source.path))
    return ContractEngine(spark).process(contract)


def test_transactions_edge_cases(spark):
    res = _process(spark, "transactions.yaml")
    valid = {r["transaction_id"]: r for r in res.valid.collect()}
    q_reasons = [r["_quarantine_reason"] for r in res.quarantine.collect()]

    assert any("null_primary_key" in r for r in q_reasons)
    assert any("not_nullable_violation:transaction_amount" in r for r in q_reasons)
    assert "550e8400-e29b-41d4-a716-446655440007" not in valid

    # timestamp multi-formato: 440004 usa 'yyyy-MM-dd HH:mm:ss'
    assert valid["550e8400-e29b-41d4-a716-446655440004"]["transaction_date"] is not None

    # duplicata 440001: keep=last mantém o mais recente (customer 1006)
    assert valid["550e8400-e29b-41d4-a716-446655440001"]["customer_id"] == 1006

    assert "transaction_date_ref" in res.valid.columns


def test_customers_no_customer_dropped(spark):
    res = _process(spark, "customers.yaml")
    rows = {r["customer_id"]: r for r in res.valid.collect()}
    assert set(rows) == set(range(1001, 1011))  # nenhum cliente descartado
    assert any("duplicate:customer_email" in f for f in rows[1001]["_dq_flags"])
    assert any("invalid_email" in f for f in rows[1007]["_dq_flags"])


def test_fraud_score_scales(spark):
    res = _process(spark, "fraud_flags.yaml")
    rows = {r["transaction_id"]: r for r in res.valid.collect()}
    assert rows["550e8400-e29b-41d4-a716-446655440004"]["fraud_score"] == 0.5  # 'MEDIUM'
    assert rows["550e8400-e29b-41d4-a716-446655440003"]["fraud_score"] == 0.82
