# Pipeline Silver dirigida por Data Contract

Ingestão Bronze → Silver de uma plataforma de pagamentos, onde **uma engine
genérica lê o contrato de dados e aplica tipagem, validação, qualidade,
deduplicação e particionamento automaticamente**. O time de Produto/Dev só
preenche o YAML do contrato; nenhuma regra é escrita por fonte.

```
BRONZE (json/csv) ─▶ DATA CONTRACT (yaml) ─▶ ENGINE genérica ─▶ SILVER + QUARENTENA
  dados brutos        Produto preenche        um só código        limpo / rejeitado
```

Código e contratos são empacotados numa **wheel** e implantados via **Databricks
Asset Bundle** (`python_wheel_task`) — o artefato declarativo (code + contratos)
viaja versionado como uma unidade.

## Estrutura

```
src/silver_pipeline/
  contract.py          modelo Pydantic do contrato (valida o contrato no load)
  quality.py           funções puras: CPF, e-mail, timestamp, score, valor
  engine.py            lê contrato → transforma → tipa → quarentena → dedup → regras → escreve
  pipeline.py          entry point `silver-pipeline` (main)
  contracts/*.yaml     os 3 contratos, EMBUTIDOS no pacote
tests/                 test_quality (puros) + test_engine (Spark)
resources/*.job.yml    job do Databricks Asset Bundle
databricks.yml         bundle (variáveis, artifacts=wheel, targets)
data/bronze/*          dados de exemplo (para rodar local e/ou subir a um Volume)
```

Fluxo da engine, por dataset: leitura (adapter por formato) → transformações
declaradas → cast de tipo → **regras hard** (PK nula / `nullable:false` → quarentena)
→ dedup pela chave → **regras soft** (política `on_fail`) → derivadas → escrita.

## Como rodar

### Databricks (via Asset Bundle — caminho principal)

Pré-requisitos: Databricks CLI configurada, um Volume com os 3 arquivos Bronze e
permissão de escrita no schema Silver. Ajuste `host` em `databricks.yml` e as
variáveis `silver_schema` / `bronze_volume` (ou sobrescreva com `--var`).

```bash
databricks bundle validate
databricks bundle deploy -t dev          # builda a wheel e sobe o job
databricks bundle run silver_pipeline -t dev
```

O bundle constrói `dist/*.whl` (artifact `type: whl`), instala no cluster do job
e roda o entry point `silver-pipeline` com `--output ${silver_schema}` e
`--source-base ${bronze_volume}`. Saída: tabelas gerenciadas **Delta** no Unity
Catalog `⟨schema⟩.transactions_silver` (+ `_quarantine`), idem customers e fraud.

### Local (Spark + Java 17)

```bash
pip install -e ".[dev]"                   # pacote + pyspark/delta/pytest/ruff
pytest -q                                 # 31 testes (funções puras + engine)
silver-pipeline                           # usa contratos embutidos, grava data/silver/*
# ou: python -m silver_pipeline.pipeline --output data/silver
```

O **mesmo código** serve aos dois ambientes:
- `--output` com `catalog.schema` → tabelas gerenciadas Delta (`saveAsTable`); com um
  diretório → arquivos por path (Delta no cluster, Parquet local).
- `--source-base` reancora as fontes num Volume mantendo os nomes; sem ele usa os
  paths dos contratos (`data/bronze/...`).
- Delta é detectado pelo runtime Databricks (nativo), não por config frágil de sessão.
- No cluster a wheel torna `silver_pipeline` importável nos executors (UDFs), sem gambiarra.

Saída de referência (dados fornecidos):

```
[customers_silver]    validas=10 quarentena=0
[fraud_flags_silver]  validas=11 quarentena=0
[transactions_silver] validas=12 quarentena=2   (PK nula + amount 'INVALIDO'; 1 duplicata removida)
```

## Decisões técnicas

**Engine dirigida por contrato (não 3 scripts).** O enunciado pede abstrair a ingestão
*atrás do contrato*. Uma 4ª fonte não custa código — custa um YAML. O contrato é
validado por Pydantic no load: contrato malformado falha cedo, não em runtime.

**Wheel + contratos embutidos.** O artefato entregue é code + contratos juntos,
versionados (`version` no pyproject e em cada contrato). O `python_wheel_task` instala
a wheel no cluster; `pyspark`/`delta` são nativos do runtime, então a wheel só carrega
`pydantic` + `PyYAML`.

**Funções puras + UDF.** Regras (CPF, escala de score, valor) são funções puras,
testadas sem JVM (rápido, determinístico) e reusadas via UDF. Para escala, trocar por
`pandas_udf`/expressão nativa não muda contrato nem teste (assinatura escalar→escalar).

**Quarentena, não descarte.** Violação hard (PK nula, tipo obrigatório não-parseável)
vai para `⟨tabela⟩_quarantine` com `_quarantine_reason`. Permite auditoria e reprocesso.

**Política `on_fail` por regra soft** (`quarantine`/`nullify`/`warn`): o contrato decide
o rigor sem tocar na engine.

**Decisões aterradas nos dados reais (explorados antes de codar):**

- **CPF** — os 10 CPFs do arquivo **reprovam** o dígito verificador (dados fake). Por isso
  `valid_cpf` usa `on_fail: nullify`: anula o valor e sinaliza em `_dq_flags`, **sem
  descartar o cliente**. `quarantine` aqui zeraria a base.
- **E-mail duplicado** (1001 e 1006, clientes distintos) — dedup de clientes é por
  `customer_id`, **nunca por e-mail**, senão perderia um cliente legítimo. Duplicidade e
  e-mail malformado (`roberto@email`) são **sinalizados** em `_dq_flags`.
- **`transaction_id` duplicado** (`...440001`) — é a PK real: dedup `keep=last` por
  `transaction_date` mantém o evento mais recente e torna a carga idempotente.
- **Timestamp em 2 formatos** (`...Z` e `yyyy-MM-dd HH:mm:ss`) — `parse_timestamp` tenta a
  lista de formatos do contrato via `coalesce`.
- **`fraud_score` em escalas diferentes** (float 0–1 e `"MEDIUM"`) — `normalize_score`
  mapeia categórico→numérico via `score_map` e faz clamp em [0,1].
- **`transaction_amount`** string/número com `"INVALIDO"` — `clean_numeric` normaliza
  (milhar/decimal BR); não-parseável em coluna `nullable:false` → quarentena.

## Edge cases tratados

Timestamp malformado/multi-formato · valor não-numérico · PK nula · duplicata de PK ·
e-mail duplicado e malformado · CPF sem máscara e com dígito inválido · score categórico ·
campos nulos opcionais · fonte esparsa (fraud não cobre todas as transações → left join).
Data futura (`2025-03-15`) é preservada e observável via `transaction_date_ref`
(regra de range fica como próximo passo).

## Testes

`tests/test_quality.py` — 27 testes das transformações puras (parametrizados).
`tests/test_engine.py` — integração rodando Spark sobre os dados reais, cobrindo os edge
cases exigidos (pula se pyspark ausente) + detecção de destino. `ruff` no lint.

## Diferenciais incluídos

- **CI/CD** — `.github/workflows/ci.yml`: lint (ruff) + pytest + **build da wheel** a cada PR.
- **Databricks Asset Bundle** — `databricks.yml` + `resources/silver_pipeline.job.yml`:
  wheel como artifact, `python_wheel_task`, cluster UC, schedule; deploy reproduzível.

## Próximos passos para escala

1. **Silver → Gold**: a tabela enriquecida (transação × cliente × fraude) é Gold, não
   Silver — deixada fora de propósito. Reusa a mesma engine com um contrato Gold.
2. **`pandas_udf`/expressão nativa** no lugar das UDFs escalares para throughput.
3. **Cargas incrementais** (Auto Loader / merge Delta) em vez de overwrite.
4. **Contrato versionado + evolução de schema** com registro e checagem de compat. em CI.
5. **Expectativas nativas**: as regras mapeiam 1:1 para *expectations* de DLT/Spark
   Declarative Pipelines — a Silver pode virar declarativa reusando os mesmos contratos.
