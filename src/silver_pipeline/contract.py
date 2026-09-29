"""
Meta-contrato: o modelo que define o que um Data Contract PODE declarar.

O contrato é a única coisa que o time de Produto/Dev preenche. A engine
(src/engine.py) lê este modelo e aplica tudo automaticamente. Validar o
próprio contrato com Pydantic garante que um contrato malformado falhe
cedo (no load), não em runtime no meio do Spark.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class OnFail(str, Enum):
    """O que fazer quando uma regra de qualidade (soft) reprova numa linha."""
    quarantine = "quarantine"  # move a linha inteira para a tabela de quarentena
    nullify = "nullify"        # anula só o valor da coluna e mantém a linha
    warn = "warn"              # mantém o valor, apenas marca em _dq_flags


class Transform(str, Enum):
    """Transformações declarativas suportadas (mapeiam para funções puras)."""
    clean_numeric = "clean_numeric"
    parse_timestamp = "parse_timestamp"
    normalize_cpf = "normalize_cpf"
    normalize_score = "normalize_score"


class ColumnRules(BaseModel):
    model_config = ConfigDict(extra="forbid")
    unique: bool = False
    regex: str | None = None
    valid_cpf: bool = False
    valid_email: bool = False
    min: float | None = None
    max: float | None = None
    on_fail: OnFail = OnFail.warn


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    type: str  # tipo Spark: string, long, int, double, boolean, timestamp, date, decimal(p,s)
    nullable: bool = True
    description: str = ""
    transform: Transform | None = None
    formats: list[str] = Field(default_factory=list)      # p/ parse_timestamp
    score_map: dict[str, float] = Field(default_factory=dict)  # p/ normalize_score
    rules: ColumnRules = Field(default_factory=ColumnRules)


class SourceCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format: Literal["json", "csv"]
    path: str
    options: dict[str, Any] = Field(default_factory=dict)


class DedupCfg(BaseModel):
    model_config = ConfigDict(extra="forbid")
    keys: list[str]
    order_by: str | None = None
    keep: Literal["first", "last"] = "last"


class DerivedCol(BaseModel):
    """Coluna derivada de outra (ex.: coluna de partição por data)."""
    model_config = ConfigDict(extra="forbid")
    name: str
    from_col: str = Field(alias="from")
    expr: Literal["to_date"] = "to_date"


class DataContract(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: str
    name: str
    description: str = ""
    source: SourceCfg
    primary_key: list[str] = Field(default_factory=list)
    partition_by: list[str] = Field(default_factory=list)
    dedup: DedupCfg | None = None
    columns: list[Column]
    derived: list[DerivedCol] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_references(self) -> DataContract:
        col_names = {c.name for c in self.columns}
        derived_names = {d.name for d in self.derived}
        known = col_names | derived_names

        for pk in self.primary_key:
            if pk not in col_names:
                raise ValueError(f"primary_key '{pk}' não existe em columns")
        for p in self.partition_by:
            if p not in known:
                raise ValueError(f"partition_by '{p}' não existe em columns/derived")
        if self.dedup:
            for k in self.dedup.keys:
                if k not in col_names:
                    raise ValueError(f"dedup.keys '{k}' não existe em columns")
            if self.dedup.order_by and self.dedup.order_by not in col_names:
                raise ValueError(f"dedup.order_by '{self.dedup.order_by}' não existe em columns")
        for d in self.derived:
            if d.from_col not in col_names:
                raise ValueError(f"derived '{d.name}' referencia coluna inexistente '{d.from_col}'")
        return self

    @classmethod
    def from_yaml(cls, path: str) -> DataContract:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
