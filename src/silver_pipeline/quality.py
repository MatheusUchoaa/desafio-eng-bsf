"""
Funções PURAS de qualidade de dados.

Sem dependência de Spark: recebem valores escalares e devolvem valores
escalares. Isso as torna trivialmente testáveis (pytest puro, sem JVM) e
reutilizáveis dentro do Spark via UDF (ver src/engine.py).

Para escala, cada UDF que embrulha estas funções pode ser trocada por uma
pandas_udf ou por uma expressão nativa equivalente sem alterar o contrato
nem os testes — a assinatura (entrada escalar -> saída escalar) é a mesma.
"""
from __future__ import annotations

import re
from collections.abc import Mapping


def clean_numeric(value: object) -> float | None:
    """Normaliza valores monetários vindos como string ou número.

    Trata: números nativos, strings com espaços, separador de milhar,
    vírgula decimal (padrão BR) e símbolos de moeda. Qualquer coisa que
    não seja parseável vira None (a linha é decidida depois pelo contrato:
    se a coluna é nullable=false, vai para quarentena).

    Exemplos:
        "1250.50" -> 1250.5
        "R$ 1.250,50" -> 1250.5   (milhar '.', decimal ',')
        850.0 -> 850.0
        "500" -> 500.0
        "INVALIDO" -> None
        "" / None -> None
    """
    if value is None:
        return None
    if isinstance(value, bool):  # bool é subclasse de int; rejeitar explicitamente
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if s == "":
        return None

    # mantém apenas dígitos, ponto, vírgula e sinal negativo
    s = re.sub(r"[^\d,.\-]", "", s)
    if s in ("", "-", ".", ","):
        return None

    has_dot = "." in s
    has_comma = "," in s
    if has_dot and has_comma:
        # BR: '.' é milhar, ',' é decimal -> remove milhar, troca vírgula por ponto
        s = s.replace(".", "").replace(",", ".")
    elif has_comma:
        # só vírgula -> assume decimal
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def normalize_cpf(raw: object) -> str | None:
    """Remove máscara e devolve os 11 dígitos, ou None se não tiver 11 dígitos.

    Não valida dígito verificador (isso é validate_cpf). Só padroniza o
    formato, resolvendo a inconsistência entre '123.456.789-10' e
    '12345678901'.
    """
    if raw is None:
        return None
    digits = re.sub(r"\D", "", str(raw))
    return digits if len(digits) == 11 else None


def validate_cpf(raw: object) -> bool:
    """Valida CPF por dígito verificador (algoritmo oficial da Receita).

    Aceita com ou sem máscara. Rejeita: tamanho != 11, todos os dígitos
    iguais (000..., 111...), e dígitos verificadores incorretos.
    """
    if raw is None:
        return False
    d = re.sub(r"\D", "", str(raw))
    if len(d) != 11 or d == d[0] * 11:
        return False
    for length in (9, 10):
        total = sum(int(d[i]) * ((length + 1) - i) for i in range(length))
        check = (total * 10) % 11
        if check == 10:
            check = 0
        if check != int(d[length]):
            return False
    return True


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def is_valid_email(value: object) -> bool:
    """Validação estrutural de e-mail (exige local@dominio.tld)."""
    if value is None:
        return False
    s = str(value).strip()
    if s == "":
        return False
    return bool(_EMAIL_RE.match(s))


def normalize_score(value: object, score_map: Mapping[str, float] | None = None) -> float | None:
    """Normaliza fraud_score para float em [0, 1].

    Resolve o desafio 'scores em escalas diferentes':
      - float já em [0,1] passa direto
      - categórico textual (LOW/MEDIUM/HIGH) é mapeado pelo score_map do contrato
      - string numérica é convertida
    Valor fora de [0,1] é fixado (clamp) nas bordas; irreconhecível -> None.
    """
    score_map = score_map or {}
    if value is None:
        return None

    v: float | None = None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
    else:
        s = str(value).strip()
        if s == "":
            return None
        try:
            v = float(s)
        except ValueError:
            # busca case-insensitive no mapa categórico
            upper = {k.upper(): val for k, val in score_map.items()}
            if s.upper() in upper:
                v = float(upper[s.upper()])
            else:
                return None

    if v is None:
        return None
    # clamp para o range do contrato
    return max(0.0, min(1.0, v))
