"""Testes das transformações puras. Rodam sem Spark/JVM."""
import pytest

from silver_pipeline.quality import (
    clean_numeric,
    is_valid_email,
    normalize_cpf,
    normalize_score,
    validate_cpf,
)


class TestCleanNumeric:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("1250.50", 1250.5),
            (850.0, 850.0),
            ("500", 500.0),
            ("15000", 15000.0),
            ("R$ 1.250,50", 1250.5),   # milhar '.', decimal ','
            ("1.234,56", 1234.56),
            ("99,90", 99.9),           # só vírgula = decimal
        ],
    )
    def test_valores_validos(self, raw, expected):
        assert clean_numeric(raw) == expected

    @pytest.mark.parametrize("raw", ["INVALIDO", "", None, "abc", True, False])
    def test_valores_invalidos_viram_none(self, raw):
        assert clean_numeric(raw) is None


class TestCpf:
    def test_normalize_remove_mascara(self):
        assert normalize_cpf("123.456.789-10") == "12345678910"
        assert normalize_cpf("12345678901") == "12345678901"

    def test_normalize_tamanho_errado(self):
        assert normalize_cpf("123") is None
        assert normalize_cpf(None) is None

    def test_validate_cpf_valido(self):
        assert validate_cpf("111.444.777-35") is True
        assert validate_cpf("11144477735") is True

    def test_validate_cpf_invalido(self):
        assert validate_cpf("111.111.111-11") is False   # todos iguais
        assert validate_cpf("12345678901") is False       # sequência (linha 1005)
        assert validate_cpf("123.456.789-10") is False     # linha 1001
        assert validate_cpf(None) is False


class TestEmail:
    @pytest.mark.parametrize("raw", ["joao.silva@email.com", "a@b.co"])
    def test_validos(self, raw):
        assert is_valid_email(raw) is True

    @pytest.mark.parametrize("raw", ["roberto@email", "", None, "sem-arroba.com"])
    def test_invalidos(self, raw):
        assert is_valid_email(raw) is False


class TestNormalizeScore:
    smap = {"LOW": 0.1, "MEDIUM": 0.5, "HIGH": 0.9}

    def test_float_passa(self):
        assert normalize_score(0.15, self.smap) == 0.15
        assert normalize_score("0.82", self.smap) == 0.82

    def test_categorico_mapeado(self):
        assert normalize_score("MEDIUM", self.smap) == 0.5
        assert normalize_score("medium", self.smap) == 0.5  # case-insensitive

    def test_clamp(self):
        assert normalize_score(1.5, self.smap) == 1.0
        assert normalize_score(-2, self.smap) == 0.0

    def test_irreconhecivel(self):
        assert normalize_score("???", self.smap) is None
        assert normalize_score(None, self.smap) is None
