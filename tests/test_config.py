"""Tests for config.py — verifies env-var reading and fallback behaviour."""

import pytest

from config import _Config


def test_reads_pinecone_api_key(monkeypatch):
    """Config reads PINECONE_API_KEY from the environment."""
    monkeypatch.setenv("PINECONE_API_KEY", "my-key")
    c = _Config()
    assert c.pinecone_api_key == "my-key"


def test_pinecone_index_name_default(monkeypatch):
    """PINECONE_INDEX_NAME defaults to 'the-gaffer' when unset."""
    monkeypatch.delenv("PINECONE_INDEX_NAME", raising=False)
    c = _Config()
    assert c.pinecone_index_name == "the-gaffer"


def test_pinecone_index_name_override(monkeypatch):
    """PINECONE_INDEX_NAME can be overridden."""
    monkeypatch.setenv("PINECONE_INDEX_NAME", "my-index")
    c = _Config()
    assert c.pinecone_index_name == "my-index"


def test_database_etl_url_falls_back_to_database_url(monkeypatch):
    """DATABASE_ETL_URL falls back to DATABASE_URL when unset."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://ro:x@localhost/db")
    monkeypatch.delenv("DATABASE_ETL_URL", raising=False)
    c = _Config()
    assert c.database_etl_url == "postgresql://ro:x@localhost/db"


def test_database_etl_url_takes_precedence(monkeypatch):
    """DATABASE_ETL_URL is used when explicitly set."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://ro:x@localhost/db")
    monkeypatch.setenv("DATABASE_ETL_URL", "postgresql://etl:y@localhost/db")
    c = _Config()
    assert c.database_etl_url == "postgresql://etl:y@localhost/db"


def test_guardian_api_key_defaults_to_test(monkeypatch):
    """GUARDIAN_API_KEY defaults to 'test' (the public open key)."""
    monkeypatch.delenv("GUARDIAN_API_KEY", raising=False)
    c = _Config()
    assert c.guardian_api_key == "test"


def test_missing_pinecone_key_returns_empty_string(monkeypatch):
    """Missing PINECONE_API_KEY returns empty string (caller handles the error)."""
    monkeypatch.delenv("PINECONE_API_KEY", raising=False)
    c = _Config()
    assert c.pinecone_api_key == ""


def test_dry_run_defaults_to_false(monkeypatch):
    """DRY_RUN is False when the env var is unset."""
    monkeypatch.delenv("DRY_RUN", raising=False)
    assert _Config().dry_run is False


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "YES"])
def test_dry_run_truthy_values(monkeypatch, value):
    """DRY_RUN accepts common truthy string values."""
    monkeypatch.setenv("DRY_RUN", value)
    assert _Config().dry_run is True


def test_dry_run_false_string(monkeypatch):
    """DRY_RUN=false is not truthy."""
    monkeypatch.setenv("DRY_RUN", "false")
    assert _Config().dry_run is False
