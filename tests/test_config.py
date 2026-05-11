"""Tests for config.py — verifies env-var reading and fallback behaviour."""

import os

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
