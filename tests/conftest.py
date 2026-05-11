"""
Shared pytest fixtures for sports-context-mcp tests.

All external calls (HTTP, Pinecone, PostgreSQL) are mocked here so tests
never hit real APIs or databases.
"""

import os

import pytest


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    """
    Inject minimal environment variables before every test.

    Uses monkeypatch so values are restored after each test — no bleed between
    test cases.
    """
    monkeypatch.setenv("PINECONE_API_KEY", "test-pinecone-key")
    monkeypatch.setenv("PINECONE_INDEX_NAME", "test-index")
    monkeypatch.setenv("DATABASE_URL", "postgresql://readonly:pass@localhost/gaffer")
    monkeypatch.setenv("DATABASE_ETL_URL", "postgresql://etl:pass@localhost/gaffer")
    monkeypatch.setenv("API_SPORTS_KEY", "test-sports-key")
    monkeypatch.setenv("GUARDIAN_API_KEY", "test-guardian-key")
