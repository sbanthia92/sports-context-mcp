"""
Configuration for sports-context-mcp.

Reads from environment variables. Intentionally uses the same variable names as
the Gaffer server so a single .env file at the repo root covers both packages.

python-dotenv is loaded first so the .env two levels up (the Gaffer repo root)
is picked up automatically during local development. In CI / production, the
variables are injected directly into the environment by the workflow or secrets
manager, and dotenv is a no-op.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    # Walk up to find the nearest .env — works whether running from within
    # sports-context-mcp/ or from the Gaffer repo root.
    _here = Path(__file__).resolve().parent
    for _candidate in [_here / ".env", _here.parent / ".env"]:
        if _candidate.exists():
            load_dotenv(_candidate)
            break
except ImportError:
    pass  # python-dotenv is optional; env vars must be set another way


class _Config:
    """
    Central configuration object.

    All attributes are read lazily from environment variables so that tests
    can set os.environ before importing this module. Access via the module-level
    ``cfg`` singleton.
    """

    @property
    def pinecone_api_key(self) -> str:
        """Pinecone API key. Required for RAG tools and press ingestion."""
        return os.getenv("PINECONE_API_KEY", "")

    @property
    def pinecone_index_name(self) -> str:
        """Pinecone index name. Must match the index created in the Gaffer setup."""
        return os.getenv("PINECONE_INDEX_NAME", "the-gaffer")

    @property
    def database_url(self) -> str:
        """
        Read-only PostgreSQL connection string (gaffer_readonly user).
        Used by the MCP query_historical_stats tool.
        Format: postgresql://user:pass@host:5432/dbname
        """
        return os.getenv("DATABASE_URL", "")

    @property
    def database_etl_url(self) -> str:
        """
        Read/write PostgreSQL connection string (gaffer_etl user).
        Used by ingest_match_data to write fixture and player stats rows.
        Falls back to DATABASE_URL if not set.
        """
        return os.getenv("DATABASE_ETL_URL", "") or self.database_url

    @property
    def api_sports_key(self) -> str:
        """API-Sports key. Used by ingest_match_data for supplementary match data."""
        return os.getenv("API_SPORTS_KEY", "")

    @property
    def guardian_api_key(self) -> str:
        """
        The Guardian open platform API key.
        Register for free at https://open-platform.theguardian.com/access/.
        Defaults to 'test' (the public open key — lower rate limit, no full body text).
        """
        return os.getenv("GUARDIAN_API_KEY", "test")


# Module-level singleton — import this everywhere.
cfg = _Config()
