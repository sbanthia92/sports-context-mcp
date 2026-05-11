"""
Tests for tools/query_press_conferences.py.

All Pinecone calls are mocked — no real index is required.
"""

from unittest.mock import MagicMock, patch

import pytest

from tools.query_press_conferences import query_press_conferences


def _make_match(score: float, text: str, recency_score: float = 0.8) -> MagicMock:
    """Build a mock Pinecone match object."""
    m = MagicMock()
    m.score = score
    m.metadata = {
        "text": text,
        "type": "press_article",
        "source": "BBC Sport",
        "date": "Mon, 05 May 2026 12:00:00 GMT",
        "recency_score": recency_score,
    }
    return m


@pytest.fixture
def mock_pinecone():
    """Patch pinecone.Pinecone so no real API calls are made."""
    with patch("tools.query_press_conferences.Pinecone") as MockPC:
        pc_instance = MagicMock()
        MockPC.return_value = pc_instance

        # embed returns a list with one embedding object
        embedding = MagicMock()
        embedding.values = [0.1] * 1024
        pc_instance.inference.embed.return_value = [embedding]

        # index.query returns a results object with .matches
        index = MagicMock()
        pc_instance.Index.return_value = index

        yield pc_instance, index


@pytest.mark.asyncio
async def test_returns_formatted_results(mock_pinecone):
    """Results are returned as a formatted string with rank headers."""
    _, index = mock_pinecone
    index.query.return_value.matches = [
        _make_match(0.9, "Arsenal beat City 2-0"),
    ]

    result = await query_press_conferences("Arsenal City match")

    assert "[1]" in result
    assert "Arsenal beat City 2-0" in result


@pytest.mark.asyncio
async def test_recency_reranking_promotes_fresher_doc(mock_pinecone):
    """A slightly less semantically similar but much fresher doc should rank first."""
    _, index = mock_pinecone
    # Doc A: high semantic score, low recency
    # Doc B: lower semantic score, high recency
    # With recency_weight=0.3: A = 0.9*(1+0.3*0.1)=0.927, B = 0.85*(1+0.3*1.0)=1.105
    index.query.return_value.matches = [
        _make_match(0.9, "Older article", recency_score=0.1),
        _make_match(0.85, "Fresh article", recency_score=1.0),
    ]

    result = await query_press_conferences("latest news", recency_weight=0.3)

    # The fresh article should appear first (rank 1)
    fresh_pos = result.find("Fresh article")
    older_pos = result.find("Older article")
    assert fresh_pos < older_pos


@pytest.mark.asyncio
async def test_returns_empty_string_on_no_matches(mock_pinecone):
    """Returns empty string when Pinecone finds no matching documents."""
    _, index = mock_pinecone
    index.query.return_value.matches = []

    result = await query_press_conferences("completely unknown topic")

    assert result == ""


@pytest.mark.asyncio
async def test_returns_empty_string_on_missing_api_key(monkeypatch):
    """Returns empty string and logs error when PINECONE_API_KEY is missing."""
    monkeypatch.setenv("PINECONE_API_KEY", "")

    result = await query_press_conferences("anything")

    assert result == ""


@pytest.mark.asyncio
async def test_degrades_gracefully_on_quota_exhaustion(mock_pinecone):
    """Returns empty string (not an exception) when Pinecone returns 429."""
    pc_instance, _ = mock_pinecone
    pc_instance.inference.embed.side_effect = Exception("429 RESOURCE_EXHAUSTED")

    result = await query_press_conferences("some query")

    assert result == ""


@pytest.mark.asyncio
async def test_top_k_is_passed_to_pinecone(mock_pinecone):
    """top_k parameter is forwarded to the Pinecone query call."""
    _, index = mock_pinecone
    index.query.return_value.matches = []

    await query_press_conferences("query", top_k=10)

    call_kwargs = index.query.call_args.kwargs
    assert call_kwargs["top_k"] == 10
