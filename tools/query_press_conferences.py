"""
MCP tool: query_press_conferences

Semantic search over the 'press' Pinecone namespace, which holds Premier League
press conference summaries, match reports, and player injury/availability news
ingested by jobs/ingest_press_content.py.

Mirrors the retrieval logic in the Gaffer's server/rag.py exactly:
  - Embeds the query with multilingual-e5-large via Pinecone built-in inference
  - Re-ranks results by applying a recency score multiplier
  - Returns a formatted string ready to inject into a model context
"""

import logging

from pinecone import Pinecone

from config import cfg

log = logging.getLogger(__name__)

# Must match the model used at ingest time (jobs/ingest_press_content.py).
_EMBED_MODEL = "multilingual-e5-large"
_NAMESPACE = "press"


def _build_client() -> Pinecone:
    """Instantiate and return a Pinecone client using the configured API key."""
    return Pinecone(api_key=cfg.pinecone_api_key)


def _format_results(weighted: list[tuple[float, object]]) -> str:
    """
    Render ranked Pinecone matches as a human-readable string.

    Args:
        weighted: List of (final_score, match) tuples, already sorted descending.

    Returns:
        Newline-separated document blocks with rank, type, date, score, and text.
    """
    parts = []
    for rank, (score, match) in enumerate(weighted, start=1):
        meta = match.metadata or {}
        text = meta.get("text", "")
        doc_type = meta.get("type", "unknown")
        date = meta.get("date", "")

        header = f"[{rank}] {doc_type}"
        if date:
            header += f" | {date}"
        header += f" | relevance: {score:.3f}"

        parts.append(f"{header}\n{text}")

    return "\n\n".join(parts)


async def query_press_conferences(
    query: str,
    top_k: int = 5,
    recency_weight: float = 0.3,
) -> str:
    """
    Retrieve relevant press conference and sports news documents from Pinecone.

    Embeds ``query`` with the same model used at ingest time, fetches the top-k
    semantic matches from the 'press' namespace, applies a recency multiplier so
    fresher articles rank higher for equally-relevant queries, and returns a
    formatted string.

    This function is async so it can be called directly from the async MCP server
    handler without blocking the event loop. The Pinecone SDK calls are synchronous
    but fast enough (single network round-trip) that running them inline is fine.

    Args:
        query:          Natural-language question or topic to search for.
        top_k:          Number of documents to retrieve before re-ranking. Default 5.
        recency_weight: Strength of the recency boost (0.0 = pure semantic,
                        1.0 = heavy recency bias). Default 0.3.

    Returns:
        Formatted string of retrieved documents, or an empty string if Pinecone
        returns no matches or is quota-exhausted.
    """
    if not cfg.pinecone_api_key:
        log.error("PINECONE_API_KEY is not set — cannot query press conferences.")
        return ""

    if cfg.dry_run:
        log.info("[dry run] query_press_conferences: would search Pinecone, query=%r", query)
        return (
            "[DRY RUN] No Pinecone call was made. "
            f"Would have searched index '{cfg.pinecone_index_name}' "
            f"namespace='{_NAMESPACE}' with:\n"
            f"  query={query!r}\n"
            f"  top_k={top_k}\n"
            f"  recency_weight={recency_weight}"
        )

    pc = _build_client()
    index = pc.Index(cfg.pinecone_index_name)

    # Embed the query using the same model as at ingest time, with input_type="query"
    # (Pinecone optimises the embedding differently for queries vs passages).
    try:
        embeddings = pc.inference.embed(
            model=_EMBED_MODEL,
            inputs=[query],
            parameters={"input_type": "query"},
        )
    except Exception as exc:
        # Quota exhaustion (HTTP 429) is expected under free-tier limits — degrade
        # gracefully rather than surfacing an error to the MCP caller.
        if "429" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc):
            log.warning("Pinecone quota exhausted — returning empty RAG context.")
            return ""
        log.error("Failed to embed query: %s", exc, exc_info=True)
        raise

    query_vector = embeddings[0].values

    results = index.query(
        vector=query_vector,
        top_k=top_k,
        namespace=_NAMESPACE,
        include_metadata=True,
    )

    if not results.matches:
        return ""

    # Re-rank: final_score = semantic_score × (1 + recency_weight × recency_score)
    # recency_score is stored as metadata at ingest time: 1.0 today → 0.1 at 14 days.
    weighted: list[tuple[float, object]] = []
    for match in results.matches:
        recency_score = (match.metadata or {}).get("recency_score", 0.5)
        final_score = match.score * (1 + recency_weight * recency_score)
        weighted.append((final_score, match))

    weighted.sort(key=lambda x: x[0], reverse=True)

    log.info(
        "query_press_conferences: query=%r top_k=%d returned %d matches",
        query,
        top_k,
        len(weighted),
    )
    return _format_results(weighted)
