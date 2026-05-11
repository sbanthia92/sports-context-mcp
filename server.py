"""
sports-context-mcp — MCP server entry point.

Exposes two tools over the MCP stdio transport:

  query_historical_stats   — read-only SQL against the Gaffer PostgreSQL database
  query_press_conferences  — semantic search over the Pinecone 'press' namespace

Run locally:
    cd sports-context-mcp
    python server.py

Register in Claude Desktop (claude_desktop_config.json):
    {
      "mcpServers": {
        "sports-context": {
          "command": "python",
          "args": ["/absolute/path/to/sports-context-mcp/server.py"]
        }
      }
    }
"""

import asyncio
import logging

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from tools.query_historical_stats import (
    SCHEMA_DESCRIPTION,
    query_historical_stats,
)
from tools.query_press_conferences import query_press_conferences

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

server = Server("sports-context-mcp")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """
    Advertise the two MCP tools this server exposes.

    Called by the MCP client (e.g. Claude Desktop) during initialisation to
    discover what capabilities are available.

    Returns:
        List of Tool descriptors with names, descriptions, and JSON Schema for
        their input parameters.
    """
    return [
        types.Tool(
            name="query_historical_stats",
            description=(
                "Execute a read-only SQL SELECT against the Gaffer historical sports "
                "database. Use this to answer questions about player stats, fixtures, "
                "team strength, or gameweek history across multiple Premier League seasons.\n\n"
                + SCHEMA_DESCRIPTION
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": (
                            "A SQL SELECT statement. Only SELECT is allowed — "
                            "mutations will be rejected. LIMIT is injected automatically "
                            "if omitted (max 100 rows)."
                        ),
                    }
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="query_press_conferences",
            description=(
                "Semantic search over Premier League press conference summaries, match "
                "reports, and player injury/availability updates ingested from BBC Sport "
                "and The Guardian. Use this to find recent quotes, injury news, or "
                "manager/team news."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language question or topic to search for.",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of documents to retrieve. Default 5.",
                        "default": 5,
                    },
                    "recency_weight": {
                        "type": "number",
                        "description": (
                            "How strongly to boost recent articles in ranking. "
                            "0.0 = pure semantic similarity, 1.0 = heavy recency bias. "
                            "Default 0.3."
                        ),
                        "default": 0.3,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(
    name: str,
    arguments: dict,
) -> list[types.TextContent]:
    """
    Dispatch an incoming tool call to the appropriate implementation.

    Args:
        name:      Tool name as registered in list_tools().
        arguments: Dict of arguments matching the tool's inputSchema.

    Returns:
        List containing a single TextContent with the tool's output.

    Raises:
        ValueError: If an unknown tool name is requested.
    """
    log.info("tool call: %s args=%r", name, arguments)

    if name == "query_historical_stats":
        sql = arguments.get("sql", "")
        result = await query_historical_stats(sql)

    elif name == "query_press_conferences":
        query = arguments.get("query", "")
        top_k = int(arguments.get("top_k", 5))
        recency_weight = float(arguments.get("recency_weight", 0.3))
        result = await query_press_conferences(
            query=query,
            top_k=top_k,
            recency_weight=recency_weight,
        )

    else:
        raise ValueError(f"Unknown tool: {name!r}")

    return [types.TextContent(type="text", text=result)]


async def _serve() -> None:
    """Wire the MCP server to stdio and run until the client disconnects."""
    async with stdio_server() as (read_stream, write_stream):
        log.info("sports-context-mcp server started (stdio transport)")
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> None:
    """Entry point — run the MCP server synchronously via asyncio."""
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
