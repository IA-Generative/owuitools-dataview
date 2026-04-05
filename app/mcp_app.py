"""ASGI app MCP standalone — montée via uvicorn sur le port MCP."""

from app.mcp_server import mcp

# Expose l'app ASGI Streamable HTTP
app = mcp.streamable_http_app()
