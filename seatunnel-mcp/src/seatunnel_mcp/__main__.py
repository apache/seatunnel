"""Main entry point for the SeaTunnel MCP server."""

import os
import sys
import logging
from typing import Dict, Any, Optional

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from .client import SeaTunnelClient
from .tools import get_all_tools

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Default values
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
DEFAULT_API_URL = "http://localhost:8090"  # Default SeaTunnel API URL


def main():
    """Run the SeaTunnel MCP server."""
    # Get configuration from environment
    host = os.environ.get("MCP_HOST", DEFAULT_HOST)
    port = int(os.environ.get("MCP_PORT", DEFAULT_PORT))
    api_url = os.environ.get("SEATUNNEL_API_URL", DEFAULT_API_URL)
    api_key = os.environ.get("SEATUNNEL_API_KEY", None)

    # Create SeaTunnel client
    client = SeaTunnelClient(base_url=api_url, api_key=api_key)

    # Create MCP server
    server = FastMCP(
        name="SeaTunnel MCP Server",
        instructions="A Model Context Protocol server for interacting with SeaTunnel through LLM interfaces",
        log_level="INFO",
        host=host,
        port=port,
    )

    # Register all tools
    tools = get_all_tools(client)
    for tool_fn in tools:
        # 直接添加函数作为工具
        server.add_tool(tool_fn)

    # Run server
    logger.info(f"Starting SeaTunnel MCP server at http://{host}:{port}")
    server.run()


if __name__ == "__main__":
    main() 