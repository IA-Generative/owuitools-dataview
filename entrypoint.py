"""Entrypoint: lance le serveur FastAPI REST et le serveur MCP en parallèle."""

import asyncio
import multiprocessing

import uvicorn

from app.config import settings


def run_api():
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.PORT, log_level="info")


def run_mcp():
    uvicorn.run("app.mcp_app:app", host="0.0.0.0", port=settings.MCP_PORT, log_level="info")


if __name__ == "__main__":
    api_process = multiprocessing.Process(target=run_api)
    mcp_process = multiprocessing.Process(target=run_mcp)

    api_process.start()
    mcp_process.start()

    api_process.join()
    mcp_process.join()
