"""FastAPI application principale."""

import logging

from fastapi import FastAPI

from app.api import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = FastAPI(
    title="data-query-owui",
    description="Service d'interrogation de fichiers tabulaires en langage naturel",
    version="1.0.0",
)

app.include_router(router)
