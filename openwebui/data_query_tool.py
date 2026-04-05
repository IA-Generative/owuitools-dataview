"""
title: Data Query Tool
author: miraiku
version: 1.0.0
description: Interroge des fichiers tabulaires (CSV, Excel, JSON, Parquet) en langage naturel via le service data-query.
"""

import json
from typing import Any

import httpx
from pydantic import BaseModel, Field


class Tools:
    class Valves(BaseModel):
        data_query_api_url: str = Field(
            default="http://data-query:8093",
            description="URL du service data-query",
        )
        timeout: int = Field(default=60, description="Timeout en secondes")

    def __init__(self):
        self.valves = self.Valves()

    async def _call(self, endpoint: str, payload: dict) -> str:
        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            resp = await client.post(
                f"{self.valves.data_query_api_url}{endpoint}",
                json=payload,
            )
            if resp.status_code >= 400:
                error = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"error": resp.text}
                return json.dumps(error, ensure_ascii=False)
            return json.dumps(resp.json(), ensure_ascii=False, default=str)

    async def data_preview(
        self,
        url: str,
        __user__: dict = {},
    ) -> str:
        """Aperçu d'un fichier de données (CSV, Excel, JSON, Parquet). Retourne les colonnes, types et premières lignes.

        :param url: URL du fichier à analyser
        :return: Aperçu du fichier avec colonnes, types et 5 premières lignes
        """
        return await self._call("/preview", {"url": url})

    async def data_schema(
        self,
        url: str,
        __user__: dict = {},
    ) -> str:
        """Schéma détaillé d'un fichier de données : colonnes, types, statistiques, valeurs uniques.

        :param url: URL du fichier à analyser
        :return: Schéma détaillé du fichier
        """
        return await self._call("/schema", {"url": url})

    async def data_query(
        self,
        url: str,
        question: str,
        __user__: dict = {},
    ) -> str:
        """Interroge un fichier de données en langage naturel. Exemples : "Quelles sont les 10 communes les plus peuplées ?", "Population moyenne par département".

        :param url: URL du fichier à interroger
        :param question: Question en langage naturel sur les données
        :return: Résultat de la requête (lignes de données)
        """
        return await self._call("/query", {"url": url, "question": question})
