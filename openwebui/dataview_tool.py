"""
title: DataView Tool
author: miraiku
version: 1.1.0
description: Interroge des fichiers tabulaires (CSV, Excel, JSON, Parquet) en langage naturel via le service dataview.
"""

import json
from typing import Any

import httpx
from pydantic import BaseModel, Field


class Tools:
    class Valves(BaseModel):
        dataview_api_url: str = Field(
            default="http://dataview:8093",
            description="URL du service dataview",
        )
        timeout: int = Field(default=60, description="Timeout en secondes")

    def __init__(self):
        self.valves = self.Valves()

    async def _call(self, endpoint: str, payload: dict) -> dict | list | str:
        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            resp = await client.post(
                f"{self.valves.dataview_api_url}{endpoint}",
                json=payload,
            )
            if resp.status_code >= 400:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    return resp.json()
                return {"error": resp.text}
            return resp.json()

    async def data_preview(
        self,
        url: str,
        __user__: dict = {},
    ) -> str:
        """Aperçu d'un fichier de données (CSV, Excel, JSON, Parquet). Retourne les colonnes, types et premières lignes.

        :param url: URL du fichier à analyser
        :return: Aperçu du fichier avec colonnes, types et 5 premières lignes
        """
        data = await self._call("/preview", {"url": url})

        if isinstance(data, dict) and "error" in data:
            return json.dumps(data, ensure_ascii=False)

        columns = data.get("columns", [])
        rows = data.get("rows", 0)

        suggestions = [
            f"\n\n---\n**Pour poursuivre l'exploration, vous pouvez :**",
            f"- Demander le schéma détaillé (types, stats, valeurs uniques) avec `data_schema`",
            f"- Poser une question sur ces données, par exemple :",
        ]
        if columns:
            col_examples = columns[:3]
            suggestions.append(f'  - "Quelles sont les valeurs uniques de {col_examples[0]} ?"')
            if len(col_examples) >= 2:
                suggestions.append(f'  - "Combien de lignes par {col_examples[1]} ?"')
            if rows > 100:
                suggestions.append(f'  - "Donne-moi les 10 premières lignes triées par {col_examples[-1]}"')

        data["_suggestions"] = "\n".join(suggestions)
        return json.dumps(data, ensure_ascii=False, default=str)

    async def data_schema(
        self,
        url: str,
        __user__: dict = {},
    ) -> str:
        """Schéma détaillé d'un fichier de données : colonnes, types, statistiques, valeurs uniques.

        :param url: URL du fichier à analyser
        :return: Schéma détaillé du fichier
        """
        data = await self._call("/schema", {"url": url})

        if isinstance(data, dict) and "error" in data:
            return json.dumps(data, ensure_ascii=False)

        columns = data.get("columns", [])
        numeric_cols = [c["name"] for c in columns if c.get("dtype") in ("int64", "float64", "int32", "float32")]
        text_cols = [c["name"] for c in columns if c.get("dtype") == "object"]

        suggestions = [
            f"\n\n---\n**Maintenant que vous connaissez le schéma, vous pouvez demander :**",
        ]
        if numeric_cols:
            suggestions.append(f'- "Quelle est la moyenne de {numeric_cols[0]} ?"')
            if len(numeric_cols) >= 2:
                suggestions.append(f'- "Top 10 par {numeric_cols[0]}"')
        if text_cols:
            suggestions.append(f'- "Combien d\'entrées par {text_cols[0]} ?"')
            if len(text_cols) >= 2:
                suggestions.append(f'- "Cherche toutes les lignes contenant \'...\'  dans {text_cols[0]}"')
        suggestions.append(f'- Ou posez directement votre question en langage naturel !')

        data["_suggestions"] = "\n".join(suggestions)
        return json.dumps(data, ensure_ascii=False, default=str)

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
        data = await self._call("/query", {"url": url, "question": question})

        if isinstance(data, dict) and "error" in data:
            return json.dumps(data, ensure_ascii=False)

        result = data.get("result", [])
        row_count = data.get("row_count", 0)
        truncated = data.get("truncated", False)
        operation = data.get("operation", "")

        suggestions = [f"\n\n---\n**Pour aller plus loin :**"]

        if truncated:
            suggestions.append(f"- Les résultats sont tronqués ({row_count} lignes affichées). Affinez votre question pour réduire le nombre de résultats.")

        if result:
            result_cols = list(result[0].keys())
            if len(result_cols) >= 2:
                suggestions.append(f'- "Trie ces résultats par {result_cols[-1]} décroissant"')
            suggestions.append(f'- "Donne-moi des statistiques sur {result_cols[0]}"')

        suggestions.append(f"- Posez une autre question sur le même fichier, ou explorez un nouveau dataset.")

        data["_suggestions"] = "\n".join(suggestions)
        return json.dumps(data, ensure_ascii=False, default=str)
