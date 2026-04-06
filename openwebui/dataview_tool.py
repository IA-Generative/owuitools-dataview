"""
title: DataView Tool
author: miraiku
version: 1.3.0
description: Recherche et interroge des fichiers open data (CSV, Excel, JSON, Parquet). Supporte la recherche sur data.gouv.fr, les URLs et les fichiers uploadés.
"""

import json
from typing import Any

import httpx
from pydantic import BaseModel, Field


def _extract_file_from_messages(messages: list[dict] | None) -> dict | None:
    """Extrait le dernier fichier tabulaire uploadé depuis les messages."""
    if not messages:
        return None
    TABULAR_EXTENSIONS = {".csv", ".xls", ".xlsx", ".json", ".parquet", ".ods", ".tsv"}
    TABULAR_MIMETYPES = {
        "text/csv", "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/json", "application/vnd.oasis.opendocument.spreadsheet",
        "application/octet-stream", "application/parquet",
    }
    for message in reversed(messages):
        for f in reversed(message.get("files", [])):
            name = f.get("name", "") or f.get("file", {}).get("filename", "")
            ct = (f.get("content_type", "")
                  or f.get("file", {}).get("meta", {}).get("content_type", ""))
            file_id = f.get("id", "") or f.get("file", {}).get("id", "")
            if not file_id:
                continue
            ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ""
            if ext in TABULAR_EXTENSIONS or ct in TABULAR_MIMETYPES:
                return {"id": file_id, "name": name, "content_type": ct}
    return None


class Tools:
    class Valves(BaseModel):
        dataview_api_url: str = Field(
            default="http://dataview:8093",
            description="URL du service dataview",
        )
        openwebui_url: str = Field(
            default="http://openwebui:8080",
            description="URL interne d'Open WebUI (pour fetch les fichiers uploadés)",
        )
        datagouv_api_url: str = Field(
            default="https://www.data.gouv.fr/api/1",
            description="URL de l'API data.gouv.fr",
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

    async def _call_upload(
        self, endpoint: str, file_id: str, filename: str,
        content_type: str, user: dict, extra_fields: dict | None = None,
    ) -> dict | list | str:
        """Fetch le fichier depuis OWUI puis l'envoie au backend dataview."""
        headers = {}
        if user and user.get("token"):
            headers["Authorization"] = f"Bearer {user['token']}"

        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            # Fetch file from OpenWebUI
            file_resp = await client.get(
                f"{self.valves.openwebui_url}/api/v1/files/{file_id}/content",
                headers=headers,
            )
            if file_resp.status_code != 200:
                return {"error": f"Impossible de récupérer le fichier ({file_resp.status_code})"}

            # Send to dataview backend as multipart
            files = {"file": (filename, file_resp.content, content_type or "application/octet-stream")}
            data = extra_fields or {}
            resp = await client.post(
                f"{self.valves.dataview_api_url}{endpoint}",
                files=files,
                data=data,
            )
            if resp.status_code >= 400:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    return resp.json()
                return {"error": resp.text}
            return resp.json()

    async def data_search(
        self,
        query: str,
        __user__: dict = {},
    ) -> str:
        """Recherche des jeux de données open data sur data.gouv.fr. Retourne une liste de datasets avec leurs fichiers téléchargeables.

        :param query: Mots-clés de recherche en langage naturel
        :return: Liste de datasets avec titre, description, formats et URLs des ressources
        """
        if not query:
            query = "données ouvertes"
        SUPPORTED = {"csv", "xls", "xlsx", "json", "parquet", "ods"}
        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            resp = await client.get(
                f"{self.valves.datagouv_api_url}/datasets/",
                params={"q": query, "page_size": 10},
            )
            if resp.status_code != 200:
                return json.dumps({"error": f"Erreur API data.gouv.fr ({resp.status_code})"})

            data = resp.json()
            results = []
            for ds in data.get("data", []):
                resources = []
                for r in ds.get("resources", []):
                    fmt = (r.get("format") or "").lower()
                    if fmt not in SUPPORTED:
                        continue
                    resources.append({
                        "title": r.get("title", ""),
                        "format": fmt,
                        "url": f"https://www.data.gouv.fr/fr/datasets/r/{r['id']}",
                        "filesize_mb": round(r["filesize"] / (1024 * 1024), 1) if r.get("filesize") else None,
                    })
                if not resources:
                    continue
                results.append({
                    "title": ds.get("title", ""),
                    "description": (ds.get("description") or "")[:200],
                    "organization": (ds.get("organization") or {}).get("name", ""),
                    "last_update": (ds.get("last_update") or "")[:10],
                    "resources": resources[:3],
                })

            if not results:
                return json.dumps({
                    "message": f"Aucun dataset tabulaire trouvé pour '{query}'.",
                    "suggestion": "Essayez avec d'autres mots-clés ou consultez https://www.data.gouv.fr",
                }, ensure_ascii=False)

            output = {
                "query": query,
                "count": len(results),
                "datasets": results,
                "_suggestions": (
                    "\n\n---\n**Pour explorer un dataset, copiez son URL et demandez :**\n"
                    '- "Donne-moi un aperçu de ce fichier : [URL]"\n'
                    '- "Quel est le schéma de ce fichier : [URL]"\n'
                    '- "Dans ce fichier [URL], quelles sont les 10 premières lignes ?"'
                ),
            }
            return json.dumps(output, ensure_ascii=False, default=str)

    async def data_preview(
        self,
        url: str = "",
        __user__: dict = {},
        __messages__: list[dict] = None,
    ) -> str:
        """Aperçu d'un fichier de données (CSV, Excel, JSON, Parquet). Fonctionne avec une URL ou un fichier uploadé. Retourne les colonnes, types et premières lignes.

        :param url: URL du fichier à analyser (optionnel si un fichier est uploadé)
        :return: Aperçu du fichier avec colonnes, types et 5 premières lignes
        """
        uploaded = _extract_file_from_messages(__messages__)

        is_url = url.startswith("http://") or url.startswith("https://")

        if uploaded:
            data = await self._call_upload(
                "/preview/upload", uploaded["id"], uploaded["name"],
                uploaded["content_type"], __user__,
            )
        elif is_url:
            data = await self._call("/preview", {"url": url})
        else:
            return json.dumps({"error": "Veuillez fournir une URL ou uploader un fichier."})

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
        url: str = "",
        __user__: dict = {},
        __messages__: list[dict] = None,
    ) -> str:
        """Schéma détaillé d'un fichier de données : colonnes, types, statistiques, valeurs uniques.

        :param url: URL du fichier à analyser (optionnel si un fichier est uploadé)
        :return: Schéma détaillé du fichier
        """
        uploaded = _extract_file_from_messages(__messages__)
        is_url = url.startswith("http://") or url.startswith("https://")

        if uploaded:
            data = await self._call_upload(
                "/schema/upload", uploaded["id"], uploaded["name"],
                uploaded["content_type"], __user__,
            )
        elif is_url:
            data = await self._call("/schema", {"url": url})
        else:
            return json.dumps({"error": "Veuillez fournir une URL ou uploader un fichier."})

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
        url: str = "",
        question: str = "",
        __user__: dict = {},
        __messages__: list[dict] = None,
    ) -> str:
        """Interroge un fichier de données en langage naturel. Exemples : "Quelles sont les 10 communes les plus peuplées ?", "Population moyenne par département".

        :param url: URL du fichier à interroger (optionnel si un fichier est uploadé)
        :param question: Question en langage naturel sur les données
        :return: Résultat de la requête (lignes de données)
        """
        if not question:
            return json.dumps({"error": "Veuillez poser une question sur les données."})

        uploaded = _extract_file_from_messages(__messages__)
        is_url = url.startswith("http://") or url.startswith("https://")

        if uploaded:
            data = await self._call_upload(
                "/query/upload", uploaded["id"], uploaded["name"],
                uploaded["content_type"], __user__,
                extra_fields={"question": question},
            )
        elif is_url:
            data = await self._call("/query", {"url": url, "question": question})
        else:
            return json.dumps({"error": "Veuillez fournir une URL ou uploader un fichier."})

        if isinstance(data, dict) and "error" in data:
            return json.dumps(data, ensure_ascii=False)

        result = data.get("result", [])
        row_count = data.get("row_count", 0)
        truncated = data.get("truncated", False)

        suggestions = [f"\n\n---\n**Pour aller plus loin :**"]
        if truncated:
            suggestions.append(f"- Les résultats sont tronqués ({row_count} lignes affichées). Affinez votre question.")
        if result:
            result_cols = list(result[0].keys())
            if len(result_cols) >= 2:
                suggestions.append(f'- "Trie ces résultats par {result_cols[-1]} décroissant"')
            suggestions.append(f'- "Donne-moi des statistiques sur {result_cols[0]}"')
        suggestions.append(f"- Posez une autre question sur le même fichier, ou explorez un nouveau dataset.")

        data["_suggestions"] = "\n".join(suggestions)
        return json.dumps(data, ensure_ascii=False, default=str)
