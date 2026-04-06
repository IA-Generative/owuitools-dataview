"""
title: DataView Tool
author: miraiku
version: 1.4.0
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

    SUPPORTED_FORMATS = {"csv", "xls", "xlsx", "json", "parquet", "ods"}

    def _format_dataset(self, ds: dict) -> dict | None:
        """Formate un dataset data.gouv.fr avec ses ressources tabulaires."""
        resources = []
        for r in ds.get("resources", []):
            fmt = (r.get("format") or "").lower()
            if fmt not in self.SUPPORTED_FORMATS:
                continue
            resources.append({
                "title": r.get("title", ""),
                "format": fmt,
                "url": f"https://www.data.gouv.fr/fr/datasets/r/{r['id']}",
                "filesize_mb": round(r["filesize"] / (1024 * 1024), 1) if r.get("filesize") else None,
            })
        if not resources:
            return None
        return {
            "title": ds.get("title", ""),
            "description": (ds.get("description") or "")[:200],
            "organization": (ds.get("organization") or {}).get("name", ""),
            "tags": (ds.get("tags") or [])[:5],
            "last_update": (ds.get("last_update") or "")[:10],
            "page": ds.get("page", ""),
            "resources": resources[:5],
        }

    async def _fetch_datasets(self, params: dict) -> tuple[list[dict], int, int, int]:
        """Fetch datasets from data.gouv.fr API. Returns (results, total, page, page_size)."""
        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            resp = await client.get(
                f"{self.valves.datagouv_api_url}/datasets/",
                params=params,
            )
            if resp.status_code != 200:
                return [], 0, 1, 0

            data = resp.json()
            results = [r for r in (self._format_dataset(ds) for ds in (data.get("data") or [])) if r]
            return results, data.get("total", 0), data.get("page", 1), data.get("page_size", 20)

    async def data_search(
        self,
        query: str = "",
        organization: str = "",
        tag: str = "",
        page: int = 1,
        __user__: dict = {},
    ) -> str:
        """Recherche des jeux de données open data sur data.gouv.fr (74 000+ datasets publics). Retourne les datasets avec leurs fichiers téléchargeables (CSV, Excel, JSON, Parquet). Utilise le paramètre page pour voir les résultats suivants.

        :param query: Mots-clés de recherche en langage naturel
        :param organization: Filtrer par organisation (ex: "SNCF", "INSEE", "Ministère")
        :param tag: Filtrer par tag (ex: "transport", "emploi", "sante", "environnement")
        :param page: Numéro de page (1, 2, 3...) pour parcourir tous les résultats
        :return: Liste de datasets avec titre, description, formats et URLs
        """
        if not query and not organization and not tag:
            query = "données ouvertes"

        params = {"page_size": 20, "page": page or 1}
        if query:
            params["q"] = query
        if organization:
            params["organization"] = organization
        if tag:
            params["tag"] = tag

        results, total, current_page, page_size = await self._fetch_datasets(params)

        if not results:
            return json.dumps({
                "message": "Aucun dataset tabulaire trouvé pour cette recherche.",
                "suggestion": "Essayez d'autres mots-clés, ou utilisez data_list_popular() pour voir les datasets les plus consultés.",
            }, ensure_ascii=False)

        has_more = (current_page * page_size) < total
        output = {
            "query": query or (f"organization={organization}" if organization else f"tag={tag}" if tag else ""),
            "total": total,
            "page": current_page,
            "page_size": page_size,
            "count": len(results),
            "has_more": has_more,
            "datasets": results,
            "_suggestions": (
                "\n\n---\n**Pour explorer un dataset, utilisez son URL :**\n"
                '- `data_preview(url)` pour un aperçu\n'
                '- `data_schema(url)` pour le schéma détaillé\n'
                '- `data_query(url, question)` pour interroger les données\n'
                + (f'- **Page suivante** : `data_search(query="{query}", page={current_page + 1})`' if has_more else '')
            ),
        }
        return json.dumps(output, ensure_ascii=False, default=str)

    async def data_list_popular(
        self,
        theme: str = "",
        page: int = 1,
        __user__: dict = {},
    ) -> str:
        """Liste les jeux de données open data les plus populaires sur data.gouv.fr, triés par nombre de vues. Utilise le paramètre page pour voir plus de résultats.

        :param theme: Thème optionnel pour filtrer (ex: "transport", "sante", "education", "environnement", "emploi", "logement")
        :param page: Numéro de page (1, 2, 3...) pour parcourir tous les résultats
        :return: Les datasets les plus consultés avec leurs fichiers
        """
        params = {"page_size": 20, "sort": "-views", "page": page or 1}
        if theme:
            params["tag"] = theme

        results, total, current_page, page_size = await self._fetch_datasets(params)

        if not results:
            return json.dumps({
                "message": f"Aucun dataset tabulaire populaire trouvé" + (f" pour le thème '{theme}'" if theme else "") + ".",
                "suggestion": "Essayez sans thème, ou utilisez data_search(query) pour une recherche par mots-clés.",
            }, ensure_ascii=False)

        has_more = (current_page * page_size) < total
        output = {
            "theme": theme or "tous",
            "total": total,
            "page": current_page,
            "count": len(results),
            "has_more": has_more,
            "datasets": results,
            "_suggestions": (
                "\n\n---\n**Pour explorer un dataset, utilisez son URL :**\n"
                '- `data_preview(url)` pour un aperçu\n'
                '- `data_search(query)` pour affiner la recherche\n'
                + (f'- **Page suivante** : `data_list_popular(theme="{theme}", page={current_page + 1})`' if has_more else '')
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
