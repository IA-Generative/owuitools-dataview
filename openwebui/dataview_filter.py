"""
title: DataView Auto-Preview Filter
author: miraiku
version: 1.0.0
description: Détecte les fichiers tabulaires uploadés et appelle automatiquement data_preview pour injecter un aperçu dans le contexte du LLM.
"""

import json
import logging
from typing import Optional

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

TABULAR_EXTENSIONS = {".csv", ".xls", ".xlsx", ".json", ".parquet", ".ods", ".tsv"}
TABULAR_MIMETYPES = {
    "text/csv", "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/json", "application/vnd.oasis.opendocument.spreadsheet",
    "application/octet-stream", "application/parquet",
}


class Filter:
    class Valves(BaseModel):
        dataview_api_url: str = Field(
            default="http://dataview:8093",
            description="URL du service dataview",
        )
        openwebui_url: str = Field(
            default="http://openwebui:8080",
            description="URL interne d'Open WebUI",
        )
        timeout: int = Field(default=60, description="Timeout en secondes")
        enabled: bool = Field(default=True, description="Activer l'auto-preview")

    def __init__(self):
        self.valves = self.Valves()

    def _find_tabular_file(self, messages: list[dict]) -> Optional[dict]:
        """Trouve le dernier fichier tabulaire uploadé."""
        for message in reversed(messages):
            for f in reversed(message.get("files", [])):
                file_obj = f.get("file", {})
                meta = file_obj.get("meta", {})
                name = f.get("name", "") or file_obj.get("filename", "") or meta.get("name", "")
                ct = f.get("content_type", "") or meta.get("content_type", "")
                file_id = f.get("id", "") or file_obj.get("id", "")
                if not file_id or not name:
                    continue
                ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ""
                if ext in TABULAR_EXTENSIONS or ct in TABULAR_MIMETYPES:
                    return {"id": file_id, "name": name, "content_type": ct}
        return None

    async def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        """Intercepte les messages avant le LLM. Si un fichier tabulaire est uploadé,
        fetch le fichier et appelle /preview/upload, puis injecte le résultat."""
        if not self.valves.enabled:
            return body

        messages = body.get("messages", [])
        if not messages:
            return body

        uploaded = self._find_tabular_file(messages)
        if not uploaded:
            return body

        logger.info(f"DataView filter: detected tabular file {uploaded['name']}")

        try:
            headers = {}
            if __user__ and __user__.get("token"):
                headers["Authorization"] = f"Bearer {__user__['token']}"

            async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
                # Fetch file from OWUI
                file_resp = await client.get(
                    f"{self.valves.openwebui_url}/api/v1/files/{uploaded['id']}/content",
                    headers=headers,
                )
                if file_resp.status_code != 200:
                    logger.warning(f"DataView filter: could not fetch file ({file_resp.status_code})")
                    return body

                # Send to dataview for preview
                files = {"file": (uploaded["name"], file_resp.content,
                                  uploaded["content_type"] or "application/octet-stream")}
                preview_resp = await client.post(
                    f"{self.valves.dataview_api_url}/preview/upload",
                    files=files,
                )
                if preview_resp.status_code != 200:
                    logger.warning(f"DataView filter: preview failed ({preview_resp.status_code})")
                    return body

                data = preview_resp.json()

            # Format preview as text to inject
            cols = data.get("columns", [])
            rows = data.get("rows", 0)
            preview = data.get("preview", [])
            dtypes = data.get("dtypes", {})
            sheets = data.get("sheets")

            parts = [
                f"\n\n[Aperçu automatique du fichier '{uploaded['name']}']",
                f"Format: {data.get('format', '?')} | Lignes: {rows} | Colonnes: {len(cols)}",
            ]
            if sheets:
                parts.append(f"Feuilles: {', '.join(sheets)}")
            if cols:
                parts.append(f"Colonnes: {', '.join(cols)}")
                type_info = ', '.join(f"{c}({dtypes.get(c, '?')})" for c in cols[:10])
                parts.append(f"Types: {type_info}")
            if preview:
                parts.append("\nPremières lignes:")
                # Table header
                parts.append("| " + " | ".join(str(c) for c in cols) + " |")
                parts.append("|" + "|".join("---" for _ in cols) + "|")
                for row in preview[:5]:
                    parts.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")

            parts.append(
                "\nPour explorer ces données, vous pouvez utiliser : "
                "`data_schema()` pour le schéma détaillé, "
                "`data_query(question=...)` pour interroger les données."
            )

            preview_text = "\n".join(parts)

            # Inject preview into the last user message
            last_msg = messages[-1]
            if last_msg.get("role") == "user":
                last_msg["content"] = last_msg.get("content", "") + preview_text

            logger.info(f"DataView filter: injected preview ({rows} rows, {len(cols)} cols)")

        except Exception as e:
            logger.error(f"DataView filter error: {e}")

        return body
