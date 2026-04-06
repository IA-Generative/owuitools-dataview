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

    def _find_all_tabular_files(self, body: dict) -> list[dict]:
        """Trouve les fichiers tabulaires du message courant uniquement.
        metadata.files accumule TOUS les fichiers de la conversation.
        On ne garde que ceux qui ne sont PAS dans les messages précédents.
        """
        # Collect file IDs from previous messages (already processed)
        old_ids: set[str] = set()
        messages = body.get("messages", [])
        for m in messages[:-1]:  # All messages except the last (current)
            for f in m.get("files", []):
                fid = f.get("id", "") or f.get("file", {}).get("id", "")
                if fid:
                    old_ids.add(fid)

        seen_ids: set[str] = set()
        results: list[dict] = []

        # Source 1: metadata.files (where OWUI puts uploaded files)
        metadata_files = body.get("metadata", {}).get("files", [])
        # Source 2: body.files
        body_files = body.get("files", [])

        for file_list in [metadata_files, body_files]:
            for f in file_list:
                file_obj = f.get("file", {})
                meta = file_obj.get("meta", {})
                name = (f.get("name", "")
                        or file_obj.get("filename", "")
                        or meta.get("name", ""))
                ct = (f.get("content_type", "")
                      or meta.get("content_type", ""))
                file_id = f.get("id", "") or file_obj.get("id", "")
                if not file_id or not name or file_id in seen_ids or file_id in old_ids:
                    continue
                ext = ("." + name.rsplit(".", 1)[-1]).lower() if "." in name else ""
                if ext in TABULAR_EXTENSIONS or ct in TABULAR_MIMETYPES:
                    seen_ids.add(file_id)
                    results.append({"id": file_id, "name": name, "content_type": ct})

        return results

    def _read_file_from_disk(self, file_id: str) -> Optional[bytes]:
        """Lit un fichier uploadé depuis le filesystem OWUI."""
        from pathlib import Path
        for upload_dir in [Path("/app/backend/data/uploads"), Path("/app/backend/data/cache/files")]:
            if not upload_dir.exists():
                continue
            for fpath in upload_dir.iterdir():
                if fpath.name.startswith(file_id):
                    return fpath.read_bytes()
        return None

    def _format_preview(self, data: dict, filename: str) -> str:
        """Formate un aperçu dataview en texte markdown."""
        cols = data.get("columns", [])
        rows = data.get("rows", 0)
        preview = data.get("preview", [])
        dtypes = data.get("dtypes", {})
        sheets = data.get("sheets")

        parts = [
            f"\n\n[Aperçu automatique du fichier '{filename}']",
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
            parts.append("| " + " | ".join(str(c) for c in cols) + " |")
            parts.append("|" + "|".join("---" for _ in cols) + "|")
            for row in preview[:5]:
                parts.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")

        parts.append(
            "\nPour explorer ces données : "
            "`data_schema()` pour le schéma détaillé, "
            "`data_query(question=...)` pour interroger."
        )
        return "\n".join(parts)

    async def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        """Intercepte les messages avant le LLM. Détecte tous les fichiers tabulaires
        uploadés, génère un aperçu pour chacun et l'injecte dans le contexte."""
        if not self.valves.enabled:
            return body

        messages = body.get("messages", [])
        if not messages:
            return body

        uploaded_files = self._find_all_tabular_files(body)
        if not uploaded_files:
            return body

        logger.info(f"DataView filter: detected {len(uploaded_files)} tabular file(s)")

        all_previews: list[str] = []

        async with httpx.AsyncClient(timeout=self.valves.timeout) as client:
            for uploaded in uploaded_files:
                try:
                    file_content = self._read_file_from_disk(uploaded["id"])
                    if not file_content:
                        logger.warning(f"DataView filter: {uploaded['name']} ({uploaded['id']}) not found on disk")
                        continue

                    logger.info(f"DataView filter: processing {uploaded['name']} ({len(file_content)} bytes)")

                    files = {"file": (uploaded["name"], file_content,
                                      uploaded["content_type"] or "application/octet-stream")}
                    preview_resp = await client.post(
                        f"{self.valves.dataview_api_url}/preview/upload",
                        files=files,
                    )
                    if preview_resp.status_code != 200:
                        logger.warning(f"DataView filter: preview failed for {uploaded['name']} ({preview_resp.status_code})")
                        continue

                    data = preview_resp.json()
                    preview_text = self._format_preview(data, uploaded["name"])
                    all_previews.append(preview_text)

                    logger.info(f"DataView filter: {uploaded['name']} → {data.get('rows', 0)} rows, {len(data.get('columns', []))} cols")

                except Exception as e:
                    logger.error(f"DataView filter error on {uploaded['name']}: {e}")

        if all_previews:
            last_msg = messages[-1]
            if last_msg.get("role") == "user":
                last_msg["content"] = last_msg.get("content", "") + "".join(all_previews)
            logger.info(f"DataView filter: injected {len(all_previews)} preview(s)")

        return body
