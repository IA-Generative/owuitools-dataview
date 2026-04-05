"""Téléchargement et parsing de fichiers tabulaires avec fallback data.gouv.fr."""

from __future__ import annotations

import io
import logging
import re
from typing import Any
from urllib.parse import urlparse

import httpx
import pandas as pd

from app import cache
from app.config import settings

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = {"csv", "xls", "xlsx", "json", "parquet", "ods"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

CONTENT_TYPE_MAP = {
    "text/csv": "csv",
    "application/csv": "csv",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/json": "json",
    "application/octet-stream": None,  # fallback to extension / magic bytes
    "application/parquet": "parquet",
    "application/vnd.apache.parquet": "parquet",
    "application/vnd.oasis.opendocument.spreadsheet": "ods",
}

# Magic bytes pour détection de format
MAGIC_SIGNATURES = [
    (b"PK\x03\x04", "xlsx"),  # ZIP-based (xlsx ou ods, on tente xlsx d'abord)
    (b"\xd0\xcf\x11\xe0", "xls"),  # OLE2 Compound Document
    (b"PAR1", "parquet"),
]

DATAGOUV_RESOURCE_RE = re.compile(
    r"data\.gouv\.fr/(?:fr/)?datasets/r/([0-9a-f-]{36})"
)
DATAGOUV_DATASET_RE = re.compile(
    r"data\.gouv\.fr/(?:fr/)?datasets/([^/]+)"
)


def detect_format_from_url(url: str) -> str | None:
    path = urlparse(url).path.rstrip("/")
    ext = path.rsplit(".", 1)[-1].lower() if "." in path else None
    if ext in SUPPORTED_FORMATS:
        return ext
    return None


def detect_format_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    ct = content_type.split(";")[0].strip().lower()
    return CONTENT_TYPE_MAP.get(ct)


def detect_format_from_magic(data: bytes) -> str | None:
    for sig, fmt in MAGIC_SIGNATURES:
        if data[:len(sig)] == sig:
            return fmt
    # Heuristique CSV : les premiers octets ressemblent à du texte avec des séparateurs
    try:
        head = data[:2048].decode("utf-8", errors="strict")
        if any(sep in head for sep in [",", ";", "\t"]) and "\n" in head:
            return "csv"
    except (UnicodeDecodeError, ValueError):
        pass
    return None


def detect_format(url: str, content_type: str | None, data: bytes) -> str | None:
    return (
        detect_format_from_url(url)
        or detect_format_from_content_type(content_type)
        or detect_format_from_magic(data)
    )


def extract_resource_id(url: str) -> str | None:
    m = DATAGOUV_RESOURCE_RE.search(url)
    return m.group(1) if m else None


def extract_dataset_id(url: str) -> str | None:
    m = DATAGOUV_DATASET_RE.search(url)
    return m.group(1) if m else None


async def _download(client: httpx.AsyncClient, url: str) -> tuple[bytes, str | None, str]:
    """Télécharge une URL avec retry sur 5xx. Retourne (data, content_type, final_url)."""
    max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    last_error = None

    for attempt in range(2):
        try:
            resp = await client.get(
                url,
                follow_redirects=True,
                timeout=settings.DOWNLOAD_TIMEOUT_SECONDS,
            )
            if resp.status_code >= 500 and attempt == 0:
                import asyncio
                await asyncio.sleep(2)
                continue
            resp.raise_for_status()

            content_length = resp.headers.get("content-length")
            if content_length and int(content_length) > max_size:
                raise FileTooLargeError(
                    int(content_length) / (1024 * 1024),
                    settings.MAX_FILE_SIZE_MB,
                )

            data = resp.content
            if len(data) > max_size:
                raise FileTooLargeError(
                    len(data) / (1024 * 1024),
                    settings.MAX_FILE_SIZE_MB,
                )

            content_type = resp.headers.get("content-type")
            final_url = str(resp.url)
            return data, content_type, final_url

        except httpx.HTTPStatusError as e:
            last_error = f"{url} → {e.response.status_code}"
            if e.response.status_code >= 500 and attempt == 0:
                import asyncio
                await asyncio.sleep(2)
                continue
            raise DownloadError(last_error) from e
        except httpx.TimeoutException as e:
            last_error = f"{url} → timeout"
            raise DownloadError(last_error) from e
        except (httpx.TooManyRedirects, httpx.ConnectError, httpx.RequestError) as e:
            last_error = f"{url} → {type(e).__name__}"
            raise DownloadError(last_error) from e

    raise DownloadError(last_error or f"{url} → échec après retry")


async def load_file(
    url: str,
    sheet: str | None = None,
    dataset_id: str | None = None,
    resource_id: str | None = None,
) -> tuple[pd.DataFrame, str, str | None, list[str] | None]:
    """
    Télécharge et parse un fichier tabulaire.

    Retourne (df, format, filename, sheets_list).
    Implémente la stratégie de fallback pour les URLs cassées.
    """
    # Check cache first
    cached = cache.get(url)
    tried_urls: list[str] = []

    client_kwargs: dict[str, Any] = {
        "headers": {"User-Agent": USER_AGENT},
        "follow_redirects": True,
        "max_redirects": 5,
    }

    async with httpx.AsyncClient(**client_kwargs) as client:
        data = None
        content_type = None
        effective_url = url

        if cached:
            data = cached
            meta = cache.get_meta(url)
            content_type = meta.get("content_type") if meta else None
            effective_url = meta.get("effective_url") or url if meta else url
        else:
            # Stratégie de fallback
            urls_to_try = [url]

            # Extraire IDs si pas fournis
            if not resource_id:
                resource_id = extract_resource_id(url)
            if not dataset_id:
                dataset_id = extract_dataset_id(url)

            for try_url in urls_to_try:
                try:
                    data, content_type, final = await _download(client, try_url)
                    effective_url = final
                    break
                except (DownloadError, FileTooLargeError) as e:
                    tried_urls.append(str(e))
                    continue

            # Fallback 1: API data.gouv.fr resource
            if data is None and resource_id and dataset_id:
                try:
                    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/"
                    resp = await client.get(api_url, timeout=10)
                    resp.raise_for_status()
                    info = resp.json()
                    for key in ("latest", "url"):
                        alt_url = info.get(key)
                        if alt_url and alt_url != url:
                            try:
                                data, content_type, final = await _download(client, alt_url)
                                effective_url = final
                                break
                            except (DownloadError, FileTooLargeError) as e:
                                tried_urls.append(str(e))
                except Exception:
                    pass

            # Fallback 2: proxy data.gouv.fr
            if data is None and resource_id:
                proxy_url = f"https://www.data.gouv.fr/fr/datasets/r/{resource_id}"
                if proxy_url != url:
                    try:
                        data, content_type, final = await _download(client, proxy_url)
                        effective_url = final
                    except (DownloadError, FileTooLargeError) as e:
                        tried_urls.append(str(e))

            # Fallback 3: autres resources du même dataset
            if data is None and dataset_id:
                try:
                    ds_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
                    resp = await client.get(ds_url, timeout=10)
                    resp.raise_for_status()
                    ds_info = resp.json()
                    target_fmt = detect_format_from_url(url)
                    resources = ds_info.get("resources", [])
                    for res in resources:
                        res_url = res.get("latest") or res.get("url")
                        if not res_url or res_url == url:
                            continue
                        res_fmt = res.get("format", "").lower()
                        if target_fmt and res_fmt != target_fmt:
                            continue
                        try:
                            data, content_type, final = await _download(client, res_url)
                            effective_url = final
                            break
                        except (DownloadError, FileTooLargeError) as e:
                            tried_urls.append(str(e))
                except Exception:
                    pass

            if data is None:
                raise FileUnavailableError(tried_urls)

            # Mettre en cache
            cache.put(url, data, content_type=content_type, effective_url=effective_url)

    # Détection du format
    fmt = detect_format(effective_url, content_type, data)
    if not fmt:
        raise UnsupportedFormatError("inconnu")

    if fmt not in SUPPORTED_FORMATS:
        raise UnsupportedFormatError(fmt)

    # Extraire le nom de fichier
    filename = urlparse(effective_url).path.rstrip("/").rsplit("/", 1)[-1] or "data"
    if "." not in filename:
        filename = f"{filename}.{fmt}"

    # Parser le fichier
    df, sheets = _parse(data, fmt, sheet)
    return df, fmt, filename, sheets


def _parse(
    data: bytes, fmt: str, sheet: str | None
) -> tuple[pd.DataFrame, list[str] | None]:
    """Parse les données brutes en DataFrame."""
    buf = io.BytesIO(data)
    sheets_list: list[str] | None = None

    if fmt == "csv":
        # Tenter plusieurs séparateurs
        text = data.decode("utf-8", errors="replace")
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(io.StringIO(text), sep=sep)
                if len(df.columns) > 1:
                    return df, None
            except Exception:
                continue
        df = pd.read_csv(io.StringIO(text))
        return df, None

    elif fmt in ("xls", "xlsx"):
        xls = pd.ExcelFile(buf)
        sheets_list = xls.sheet_names
        target_sheet = sheet if sheet and sheet in sheets_list else sheets_list[0]
        df = pd.read_excel(xls, sheet_name=target_sheet)
        return df, sheets_list

    elif fmt == "ods":
        xls = pd.ExcelFile(buf, engine="odf")
        sheets_list = xls.sheet_names
        target_sheet = sheet if sheet and sheet in sheets_list else sheets_list[0]
        df = pd.read_excel(xls, sheet_name=target_sheet, engine="odf")
        return df, sheets_list

    elif fmt == "json":
        df = pd.read_json(buf)
        return df, None

    elif fmt == "parquet":
        df = pd.read_parquet(buf)
        return df, None

    raise UnsupportedFormatError(fmt)


def load_file_from_bytes(
    data: bytes,
    filename: str,
    content_type: str | None = None,
    sheet: str | None = None,
) -> tuple[pd.DataFrame, str, str, list[str] | None]:
    """Parse un fichier déjà téléchargé (upload direct). Retourne (df, format, filename, sheets)."""
    fmt = (
        detect_format_from_url(filename)
        or detect_format_from_content_type(content_type)
        or detect_format_from_magic(data)
    )
    if not fmt or fmt not in SUPPORTED_FORMATS:
        raise UnsupportedFormatError(fmt or "inconnu")

    df, sheets = _parse(data, fmt, sheet)
    return df, fmt, filename, sheets


# --- Exceptions ---

class DownloadError(Exception):
    pass


class FileTooLargeError(Exception):
    def __init__(self, actual_mb: float, max_mb: int):
        self.actual_mb = actual_mb
        self.max_mb = max_mb
        super().__init__(f"Fichier trop gros : {actual_mb:.0f} Mo (limite : {max_mb} Mo)")


class FileUnavailableError(Exception):
    def __init__(self, tried_urls: list[str]):
        self.tried_urls = tried_urls
        super().__init__(f"Fichier inaccessible. URLs testées : {tried_urls}")


class UnsupportedFormatError(Exception):
    def __init__(self, fmt: str):
        self.fmt = fmt
        super().__init__(
            f"Format '{fmt}' non supporté. "
            f"Formats acceptés : {', '.join(sorted(SUPPORTED_FORMATS))}."
        )
