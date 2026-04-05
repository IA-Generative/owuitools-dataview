"""Tests pour file_loader.py — tous mockés, pas de réseau."""

import io
import json
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from app.file_loader import (
    DownloadError,
    FileUnavailableError,
    FileTooLargeError,
    UnsupportedFormatError,
    detect_format,
    detect_format_from_magic,
    detect_format_from_url,
    load_file,
)


# --- Détection de format ---

class TestDetectFormat:
    def test_csv_from_url(self):
        assert detect_format_from_url("https://example.com/data.csv") == "csv"

    def test_xlsx_from_url(self):
        assert detect_format_from_url("https://example.com/data.xlsx") == "xlsx"

    def test_parquet_from_url(self):
        assert detect_format_from_url("https://example.com/data.parquet") == "parquet"

    def test_no_extension(self):
        assert detect_format_from_url("https://example.com/datasets/r/abc123") is None

    def test_unsupported_extension(self):
        assert detect_format_from_url("https://example.com/data.pdf") is None

    def test_csv_from_magic(self):
        data = b"col1,col2,col3\n1,2,3\n4,5,6\n"
        assert detect_format_from_magic(data) == "csv"

    def test_parquet_from_magic(self):
        assert detect_format_from_magic(b"PAR1" + b"\x00" * 100) == "parquet"

    def test_xls_from_magic(self):
        assert detect_format_from_magic(b"\xd0\xcf\x11\xe0" + b"\x00" * 100) == "xls"

    def test_detect_format_fallback_chain(self):
        # URL sans extension, pas de content-type, mais magic bytes CSV
        data = b"a;b;c\n1;2;3\n"
        assert detect_format("https://example.com/r/abc", None, data) == "csv"


# --- Téléchargement et parsing ---

def _make_csv_bytes() -> bytes:
    return b"code_commune,nom_commune,population,departement\n01001,L'Abergement,789,01\n93001,Aubervilliers,87000,93\n"


def _make_response(data: bytes, status: int = 200, content_type: str = "text/csv") -> httpx.Response:
    return httpx.Response(
        status_code=status,
        content=data,
        headers={"content-type": content_type},
        request=httpx.Request("GET", "https://example.com/data.csv"),
    )


@pytest.mark.asyncio
async def test_load_csv():
    csv_data = _make_csv_bytes()

    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None), \
         patch("app.cache.put"):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=_make_response(csv_data))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        df, fmt, filename, sheets = await load_file("https://example.com/data.csv")

    assert fmt == "csv"
    assert len(df) == 2
    assert "code_commune" in df.columns
    assert sheets is None


@pytest.mark.asyncio
async def test_load_from_cache():
    csv_data = _make_csv_bytes()

    with patch("app.cache.get", return_value=csv_data):
        df, fmt, filename, sheets = await load_file("https://example.com/data.csv")

    assert fmt == "csv"
    assert len(df) == 2


@pytest.mark.asyncio
async def test_404_triggers_fallback():
    csv_data = _make_csv_bytes()

    resp_404 = httpx.Response(
        status_code=404,
        content=b"Not Found",
        request=httpx.Request("GET", "https://www.data.gouv.fr/fr/datasets/r/abc-123"),
    )

    api_response = httpx.Response(
        status_code=200,
        content=json.dumps({"latest": "https://new-url.com/data.csv", "url": "https://old.com"}).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://www.data.gouv.fr/api/1/datasets/ds1/resources/abc-123/"),
    )

    call_count = 0

    async def mock_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if "api/1/datasets" in url:
            return api_response
        if call_count <= 1:
            raise httpx.HTTPStatusError("404", request=resp_404.request, response=resp_404)
        return _make_response(csv_data)

    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None), \
         patch("app.cache.put"):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=mock_get)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        df, fmt, filename, sheets = await load_file(
            "https://www.data.gouv.fr/fr/datasets/r/abc-123",
            dataset_id="ds1",
            resource_id="abc-123",
        )

    assert fmt == "csv"
    assert len(df) == 2


@pytest.mark.asyncio
async def test_timeout_raises_error():
    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        with pytest.raises(FileUnavailableError):
            await load_file("https://example.com/data.csv")


@pytest.mark.asyncio
async def test_file_too_large():
    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None), \
         patch("app.config.settings") as mock_settings:
        mock_settings.MAX_FILE_SIZE_MB = 0  # 0 Mo limit
        mock_settings.DOWNLOAD_TIMEOUT_SECONDS = 30

        big_resp = httpx.Response(
            status_code=200,
            content=b"x" * 1024,
            headers={"content-type": "text/csv", "content-length": str(200 * 1024 * 1024)},
            request=httpx.Request("GET", "https://example.com/big.csv"),
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=big_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        with pytest.raises(FileUnavailableError):
            await load_file("https://example.com/big.csv")


@pytest.mark.asyncio
async def test_retry_on_5xx():
    csv_data = _make_csv_bytes()
    call_count = 0

    resp_503 = httpx.Response(
        status_code=503,
        content=b"Service Unavailable",
        request=httpx.Request("GET", "https://example.com/data.csv"),
    )

    async def mock_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.HTTPStatusError("503", request=resp_503.request, response=resp_503)
        return _make_response(csv_data)

    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None), \
         patch("app.cache.put"), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=mock_get)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        df, fmt, _, _ = await load_file("https://example.com/data.csv")

    assert fmt == "csv"
    assert call_count == 2


@pytest.mark.asyncio
async def test_unsupported_format():
    pdf_data = b"%PDF-1.4 some pdf content"

    with patch("app.file_loader.httpx.AsyncClient") as mock_client_cls, \
         patch("app.cache.get", return_value=None), \
         patch("app.cache.put"):

        resp = httpx.Response(
            status_code=200,
            content=pdf_data,
            headers={"content-type": "application/pdf"},
            request=httpx.Request("GET", "https://example.com/data.pdf"),
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_client

        with pytest.raises(UnsupportedFormatError):
            await load_file("https://example.com/data.pdf")
