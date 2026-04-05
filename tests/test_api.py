"""Tests pour api.py — endpoints REST avec fichier CSV embarqué."""

import io
import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


CSV_DATA = b"code_commune,nom_commune,population,departement\n01001,L'Abergement,789,01\n93001,Aubervilliers,87000,93\n93002,Saint-Denis,113024,93\n75056,Paris,2161000,75\n"


def _mock_load_file(*args, **kwargs):
    df = pd.read_csv(io.BytesIO(CSV_DATA))
    return df, "csv", "test.csv", None


class TestHealthz:
    def test_healthz(self):
        resp = client.get("/healthz")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "csv" in data["formats"]
        assert "xlsx" in data["formats"]


class TestPreview:
    @patch("app.api.load_file", new_callable=AsyncMock, side_effect=_mock_load_file)
    def test_preview_ok(self, mock_load):
        resp = client.post("/preview", json={"url": "https://example.com/data.csv"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["format"] == "csv"
        assert data["rows"] == 4
        assert "code_commune" in data["columns"]
        assert len(data["preview"]) <= 5

    @patch("app.api.load_file", new_callable=AsyncMock)
    def test_preview_file_unavailable(self, mock_load):
        from app.file_loader import FileUnavailableError
        mock_load.side_effect = FileUnavailableError(["url1 → 404"])
        resp = client.post("/preview", json={"url": "https://example.com/gone.csv"})
        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "file_unavailable"

    @patch("app.api.load_file", new_callable=AsyncMock)
    def test_preview_unsupported_format(self, mock_load):
        from app.file_loader import UnsupportedFormatError
        mock_load.side_effect = UnsupportedFormatError("pdf")
        resp = client.post("/preview", json={"url": "https://example.com/data.pdf"})
        assert resp.status_code == 415


class TestSchema:
    @patch("app.api.load_file", new_callable=AsyncMock, side_effect=_mock_load_file)
    def test_schema_ok(self, mock_load):
        resp = client.post("/schema", json={"url": "https://example.com/data.csv"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 4
        col_names = [c["name"] for c in data["columns"]]
        assert "population" in col_names
        # Check numeric column has stats
        pop_col = next(c for c in data["columns"] if c["name"] == "population")
        assert pop_col["min"] is not None


class TestQuery:
    @patch("app.api.load_file", new_callable=AsyncMock, side_effect=_mock_load_file)
    @patch("app.api.run_query", new_callable=AsyncMock)
    def test_query_ok(self, mock_run, mock_load):
        result_df = pd.DataFrame({
            "nom_commune": ["Saint-Denis", "Aubervilliers"],
            "population": [113024, 87000],
        })
        mock_run.return_value = (result_df, "filter(departement) → top_n(population)")

        resp = client.post("/query", json={
            "url": "https://example.com/data.csv",
            "question": "Les communes les plus peuplées du 93",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 2
        assert data["result"][0]["nom_commune"] == "Saint-Denis"

    @patch("app.api.load_file", new_callable=AsyncMock, side_effect=_mock_load_file)
    @patch("app.api.run_query", new_callable=AsyncMock)
    def test_query_translation_error(self, mock_run, mock_load):
        from app.query_engine import QueryTranslationError
        mock_run.side_effect = QueryTranslationError("Incompréhensible", ["col1", "col2"])

        resp = client.post("/query", json={
            "url": "https://example.com/data.csv",
            "question": "blabla",
        })
        assert resp.status_code == 400
        assert resp.json()["detail"]["error"] == "query_failed"
