"""Tests pour query_engine.py — LLM mocké."""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from app.models import OperationStep, QueryPlan
from app.query_engine import translate_question, run_query, QueryTranslationError


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "code_commune": ["01001", "93001", "93002", "75056", "93003"],
        "nom_commune": ["L'Abergement", "Aubervilliers", "Saint-Denis", "Paris", "Saint-Ouen"],
        "population": [789, 87000, 113024, 2161000, 51000],
        "departement": ["01", "93", "93", "75", "93"],
    })


def _mock_llm_response(plan: dict) -> httpx.Response:
    return httpx.Response(
        status_code=200,
        content=json.dumps({
            "choices": [{"message": {"content": json.dumps(plan)}}]
        }).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("POST", "https://api.scaleway.ai/v1/chat/completions"),
    )


@pytest.mark.asyncio
async def test_top_n():
    df = pd.DataFrame({
        "nom_commune": ["A", "B", "C", "D"],
        "population": [100, 500, 300, 200],
    })

    plan = {"steps": [{"op": "top_n", "col": "population", "n": 2}]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(df, "Les 2 communes les plus peuplées")

    assert len(result_df) == 2
    assert result_df.iloc[0]["nom_commune"] == "B"


@pytest.mark.asyncio
async def test_filter_and_count(sample_df):
    plan = {"steps": [
        {"op": "filter", "col": "departement", "operator": "==", "value": "93"},
        {"op": "count"},
    ]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(sample_df, "Combien de communes dans le 93 ?")

    assert result_df.iloc[0]["count"] == 3


@pytest.mark.asyncio
async def test_group_mean(sample_df):
    plan = {"steps": [
        {"op": "group_mean", "group_col": "departement", "mean_col": "population"},
    ]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(sample_df, "Population moyenne par département")

    assert len(result_df) == 3  # 3 départements
    assert "population" in result_df.columns


@pytest.mark.asyncio
async def test_search(sample_df):
    plan = {"steps": [
        {"op": "search", "col": "nom_commune", "text": "Saint"},
    ]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(sample_df, "Communes contenant 'Saint'")

    assert len(result_df) == 2
    assert all("Saint" in name for name in result_df["nom_commune"])


@pytest.mark.asyncio
async def test_invalid_json_from_llm():
    invalid_response = httpx.Response(
        status_code=200,
        content=json.dumps({
            "choices": [{"message": {"content": "This is not JSON"}}]
        }).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("POST", "https://api.scaleway.ai/v1/chat/completions"),
    )

    df = pd.DataFrame({"col": [1, 2, 3]})

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=invalid_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        with pytest.raises(QueryTranslationError):
            await run_query(df, "Question invalide")


@pytest.mark.asyncio
async def test_columns_with_accents():
    df = pd.DataFrame({
        "Département": ["Île-de-France", "Provence-Alpes"],
        "Résultat": [100, 200],
    })

    plan = {"steps": [{"op": "sort", "col": "Résultat", "ascending": False}]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(df, "Trier par résultat décroissant")

    assert result_df.iloc[0]["Résultat"] == 200


@pytest.mark.asyncio
async def test_null_values():
    df = pd.DataFrame({
        "nom": ["A", "B", None, "D"],
        "valeur": [10, None, 30, 40],
    })

    plan = {"steps": [{"op": "filter", "col": "valeur", "operator": ">", "value": 20}]}

    with patch("app.query_engine.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_mock_llm_response(plan))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_cls.return_value = mock_client

        result_df, ops = await run_query(df, "Valeurs supérieures à 20")

    assert len(result_df) == 2
