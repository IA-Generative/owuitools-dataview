"""Moteur de requête : traduction NL → plan d'opérations pandas via LLM."""

from __future__ import annotations

import json
import logging

import httpx
import pandas as pd

from app.config import settings
from app.models import OperationStep, QueryPlan
from app.sandbox import execute_plan, SandboxError

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Tu es un assistant qui traduit des questions en langage naturel en un plan d'opérations sur un DataFrame pandas.

Voici le schéma du DataFrame :
{schema}

Opérations disponibles :
- filter: filtre les lignes (col, operator [==, !=, >, <, >=, <=, contains, startswith], value)
- sort: trie (col, ascending: true/false). ascending=true pour A→Z ou petit→grand, false pour Z→A ou grand→petit.
- head: les N lignes du résultat (n, offset). Combine avec sort pour "les N premiers triés par...". offset=50 pour commencer à la position 50.
- top_n: les N plus grandes valeurs NUMERIQUES (col, n). Ne PAS utiliser sur des colonnes texte.
- bottom_n: les N plus petites valeurs NUMERIQUES (col, n). Ne PAS utiliser sur des colonnes texte.
- group_count: compte par groupe (col)
- group_sum: somme par groupe (group_col, sum_col)
- group_mean: moyenne par groupe (group_col, mean_col)
- select_columns: sélectionne des colonnes (cols: [])
- unique_values: valeurs uniques (col)
- count: nombre total de lignes
- describe: statistiques descriptives (col)
- search: recherche textuelle (col, text)

Retourne UNIQUEMENT un JSON valide avec le champ "steps". Chaque step a un champ "op" et les paramètres correspondants.

Exemples :
- "Les 10 communes les plus peuplées" → {{"steps": [{{"op": "top_n", "col": "population", "n": 10}}]}}
- "5 premières lignes triées par ordre alphabétique" → {{"steps": [{{"op": "sort", "col": "nom", "ascending": true}}, {{"op": "head", "n": 5}}]}}
- "Communes du 93 triées par population" → {{"steps": [{{"op": "filter", "col": "departement", "operator": "==", "value": "93"}}, {{"op": "sort", "col": "population", "ascending": false}}]}}
- "Population moyenne par département" → {{"steps": [{{"op": "group_mean", "group_col": "departement", "mean_col": "population"}}]}}
"""


def _build_schema_description(df: pd.DataFrame) -> str:
    lines = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        n_unique = df[col].nunique()
        samples = df[col].dropna().head(3).tolist()
        samples_str = ", ".join(str(s) for s in samples)
        lines.append(f"- {col} ({dtype}, {n_unique} valeurs uniques, ex: {samples_str})")
    return "\n".join(lines)


async def translate_question(df: pd.DataFrame, question: str) -> QueryPlan:
    """Traduit une question NL en plan d'opérations via le LLM."""
    schema_desc = _build_schema_description(df)
    system = SYSTEM_PROMPT.format(schema=schema_desc)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{settings.LLM_API_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.LLM_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.LLM_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": question},
                ],
                "temperature": 0.0,
                "max_tokens": 1024,
            },
            timeout=30,
        )
        resp.raise_for_status()

    data = resp.json()
    content = data["choices"][0]["message"]["content"]

    # Extraire le JSON du contenu (peut être wrappé dans ```json ... ```)
    json_str = content.strip()
    if json_str.startswith("```"):
        json_str = json_str.split("\n", 1)[1]
        json_str = json_str.rsplit("```", 1)[0]
    json_str = json_str.strip()

    try:
        plan_data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise QueryTranslationError(
            f"Le LLM a retourné du JSON invalide : {e}",
            list(df.columns),
        )

    try:
        plan = QueryPlan(**plan_data)
    except Exception as e:
        raise QueryTranslationError(
            f"Plan d'opérations invalide : {e}",
            list(df.columns),
        )

    return plan


async def run_query(
    df: pd.DataFrame, question: str, max_rows: int = 50
) -> tuple[pd.DataFrame, str]:
    """
    Pipeline complet : question → plan → exécution → résultat.
    Retourne (result_df, operation_description).
    """
    plan = await translate_question(df, question)
    try:
        result_df, operation_desc = await execute_plan(df, plan.steps, max_rows)
    except SandboxError as e:
        raise QueryExecutionError(str(e), list(df.columns))
    return result_df, operation_desc


class QueryTranslationError(Exception):
    def __init__(self, message: str, columns: list[str]):
        self.columns = columns
        super().__init__(message)


class QueryExecutionError(Exception):
    def __init__(self, message: str, columns: list[str]):
        self.columns = columns
        super().__init__(message)
