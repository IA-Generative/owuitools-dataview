"""Exécution sécurisée des opérations pandas — whitelist stricte, pas d'exec/eval."""

from __future__ import annotations

import asyncio
import logging
import operator
from typing import Any

import pandas as pd

from app.config import settings
from app.models import OperationStep

logger = logging.getLogger(__name__)

# Opérateurs de comparaison supportés
OP_MAP = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
}


def _validate_column(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        raise SandboxError(f"Colonne '{col}' introuvable. Colonnes disponibles : {list(df.columns)}")


def _validate_columns(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        _validate_column(df, c)


def _check_memory(df: pd.DataFrame) -> None:
    mem_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    if mem_mb > 200:
        raise SandboxError(f"DataFrame trop volumineux ({mem_mb:.0f} Mo, limite 200 Mo)")


def _op_filter(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    op_str = step.operator or "=="

    if op_str == "contains":
        return df[df[step.col].astype(str).str.contains(str(step.value), case=False, na=False)]
    elif op_str == "startswith":
        return df[df[step.col].astype(str).str.startswith(str(step.value), na=False)]
    elif op_str in OP_MAP:
        return df[OP_MAP[op_str](df[step.col], step.value)]
    else:
        raise SandboxError(f"Opérateur '{op_str}' non supporté. Supportés : {list(OP_MAP.keys()) + ['contains', 'startswith']}")


def _op_sort(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    asc = step.ascending if step.ascending is not None else True
    return df.sort_values(step.col, ascending=asc)


def _op_top_n(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    n = step.n or 10
    return df.nlargest(n, step.col)


def _op_bottom_n(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    n = step.n or 10
    return df.nsmallest(n, step.col)


def _op_group_count(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    return df.groupby(step.col).size().reset_index(name="count")


def _op_group_sum(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.group_col)
    _validate_column(df, step.sum_col)
    return df.groupby(step.group_col)[step.sum_col].sum().reset_index()


def _op_group_mean(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.group_col)
    _validate_column(df, step.mean_col)
    return df.groupby(step.group_col)[step.mean_col].mean().reset_index()


def _op_select_columns(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_columns(df, step.cols)
    return df[step.cols]


def _op_unique_values(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    values = df[step.col].unique().tolist()
    return pd.DataFrame({step.col: values})


def _op_count(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    return pd.DataFrame({"count": [len(df)]})


def _op_describe(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    desc = df[step.col].describe()
    return desc.reset_index().rename(columns={"index": "stat", step.col: "value"})


def _op_search(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    _validate_column(df, step.col)
    text = step.text or step.value or ""
    return df[df[step.col].astype(str).str.contains(str(text), case=False, na=False)]


OPERATIONS = {
    "filter": _op_filter,
    "sort": _op_sort,
    "top_n": _op_top_n,
    "bottom_n": _op_bottom_n,
    "group_count": _op_group_count,
    "group_sum": _op_group_sum,
    "group_mean": _op_group_mean,
    "select_columns": _op_select_columns,
    "unique_values": _op_unique_values,
    "count": _op_count,
    "describe": _op_describe,
    "search": _op_search,
}


def execute_step(df: pd.DataFrame, step: OperationStep) -> pd.DataFrame:
    if step.op not in OPERATIONS:
        raise SandboxError(
            f"Opération '{step.op}' non autorisée. "
            f"Opérations disponibles : {list(OPERATIONS.keys())}"
        )
    _check_memory(df)
    result = OPERATIONS[step.op](df, step)
    logger.info("Exécuté: %s → %d lignes", step.op, len(result))
    return result


async def execute_plan(
    df: pd.DataFrame, steps: list[OperationStep], max_rows: int | None = None
) -> tuple[pd.DataFrame, str]:
    """
    Exécute un plan d'opérations séquentiellement sur le DataFrame.
    Retourne (result_df, operation_description).
    """
    if not steps:
        raise SandboxError("Plan d'opérations vide")

    max_rows = max_rows or settings.MAX_ROWS_OUTPUT
    ops_desc: list[str] = []

    for step in steps:
        try:
            df = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, execute_step, df, step),
                timeout=settings.QUERY_TIMEOUT_SECONDS,
            )
            ops_desc.append(f"{step.op}({step.col or step.group_col or ''})")
        except asyncio.TimeoutError:
            raise SandboxError(
                f"Opération '{step.op}' interrompue : timeout ({settings.QUERY_TIMEOUT_SECONDS}s)"
            )

    operation_str = " → ".join(ops_desc)
    return df.head(max_rows), operation_str


class SandboxError(Exception):
    pass
