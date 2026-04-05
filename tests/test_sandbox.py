"""Tests pour sandbox.py — whitelist, timeout, sécurité."""

import asyncio

import pandas as pd
import pytest

from app.models import OperationStep
from app.sandbox import SandboxError, execute_plan, execute_step


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "nom": ["Alice", "Bob", "Charlie", "David"],
        "score": [85, 92, 78, 95],
        "groupe": ["A", "A", "B", "B"],
    })


class TestWhitelist:
    def test_filter_eq(self, sample_df):
        step = OperationStep(op="filter", col="groupe", operator="==", value="A")
        result = execute_step(sample_df, step)
        assert len(result) == 2

    def test_filter_gt(self, sample_df):
        step = OperationStep(op="filter", col="score", operator=">", value=80)
        result = execute_step(sample_df, step)
        assert len(result) == 3

    def test_filter_contains(self, sample_df):
        step = OperationStep(op="filter", col="nom", operator="contains", value="li")
        result = execute_step(sample_df, step)
        assert len(result) == 2  # Alice, Charlie

    def test_sort(self, sample_df):
        step = OperationStep(op="sort", col="score", ascending=False)
        result = execute_step(sample_df, step)
        assert result.iloc[0]["nom"] == "David"

    def test_top_n(self, sample_df):
        step = OperationStep(op="top_n", col="score", n=2)
        result = execute_step(sample_df, step)
        assert len(result) == 2
        assert result.iloc[0]["score"] == 95

    def test_bottom_n(self, sample_df):
        step = OperationStep(op="bottom_n", col="score", n=1)
        result = execute_step(sample_df, step)
        assert len(result) == 1
        assert result.iloc[0]["score"] == 78

    def test_group_count(self, sample_df):
        step = OperationStep(op="group_count", col="groupe")
        result = execute_step(sample_df, step)
        assert len(result) == 2
        assert "count" in result.columns

    def test_group_sum(self, sample_df):
        step = OperationStep(op="group_sum", group_col="groupe", sum_col="score")
        result = execute_step(sample_df, step)
        assert len(result) == 2

    def test_group_mean(self, sample_df):
        step = OperationStep(op="group_mean", group_col="groupe", mean_col="score")
        result = execute_step(sample_df, step)
        assert len(result) == 2

    def test_select_columns(self, sample_df):
        step = OperationStep(op="select_columns", cols=["nom", "score"])
        result = execute_step(sample_df, step)
        assert list(result.columns) == ["nom", "score"]

    def test_unique_values(self, sample_df):
        step = OperationStep(op="unique_values", col="groupe")
        result = execute_step(sample_df, step)
        assert len(result) == 2

    def test_count(self, sample_df):
        step = OperationStep(op="count")
        result = execute_step(sample_df, step)
        assert result.iloc[0]["count"] == 4

    def test_describe(self, sample_df):
        step = OperationStep(op="describe", col="score")
        result = execute_step(sample_df, step)
        assert "stat" in result.columns

    def test_search(self, sample_df):
        step = OperationStep(op="search", col="nom", text="ob")
        result = execute_step(sample_df, step)
        assert len(result) == 1
        assert result.iloc[0]["nom"] == "Bob"


class TestSecurity:
    def test_unknown_op_rejected(self, sample_df):
        step = OperationStep(op="exec_code", col="nom")
        with pytest.raises(SandboxError, match="non autorisée"):
            execute_step(sample_df, step)

    def test_invalid_column(self, sample_df):
        step = OperationStep(op="filter", col="colonne_inexistante", operator="==", value="x")
        with pytest.raises(SandboxError, match="introuvable"):
            execute_step(sample_df, step)

    def test_invalid_operator(self, sample_df):
        step = OperationStep(op="filter", col="nom", operator="DROP TABLE", value="x")
        with pytest.raises(SandboxError, match="non supporté"):
            execute_step(sample_df, step)


class TestPlanExecution:
    @pytest.mark.asyncio
    async def test_multi_step_plan(self, sample_df):
        steps = [
            OperationStep(op="filter", col="groupe", operator="==", value="A"),
            OperationStep(op="sort", col="score", ascending=False),
        ]
        result, desc = await execute_plan(sample_df, steps)
        assert len(result) == 2
        assert result.iloc[0]["score"] == 92

    @pytest.mark.asyncio
    async def test_empty_plan_raises(self, sample_df):
        with pytest.raises(SandboxError, match="vide"):
            await execute_plan(sample_df, [])

    @pytest.mark.asyncio
    async def test_max_rows_limit(self):
        df = pd.DataFrame({"x": range(1000)})
        steps = [OperationStep(op="sort", col="x", ascending=True)]
        result, desc = await execute_plan(df, steps, max_rows=5)
        assert len(result) == 5
