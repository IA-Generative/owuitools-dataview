from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# --- Requests ---

class PreviewRequest(BaseModel):
    url: str
    sheet: str | None = None
    dataset_id: str | None = None
    resource_id: str | None = None


class SchemaRequest(BaseModel):
    url: str
    sheet: str | None = None
    dataset_id: str | None = None
    resource_id: str | None = None


class QueryRequest(BaseModel):
    url: str
    question: str
    max_rows: int = Field(default=50, le=500)
    sheet: str | None = None
    dataset_id: str | None = None
    resource_id: str | None = None


# --- Responses ---

class PreviewResponse(BaseModel):
    filename: str
    format: str
    rows: int
    columns: list[str]
    dtypes: dict[str, str]
    preview: list[dict[str, Any]]
    sheets: list[str] | None = None


class ColumnSchema(BaseModel):
    name: str
    dtype: str
    unique_count: int | None = None
    sample_values: list[Any] | None = None
    null_count: int = 0
    min: Any | None = None
    max: Any | None = None
    mean: float | None = None


class SchemaResponse(BaseModel):
    columns: list[ColumnSchema]
    row_count: int


class PaginationInfo(BaseModel):
    total: int = 0
    offset: int = 0
    count: int = 0
    has_more: bool = False


class QueryResponse(BaseModel):
    question: str
    operation: str
    result: list[dict[str, Any]]
    row_count: int
    truncated: bool
    pagination: PaginationInfo | None = None


# --- Errors ---

class ErrorResponse(BaseModel):
    error: str
    message: str
    tried_urls: list[str] | None = None
    suggestion: str | None = None
    schema_hint: str | None = None


# --- Query engine internals ---

class OperationStep(BaseModel):
    op: str
    col: str | None = None
    cols: list[str] | None = None
    operator: str | None = None
    value: Any | None = None
    n: int | None = None
    offset: int | None = None
    ascending: bool | None = None
    group_col: str | None = None
    sum_col: str | None = None
    mean_col: str | None = None
    text: str | None = None


class QueryPlan(BaseModel):
    steps: list[OperationStep]
