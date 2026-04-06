"""Routes REST : /healthz, /preview, /schema, /query."""

from __future__ import annotations

import logging

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.config import settings
from app.file_loader import (
    FileUnavailableError,
    FileTooLargeError,
    UnsupportedFormatError,
    load_file,
    load_file_from_bytes,
)
from app.models import (
    ColumnSchema,
    ErrorResponse,
    PreviewRequest,
    PreviewResponse,
    QueryRequest,
    QueryResponse,
    SchemaRequest,
    SchemaResponse,
)
from app.query_engine import QueryExecutionError, QueryTranslationError, run_query

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "service": "dataview",
        "formats": ["csv", "xls", "xlsx", "json", "parquet", "ods"],
    }


async def _load_df(url: str, sheet: str | None, dataset_id: str | None, resource_id: str | None):
    try:
        return await load_file(url, sheet=sheet, dataset_id=dataset_id, resource_id=resource_id)
    except FileUnavailableError as e:
        raise HTTPException(status_code=422, detail=ErrorResponse(
            error="file_unavailable",
            message="Le fichier n'est plus accessible à l'URL indiquée.",
            tried_urls=e.tried_urls,
            suggestion="Essayez avec un autre fichier du même dataset, ou vérifiez l'URL.",
        ).model_dump())
    except FileTooLargeError as e:
        raise HTTPException(status_code=413, detail=ErrorResponse(
            error="file_too_large",
            message=f"Le fichier fait {e.actual_mb:.0f} Mo, la limite est {e.max_mb} Mo.",
            suggestion="Essayez avec un fichier plus petit ou une version filtrée du dataset.",
        ).model_dump())
    except UnsupportedFormatError as e:
        raise HTTPException(status_code=415, detail=ErrorResponse(
            error="unsupported_format",
            message=str(e),
        ).model_dump())


@router.post("/preview", response_model=PreviewResponse)
async def preview(req: PreviewRequest):
    df, fmt, filename, sheets = await _load_df(req.url, req.sheet, req.dataset_id, req.resource_id)

    return PreviewResponse(
        filename=filename,
        format=fmt,
        rows=len(df),
        columns=list(df.columns),
        dtypes={col: str(df[col].dtype) for col in df.columns},
        preview=df.head(5).to_dict(orient="records"),
        sheets=sheets,
    )


async def _load_df_from_upload(file: UploadFile, sheet: str | None):
    data = await file.read()
    max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    if len(data) > max_size:
        raise HTTPException(status_code=413, detail=ErrorResponse(
            error="file_too_large",
            message=f"Le fichier fait {len(data) / (1024 * 1024):.0f} Mo, la limite est {settings.MAX_FILE_SIZE_MB} Mo.",
        ).model_dump())
    try:
        return load_file_from_bytes(data, file.filename or "upload", file.content_type, sheet)
    except UnsupportedFormatError as e:
        raise HTTPException(status_code=415, detail=ErrorResponse(
            error="unsupported_format",
            message=str(e),
        ).model_dump())


@router.post("/preview/upload", response_model=PreviewResponse)
async def preview_upload(file: UploadFile = File(...), sheet: str | None = Form(None)):
    df, fmt, filename, sheets = await _load_df_from_upload(file, sheet)
    return PreviewResponse(
        filename=filename,
        format=fmt,
        rows=len(df),
        columns=list(df.columns),
        dtypes={col: str(df[col].dtype) for col in df.columns},
        preview=df.head(5).to_dict(orient="records"),
        sheets=sheets,
    )


@router.post("/schema/upload", response_model=SchemaResponse)
async def schema_upload(file: UploadFile = File(...), sheet: str | None = Form(None)):
    df, fmt, filename, sheets = await _load_df_from_upload(file, sheet)
    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = int(df[col].isnull().sum())
        col_schema = ColumnSchema(name=col, dtype=dtype, null_count=null_count)
        if pd.api.types.is_numeric_dtype(df[col]):
            col_schema.min = df[col].min().item() if not df[col].isnull().all() else None
            col_schema.max = df[col].max().item() if not df[col].isnull().all() else None
            col_schema.mean = float(df[col].mean()) if not df[col].isnull().all() else None
        else:
            col_schema.unique_count = int(df[col].nunique())
            col_schema.sample_values = df[col].dropna().unique()[:4].tolist()
        columns.append(col_schema)
    return SchemaResponse(columns=columns, row_count=len(df))


@router.post("/query/upload", response_model=QueryResponse)
async def query_upload(file: UploadFile = File(...), question: str = Form(...), max_rows: int = Form(100)):
    df, fmt, filename, sheets = await _load_df_from_upload(file, None)
    try:
        result_df, operation_desc = await run_query(df, question, max_rows)
    except (QueryTranslationError, QueryExecutionError) as e:
        raise HTTPException(status_code=400, detail=ErrorResponse(
            error="query_failed", message=str(e),
        ).model_dump())
    result_records = result_df.to_dict(orient="records")
    return QueryResponse(
        question=question, operation=operation_desc,
        result=result_records, row_count=len(result_records),
        truncated=len(result_df) >= max_rows,
    )


@router.post("/schema", response_model=SchemaResponse)
async def schema(req: SchemaRequest):
    df, fmt, filename, sheets = await _load_df(req.url, req.sheet, req.dataset_id, req.resource_id)

    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = int(df[col].isnull().sum())
        col_schema = ColumnSchema(
            name=col,
            dtype=dtype,
            null_count=null_count,
        )

        if pd.api.types.is_numeric_dtype(df[col]):
            col_schema.min = df[col].min().item() if not df[col].isnull().all() else None
            col_schema.max = df[col].max().item() if not df[col].isnull().all() else None
            col_schema.mean = float(df[col].mean()) if not df[col].isnull().all() else None
        else:
            col_schema.unique_count = int(df[col].nunique())
            col_schema.sample_values = df[col].dropna().unique()[:4].tolist()

        columns.append(col_schema)

    return SchemaResponse(columns=columns, row_count=len(df))


@router.post("/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    df, fmt, filename, sheets = await _load_df(req.url, req.sheet, req.dataset_id, req.resource_id)

    try:
        result_df, operation_desc = await run_query(df, req.question, req.max_rows)
    except QueryTranslationError as e:
        raise HTTPException(status_code=400, detail=ErrorResponse(
            error="query_failed",
            message=f"Impossible de traduire la question en opérations sur les données. {e}",
            schema_hint=f"Colonnes disponibles : {', '.join(e.columns)}",
        ).model_dump())
    except QueryExecutionError as e:
        raise HTTPException(status_code=400, detail=ErrorResponse(
            error="query_failed",
            message=str(e),
            schema_hint=f"Colonnes disponibles : {', '.join(e.columns)}",
        ).model_dump())

    result_records = result_df.to_dict(orient="records")
    truncated = len(result_df) >= req.max_rows

    return QueryResponse(
        question=req.question,
        operation=operation_desc,
        result=result_records,
        row_count=len(result_records),
        truncated=truncated,
    )
