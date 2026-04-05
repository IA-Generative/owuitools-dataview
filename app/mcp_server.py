"""Serveur MCP exposant 3 tools via FastMCP."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from app.file_loader import (
    FileUnavailableError,
    FileTooLargeError,
    UnsupportedFormatError,
    load_file,
)
from app.models import ColumnSchema
from app.query_engine import QueryExecutionError, QueryTranslationError, run_query

import pandas as pd

mcp = FastMCP("dataview", instructions="Outils pour interroger des fichiers tabulaires (CSV, Excel, JSON, Parquet) en langage naturel.")


@mcp.tool()
async def data_preview(url: str, sheet: str = "") -> str:
    """Aperçu d'un fichier tabulaire : colonnes, types, premières lignes.

    Args:
        url: URL du fichier à analyser (CSV, XLS, XLSX, JSON, Parquet, ODS)
        sheet: Nom de la feuille pour les fichiers Excel multi-feuilles (optionnel)
    """
    try:
        df, fmt, filename, sheets = await load_file(url, sheet=sheet or None)
    except (FileUnavailableError, FileTooLargeError, UnsupportedFormatError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    result = {
        "filename": filename,
        "format": fmt,
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "preview": df.head(5).to_dict(orient="records"),
    }
    if sheets:
        result["sheets"] = sheets

    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def data_schema(url: str) -> str:
    """Schéma détaillé d'un fichier : colonnes, types, stats, valeurs uniques.

    Args:
        url: URL du fichier à analyser
    """
    try:
        df, fmt, filename, sheets = await load_file(url)
    except (FileUnavailableError, FileTooLargeError, UnsupportedFormatError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = int(df[col].isnull().sum())
        info: dict = {"name": col, "dtype": dtype, "null_count": null_count}

        if pd.api.types.is_numeric_dtype(df[col]):
            if not df[col].isnull().all():
                info["min"] = df[col].min()
                info["max"] = df[col].max()
                info["mean"] = float(df[col].mean())
        else:
            info["unique_count"] = int(df[col].nunique())
            info["sample_values"] = df[col].dropna().unique()[:4].tolist()

        columns.append(info)

    result = {"columns": columns, "row_count": len(df)}
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def data_query(url: str, question: str, max_rows: int = 50) -> str:
    """Interroge un fichier tabulaire en langage naturel.

    Args:
        url: URL du fichier à interroger
        question: Question en langage naturel (ex: "Quelles sont les 10 communes les plus peuplées ?")
        max_rows: Nombre maximum de lignes en sortie (défaut: 50)
    """
    try:
        df, fmt, filename, sheets = await load_file(url)
    except (FileUnavailableError, FileTooLargeError, UnsupportedFormatError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    try:
        result_df, operation_desc = await run_query(df, question, max_rows)
    except (QueryTranslationError, QueryExecutionError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    result = {
        "question": question,
        "operation": operation_desc,
        "result": result_df.to_dict(orient="records"),
        "row_count": len(result_df),
        "truncated": len(result_df) >= max_rows,
    }
    return json.dumps(result, ensure_ascii=False, default=str)
