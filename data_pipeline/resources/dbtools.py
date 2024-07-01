#!/usr/bin/env python
"""Module contains canned database query functions for passing through SSH tunnel."""
from __future__ import annotations

import polars as pl
import psycopg2


# Define function to be used in the PostgresConfig.tunneled method
def _query_table(uri: str, query: str) -> pl.DataFrame:
    return pl.read_database_uri(query, uri)


def _read_table(uri: str, table_name: str) -> pl.DataFrame:
    query = f"SELECT * FROM {table_name}"  # noqa: S608
    return pl.read_database_uri(query, uri)


def _append_new_data(
    uri: str,
    table_name: str,
    new_data: pl.DataFrame,
    pk: str | list[str] | None = None,
    ) -> None:
    if new_data.is_empty():
        return

    if pk:
        pk = pk if isinstance(pk, list) else [pk]
        existing_keys = pl.read_database_uri(
            f"SELECT {', '.join(pk)} FROM {table_name}",  # noqa: S608
            uri,
        ).with_columns(
            status=pl.lit("existing"),
        )
        if not existing_keys.is_empty():
            _new_data = (
                new_data.join(existing_keys, on=pk, how="left")
                .filter(pl.col("status").is_null())
                .drop("status")
            )

    _new_data.write_database(table_name, uri, if_table_exists="append")


def _remove_stocks(uri: str, table_name: str, removed_stocks: pl.Series) -> None:
    if removed_stocks.is_empty():
        return
    query = "DELETE FROM %s WHERE symbol IN %s"
    query = query % (table_name, f"({', '.join(['%s']*len(removed_stocks))})")
    with psycopg2.connect(uri) as conn, conn.cursor() as cur:
        cur.execute(query, (tuple(removed_stocks),))
        conn.commit()
