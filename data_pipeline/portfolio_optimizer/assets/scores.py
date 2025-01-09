#!/usr/bin/env python
"""Calculate the Piotroski F-Score for a given stock symbol.

Piotroski F-Score:
-------------------
The Piotroski F-Score is a financial scoring system that uses nine accounting-based
metrics to determine the strength of a company's financial position. The score is
calculated by summing up the individual scores for each metric. The metrics are
grouped into three categories: profitability, leverage, liquidity, and source of funds,
and operating efficiency. The F-Score ranges from 0 to 9, with a higher score indicating
a stronger financial position.

### Profitability
- ROA = Net Income / Total Assets
- Operating Cash Flow = Total Cash From Operating Activities |cash
- Change in ROA = Delta ROA
- Accruals = (Total Assets - Cash) - (Total Liab - Total Debt)

### Leverage, Liquidity and Source of Funds
- Change in long term leverage = delta Total Liab / Total Assets
- Change in current lev = delta Total Current Liabilities / Total Current Assets
- Change in shares = delta Common Stock

### Operating Efficiency
- Change in Gross Margin = delta Gross Profit / Total Revenue
- Change in Asset Turnover Ratio = (
    delta Total Revenue / (Beginning Total Assets + Ending Total Assets) / 2
    )

### EPS & P/E ratio
- EPS = (Net Income - Preferred Dividends) / Common Stock
- P/E = Price / EPS

"""
from __future__ import annotations

import polars as pl
from dagster import asset

from ..resources.dbconn import PostgresConfig
from ..resources.dbtools import _append_data, _read_table
from ..resources.models import Scores

NEG_SCORE_COLS = ("delta_long_lev_ratio", "delta_shares")
POS_SCORE_COLS = (
            "roa",
            "delta_cash",
            "delta_roa",
            "accruals",
            "delta_current_lev_ratio",
            "delta_gross_margin",
            "delta_asset_turnover",
)

class PFScoreMeasures:
    """Column score function expressions."""

    roa: pl.Expr = pl.col("net_income") / pl.col("total_assets")
    delta_cash: pl.Expr = pl.col("cash_and_cash_equivalents").pct_change()
    delta_roa: pl.Expr = pl.col("roa").pct_change()
    accruals: pl.Expr = pl.col("cash_and_cash_equivalents") / pl.col("current_assets")
    delta_long_lev_ratio: pl.Expr = (
        pl.col("total_liabilities_net_minority_interest") / pl.col("total_assets")
        ).pct_change()
    delta_current_lev_ratio: pl.Expr = (
        pl.col("current_liabilities") / pl.col("current_assets")
        ).pct_change()
    delta_shares: pl.Expr = pl.col("capital_stock").pct_change()
    delta_gross_margin: pl.Expr = (
        pl.col("gross_profit") / pl.col("total_revenue")
        ).pct_change()
    delta_asset_turnover: pl.Expr = (
        pl.col("total_revenue") / (pl.col("total_assets") + pl.col("total_assets").shift(-1)) / 2
        )
    cash_ratio: pl.Expr = pl.col("cash_and_cash_equivalents") / pl.col("current_assets")
    eps: pl.Expr = pl.col("net_income") / pl.col("capital_stock")
    book_value: pl.Expr = pl.col("total_assets") - pl.col("total_liabilities_net_minority_interest")


def calc_pf_measures(fundamentals: pl.DataFrame) -> pl.DataFrame:
    """Calculate the financial measures for the Piotroski F-Score.

    Profitability
    - ROA = Net Income / Total Assets | 1 if positive
    - Cash Flow | 1 if positive
    - Change in ROA | 1 if positive (greater than last year)
    - Accruals | Score 1 if CFROA > ROA

    Leverage, Liquidity and Source of Funds
    - Long term leverage ratio | 1 if negative (lower than last year)
    - Current leverage ratio | 1 point if positive (higher than last year)
    - Change in shares | 1 if no new shares (<=0)

    Operating Efficiency
    - Gross margin | 1 if positive (higher than last year)
    - Asset turnover | 1 if positive (higher than last year)
    - Book value | 1 if positive (higher than last year)


    Args:
    ----
        fundamentals (pl.DataFrame): The financial fundamentals data

    Returns:
    -------
        pl.DataFrame: The calculated financial measures

    """
    _fundamentals = fundamentals
    for name in PFScoreMeasures.__annotations__:
        named_expr = {name: getattr(PFScoreMeasures, name)}
        _fundamentals = _fundamentals.with_columns(**named_expr)
        _fundamentals = _fundamentals.with_columns(
            **{name: pl.when(pl.col(name).is_infinite()).then(None).otherwise(pl.col(name))},
        )

    return _fundamentals.fill_nan(None)


def z_score_df(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate the z-score for a given DataFrame.

    Args:
    ----
        df (pl.DataFrame): The DataFrame to calculate the z-score for

    Returns:
    -------
        pl.DataFrame: The DataFrame with the z-score calculated

    """
    return df.select([
        ((pl.col(col) - pl.col(col).mean()) / pl.col(col).std()).alias(col)
        for col in df.columns # if pl.col(col).is_numeric()
    ])


@asset(
    description="Calculate the Piotroski F-Score for a given stock symbol.",
    io_manager_key="pgio_manager",
    deps={"fundamentals": "fundamentals"},
)
def scores(context) -> None:
    """Update scores (within SSH Tunnel)

    This is the working function that gets run within the SSH tunnel. This
    avoids having to reconnect repeatedly within this step.

    Args:
    ----
        context (Context): The Dagster context

    Returns:
        pl.DataFrame: The updated scores table.
    """

    # Get database URI
    uri = context.resources.pgio_manager.postgres_config.db_uri()

    # Get (possibly) outdated fundamentals (last updated more than 90 days ago)
    cols = ''
    # Commenting out, only fetch missing records not NULLs
    # if len(Fundamentals.__annotations__.keys()) > 0:
    #     cols = ['%s IS NULL' % k for k in Fundamentals.__annotations__.keys()]
    #     cols = 'OR %s' % ' OR '.join(cols)

    query = """
        SELECT * FROM fundamentals
        WHERE id NOT IN (SELECT fundamentals_id FROM scores)
        %s
    """ % cols

    outdated_fundamentals = pl.read_database_uri(query, uri)

    measures = calc_pf_measures(outdated_fundamentals)

    scoring = {
        **{k: (pl.col(k) > 0).cast(pl.Int8).fill_null(0) for k in POS_SCORE_COLS},
        **{k: (pl.col(k) <= 0).cast(pl.Int8).fill_null(0) for k in NEG_SCORE_COLS},
    }

    measures_matrix = measures.with_columns(
        **{
            k: pl.when(pl.col(k) < 0).then(pl.col(k) * -1).otherwise(pl.col(k))
            for k in NEG_SCORE_COLS
        },
    ).select(POS_SCORE_COLS + NEG_SCORE_COLS)

    score_matrix = measures.with_columns(**scoring).select(POS_SCORE_COLS + NEG_SCORE_COLS)

    updated_scores = (
        measures
        .with_columns(
            pf_score = score_matrix.sum_horizontal(),
            pf_score_weighted = (score_matrix * measures_matrix + 1).sum_horizontal(),
        )
        .rename({"id": "fundamentals_id"})
        .select(Scores.__annotations__.keys())
    )

    # Validate
    updated_scores: pl.DataFrame = Scores.validate(updated_scores) # type: ignore

    # Update scores table
    _append_data(
        uri=uri,
        table_name="scores",
        new_data=updated_scores,
        pk=["fundamentals_id"],
    )

