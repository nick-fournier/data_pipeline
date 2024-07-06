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
import polars as pl
from dagster import asset


def calc_measures(fundamentals: pl.DataFrame) -> pl.DataFrame:
    """Calculate the financial measures for the Piotroski F-Score.

    Profitability
    - ROA = Net Income / Total Assets | 1 if positive
    - Cash Flow | 1 if positive
    - Change in ROA | 1 if positive (greater than last year)
    - Accruals | Score 1 if CFROA > ROA

    Leverage, Liquidity and Source of Funds
    - Long term leverage ratio | 1 if negative (lower than last year)
    - Current leverage ratio | 1 point if positive (higher than last year)
    - Change in shares | 1 if no no shares (<=0)

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
    # Define the financial measures
    roa = (pl.col("net_income") / pl.col("total_assets")).alias("roa")

    delta_cash = pl.col("cash").pct_change().alias("delta_cash")

    delta_roa = pl.col("roa").pct_change().alias("delta_roa")

    accruals = (pl.col("cash") / pl.col("current_assets")).alias("accruals")

    delta_long_lev_ratio = (
        pl.col("total_liabilities") / pl.col("total_assets")
    ).pct_change().alias("delta_long_lev_ratio")

    delta_current_lev_ratio = (
        pl.col("current_liabilities") / pl.col("current_assets")
    ).pct_change().alias("delta_current_lev_ratio")

    delta_shares = pl.col("shares_outstanding").pct_change().alias("delta_shares")

    delta_gross_margin = (
        pl.col("gross_profit") / pl.col("total_revenue")
    ).pct_change().alias("delta_gross_margin")

    delta_asset_turnover = (
        pl.col("total_revenue") / (
            pl.col("total_assets") + pl.col("total_assets").shift(-1)
        ) / 2
    ).alias("delta_asset_turnover")

    cash_ratio = (pl.col("cash") / pl.col("current_assets")).alias("cash_ratio")

    eps = (pl.col("net_income") / pl.col("shares_outstanding")).alias("eps")

    book_value = (
        pl.col("total_assets") - pl.col("total_liabilities")
    ).alias("book_value")

    # Return the calculated financial measures
    return fundamentals.with_columns(
        [
            roa,
            delta_cash,
            delta_roa,
            accruals,
            delta_long_lev_ratio,
            delta_current_lev_ratio,
            delta_shares,
            delta_gross_margin,
            delta_asset_turnover,
            cash_ratio,
            eps,
            book_value,
        ],
    )


@asset
def piotroski_scores(updated_fundamentals: pl.DataFrame) -> pl.DataFrame:
    """Calculate the Piotroski F-Score for a given stock symbol.

    Args:
    ----
        updated_fundamentals (pl.DataFrame): The updated fundamentals data for the stock symbols

    Returns:
    -------
        pl.DataFrame: The Piotroski F-Score for the stock symbols

    """
    measures_df = calc_measures(updated_fundamentals)

    return measures_df
