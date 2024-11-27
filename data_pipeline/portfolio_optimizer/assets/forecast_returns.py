
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import numpy as np
import pandas as pd
import polars as pl
from dagster import asset
from scipy import stats
from statsmodels.api import WLS
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV

from ..resources.dbconn import PostgresConfig

# Model parameters
X_COLS = [
    'roa',
    'cash_ratio',
    'delta_cash',
    'delta_roa',
    'accruals',
    'delta_long_lev_ratio',
    'delta_current_lev_ratio',
    'delta_shares',
    'delta_gross_margin',
    'delta_asset_turnover'
    ]

def prepare_data(
    fundamentals: pl.DataFrame,
    piotroski_scores: pl.DataFrame,
    stock_prices: pl.DataFrame,
    ) -> pl.DataFrame:
    """Prepare the data for forecasting expected returns.

    Args:
        fundamentals (pl.DataFrame): The financial fundamentals for each security
        piotroski_scores (pl.DataFrame): The Piotroski F-Score for each security
        stock_prices (pl.DataFrame): The monthly stock prices for each security

    Returns:
        pl.DataFrame: The prepared data for forecasting expected returns
    """

    # Join scores onto fundamentals
    fin_data = (
        piotroski_scores
        .join(
            fundamentals,
            left_on='fundamentals_id',
            right_on='id',
        )
        .with_columns(
            year=pl.col('as_of_date').dt.year(),
            quarter=pl.col('as_of_date').dt.quarter(),
        )
    )

    # Calculate quarterly returns in price data
    price_dat = (
        stock_prices
        .with_columns(
            quarter=pl.col('date').dt.quarter(),
        )
        .group_by(['symbol', 'quarter']).agg(
            pl.col('close').first().alias('first_close'),
            pl.col('close').last().alias('last_close'),
            pl.col('close').mean().alias('mean'),
            pl.col('close').var().alias('var'),
            (
                pl.col('close').last() / pl.col('close').first() - 1
            ).alias('quarterly_yield'),
            # Normalized mean-variance ratio. Higher means more stable
            (
                pl.col('close').mean() / pl.col('close').var()
            ).alias('inv_mean_var_ratio')
        )
    )
    # Join stock data onto financial data
    model_data = fin_data.join(
        price_dat,
        left_on=['symbol', 'quarter'],
        right_on=['symbol', 'quarter'],
    )

    # Sort data
    model_data = model_data.sort('symbol', 'as_of_date')

    # Lag of quarterly yield for next quarter
    model_data = model_data.with_columns(
        pl.col('quarterly_yield').shift(-1).alias('next_quarterly_yield')
    )

    # Drop/fill NA and normalize
    model_data = model_data.drop_nulls()

    return model_data


def _forecast_returns(
    fundamentals: pl.DataFrame,
    piotroski_scores: pl.DataFrame,
    stock_prices: pl.DataFrame,
    # method='nn',
    backcast=False
    ) -> pl.DataFrame:

    # Get the prepared data
    model_data = prepare_data(fundamentals, piotroski_scores, stock_prices)

    # Get independent variables
    x = model_data.select(X_COLS).to_pandas()
    y = model_data['next_quarterly_yield'].to_pandas()
    w = model_data['inv_mean_var_ratio'].to_pandas()

    # Estimate model
    fit = WLS(y, x, weights=w).fit()
    fit.summary()

    # Scatter plot of actual vs predicted
    # from plotly import express as px
    # compare = pl.DataFrame({'actual': y, 'predicted': fit.predict(x)})
    # fig = px.scatter(compare, x='actual', y='predicted')
    # fig.show()


    # Expected returns as percent change
    expected_returns = model_df.apply(lambda x: (x.next_close - x.yearly_close)/x.yearly_close, axis=1)
    expected_returns.name = 'expected_returns'

    return expected_returns


@asset(
    description="Forecasted expected returns for each security",
)
def forecasted_returns(
    stock_prices: pl.DataFrame,
    piotroski_scores: pl.DataFrame,
    fundamentals: pl.DataFrame,
    ) -> pl.DataFrame:
    """
    Forecasted expected returns for each security based on
    the Piotroski F-Score and financial fundamentals.

    Args:
        stock_prices (pl.DataFrame): The monthly stock prices for each security
        piotroski_scores (pl.DataFrame): The Piotroski F-Score for each security
        fundamentals (pl.DataFrame): The financial fundamentals for each security

    Returns:
        pl.DataFrame: _description_
    """

    # Initialize SSH tunnel to Postgres database
    pg_config = PostgresConfig()

    _forecast_returns(
        fundamentals=fundamentals,
        piotroski_scores=piotroski_scores,
        stock_prices=stock_prices,
    )

    # scores = pg_config.tunneled(
    # )

    return pl.DataFrame()
