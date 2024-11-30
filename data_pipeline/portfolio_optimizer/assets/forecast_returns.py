
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
from ..resources.configs import Params

from plotly import express as px

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

def z_score(x):
    return (x - x.mean()) / x.std()


def calc_average_yield(stock_prices: pl.DataFrame) -> pl.DataFrame:

    # Add date parts
    model_frame = stock_prices.with_columns(
        quarter=pl.col('date').dt.quarter(),
        year=pl.col('date').dt.year(),
        month=pl.col('date').dt.month(),
        month_in_quarter=(pl.col('date').dt.month()-1) % 3 + 1,
        # wk_in_quarter=(pl.col('date').dt.week()-1) % 13 + 1,
    )

    # Keep the minimum day in the month for each invterval and symbol
    if Params().PRICE_INTERVAL == '1mo':
        model_frame = (
            model_frame
            .group_by(
                ['symbol', 'year', 'month']
            )
            .agg(pl.col('date').min())
            # Join back to keep only the minimum date
            .join(
                model_frame,
                on=['symbol', 'year', 'month', 'date'],
                how='inner'
            )
        )
    else:
        raise ValueError("Only monthly data is supported")

    # Pivot to wide
    model_frame = (
        model_frame
        .pivot(
            index='month_in_quarter',
            on=['symbol', 'year', 'quarter'],
            values='close',
            aggregate_function=None,
        )
        .sort('month_in_quarter')
    )


    # Drop columns with NULL values
    # # Get columns with NULL values
    # nonull_cols = [k for k, v in zip(
    #     model_frame.columns,
    #     (model_frame.null_count() == 0).transpose().to_numpy()
    # ) if v]

    # # Drop columns with NULL values
    # model_frame = model_frame.select(nonull_cols)


    # Impute missing values as mean
    model_frame = model_frame.fill_null(strategy='mean')

    # Convert to numpy arrays
    varnames = model_frame[:, 1:].columns
    y = model_frame[:, 1:].to_numpy()
    x = model_frame[:, 0].to_numpy()

    # Step 1: Center X and Y
    x_mean = np.mean(x)
    y_mean = np.nanmean(y, axis=0)

    # Step 2: Calculate slopes (beta) for each column
    # β=(x-x¯)⋅(y-y¯)/∥x-x¯∥2
    slopes = ((x - x_mean) @ (y - y_mean)) / ((x - x_mean).transpose() @ (x - x_mean))

    # Step 3: Calculate intercepts
    intercepts = y_mean - slopes * x_mean

    # Step 4: Compute predictions (hat_Y)
    y_hat = np.outer(x, slopes) + intercepts

    # Step 5: Calculate R² for each column
    ss_res = np.sum((y - y_hat) ** 2, axis=0)  # Residual sum of squares
    ss_tot = np.sum((y - y_mean) ** 2, axis=0)  # Total sum of squares

    # Get index where either ss_res or ss_tot is 0
    zero_idx = np.logical_or(ss_res == 0, ss_tot == 0)

    # Replace 0s in ss_tot with 1 to avoid division by zero
    ss_tot[ss_tot == 0] = 1

    # Calculate R²
    r_squared = 1 - (ss_res / ss_tot)

    # Replace R² with 0 where either ss_res or ss_tot is 0
    r_squared[zero_idx] = 0

    # Parse varnames into symbol, year, and quarter
    parsed_varnames = [v.strip('{}').replace('"', "").split(',') for v in varnames]
    symbols, years, quarters = zip(*parsed_varnames)

    # Create a DataFrame of results
    n_periods = len(x) - 1
    stock_yield = pl.DataFrame(
        {
        'symbol': symbols,
        'year': years,
        'quarter': quarters,
        'slope': slopes,
        'intercept': intercepts,
        'quarterly_yield_rate': n_periods * slopes / intercepts,
        'r_squared': r_squared
        }
    ).with_columns(
        year=pl.col('year').cast(pl.Int32),
        quarter=pl.col('quarter').cast(pl.Int8),
    )

    # Calculate simple yield from first to last close price in the quarter
    # Use this to compare with fitted values
    stock_yield_simple = (
        stock_prices
        .with_columns(
            quarter=pl.col('date').dt.quarter(),
            year=pl.col('date').dt.year()
        )
        .group_by(['symbol', 'quarter', 'year']).agg(
            pl.col('close').first().alias('first_close'),
            pl.col('close').last().alias('last_close'),
            pl.col('close').mean().alias('mean'),
            pl.col('close').var().alias('var'),
            (
                pl.col('close').last() / pl.col('close').first() - 1
            ).alias('quarterly_yield_close'),
            # Normalized mean-variance ratio. Higher means more stable
            (
                pl.col('close').mean() / pl.col('close').var()
            ).alias('dispersion')
        )
    ).select(
        'symbol', 'year', 'quarter', 'quarterly_yield_close', 'dispersion'
    )

    # Join simple yield onto fitted yield
    stock_yield = stock_yield.join(
        stock_yield_simple,
        on=['symbol', 'year', 'quarter'],
        how='inner'
    )

    # plot the fitted yield vs simple yield
    if False:
        px.scatter(
            stock_yield.to_pandas(),
            x='quarterly_yield_rate',
            y='quarterly_yield_close',
            color='r_squared',
        ).show()


    return stock_yield



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
    stock_yield = calc_average_yield(stock_prices)

    # Join stock data onto financial data
    model_data = fin_data.join(
        # stock_yield_simple,
        stock_yield,
        left_on=['symbol', 'year', 'quarter'],
        right_on=['symbol', 'year', 'quarter'],
        how='inner'
    )

    # Sort data
    model_data = model_data.sort('symbol', 'as_of_date')

    # Lag of quarterly yield for next quarter
    model_data = model_data.with_columns(
        pl.col('quarterly_yield_rate')
        .shift(-1)
        .over('symbol')
        .alias('next_quarterly_yield')
    )

    # Exclude cols
    exc_cols = [x for x in model_data.columns if x not in X_COLS]

    # Forward fill and backward fill missing values
    model_data = model_data.with_columns(
            pl.exclude(exc_cols).forward_fill().backward_fill().over('symbol')
        )

    # If any columns are still NULL, it is likely NULL for all rows for that symbol
    # Impute as mean of entire column
    model_data = model_data.with_columns(
        pl.exclude(exc_cols).fill_null(strategy='mean')
        )

    # Check for NULL values in X_COLS
    if not all((model_data.select(X_COLS).null_count() == 0).row(0)):
        raise ValueError("NULL values found in X_COLS")

    return model_data


def _forecast_returns(
    fundamentals: pl.DataFrame,
    piotroski_scores: pl.DataFrame,
    stock_prices: pl.DataFrame,
    ) -> pl.DataFrame:

    # Get the prepared data
    model_data = prepare_data(fundamentals, piotroski_scores, stock_prices)

    # Separate the future prediction rows from the training rows
    est_data = model_data.filter(pl.col('next_quarterly_yield').is_not_null())
    pred_data = model_data.filter(pl.col('next_quarterly_yield').is_null())

    # Drop/fill NA and normalize
    # model_data = model_data.drop_nulls()

    # Normalize financial data on z-score
    est_data = est_data.with_columns(
        **{
            k: z_score(pl.col(k)) for k in X_COLS
        }
    )

    # Get independent variables
    x = est_data.select(X_COLS).to_pandas()
    x['intercept'] = 1
    y = est_data['next_quarterly_yield'].to_numpy()
    w = est_data['r_squared'].to_numpy()

    # Estimate model
    ols_fit = WLS(y, x, weights=w).fit() # type: ignore
    ols_fit.summary()

    if False:
        # Neural network model
        nnmodel = MLPRegressor(
            hidden_layer_sizes=(30,30,30),
            max_iter = 700,
            activation = 'logistic',
            solver = 'adam'
        )

        # Grid search
        param_grid = {
            'hidden_layer_sizes': [(5, 5, 5), (15, 15, 15), (30, 30, 30), (50, 50, 50)],
            'max_iter': [500, 700, 1000],
            'activation': ['relu', 'tanh', 'logistic'],
            'solver': ['adam', 'sgd']
        }

        grid_search = GridSearchCV(nnmodel, param_grid, cv=5)
        grid_search.fit(x, y)

        # Best parameters
        grid_search.best_params_
        nn_fit = nnmodel.fit(x, y)

    # Scatter plot of actual vs predicted
    if False:
        compare_ols = pl.DataFrame({'actual': y, 'predicted': ols_fit.predict(x)})
        px.scatter(compare_ols, x='actual', y='predicted').show()

        compare_nn = pl.DataFrame({'actual': y, 'predicted': nn_fit.predict(x)})
        px.scatter(compare_nn, x='actual', y='predicted').show()


    # Get expected returns
    expected_returns = pred_data.with_columns(
        expected_returns=ols_fit.predict(x)
    )

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
