
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import datetime
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import polars as pl
import statsmodels.api as sm
from dagster import asset
from plotly import express as px
from sklearn.model_selection import RandomizedSearchCV
from sklearn.neural_network import MLPRegressor
from statsmodels.tsa.arima.model import ARIMA
from tqdm import tqdm

from data_pipeline.portfolio_optimizer.resources import dbconn
from data_pipeline.portfolio_optimizer.resources.configs import Params
from data_pipeline.portfolio_optimizer.resources.dbtools import _append_data
from data_pipeline.portfolio_optimizer.resources.models import ExpectedReturns

# Model parameters
X_COLS = [
    # 'roa',                      # 0
    'cash_ratio',               # 1
    'delta_cash',               # 2
    # 'delta_roa',                # 3
    'accruals',                 # 4
    # 'delta_long_lev_ratio',     # 5
    # 'delta_current_lev_ratio',  # 6
    'delta_shares',             # 7
    # 'delta_gross_margin',       # 8
    # 'delta_asset_turnover',     # 9
    'quarterly_yield_rate',     # 10
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
    scores: pl.DataFrame,
    security_price: pl.DataFrame,
    **kwargs
    ) -> pl.DataFrame:
    """Prepare the data for forecasting expected returns.

    Args:
        fundamentals (pl.DataFrame): The financial fundamentals for each security
        scores (pl.DataFrame): The Piotroski F-Score for each security
        security_price (pl.DataFrame): The stock prices for each security

    Returns:
        pl.DataFrame: The prepared data for forecasting expected returns
    """

    # Join scores onto fundamentals
    fin_data = (
        scores
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
    stock_yield = calc_average_yield(security_price)

    # Join stock data onto financial data
    model_data = fin_data.join(
        stock_yield,
        left_on=['symbol', 'year', 'quarter'],
        right_on=['symbol', 'year', 'quarter'],
        how='inner'
    )

    # Sort data
    model_data = model_data.sort('symbol', 'as_of_date').filter(
        pl.col('period_type') == '3M'
    )

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

def process_forecast(
    id,
    symbol,
    last_as_of_date,
    security_price,
    ) -> tuple[int, str, float, float, float, float] | None:
    """
    Helper function to process the forecast for a single security.

    Args:
        id (int): The fundamentals ID
        symbol (str): The stock symbol
        last_as_of_date (datetime.date): The last as of date
        security_price (pl.DataFrame): The stock prices for each security

    Returns:
        tuple: A tuple of
            - fundamentals_id,
            - symbol
            - last close price,
            - forecasted close price,
            - expected return, and
            - variance
    """
    price_df = security_price.filter(
        (pl.col('symbol') == symbol) &
        (pl.col('date') <= last_as_of_date),
    )

    # Skip if less than 3 months of data
    min_months = 3
    if price_df.shape[0] < min_months:
        return None

    try:
        return (id, symbol, *arima_forecast(price_df))

    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

def arima_forecast(price_df: pl.DataFrame) -> tuple:
    """Helper function to forecast using ARIMA model

    Args:
        price_df (pl.DataFrame): The price data for a single security

    Returns:
        tuple: A tuple of
            - last close price,
            - forecasted close price,
            - expected return, and
            - variance
    """
    # Sort date
    df = price_df.sort('date')

    # Fit the ARIMA model
    model = ARIMA(df['close'].to_numpy(), order=(3, 2, 0))
    model_fit = model.fit()

    # If model throws a warning, raise an exception
    if model_fit.mle_retvals['converged'] is False:
        raise ValueError("Model did not converge")

    # Make predictions
    yhat = model_fit.forecast(steps=3)[-1]
    last_date = df['date'].max()
    last_close = df.filter(pl.col('date') == last_date)['close'].to_numpy()[0]
    variance = model_fit.resid.var()

    if False:
        # Plot forecast, 3 new rows, 30 days each
        new_date = last_date + datetime.timedelta(days=30*3)
        forecast_df = pl.concat(
            [
                df.with_columns(type=pl.lit('data')).select('date', 'close', 'type'),
                pl.DataFrame(
                    {
                        'date': [last_date, new_date],
                        'close': [last_close, yhat],
                        'type': ['forecast']*2
                    })
            ])

        # Add a month to the last date
        px.line(forecast_df, x='date', y='close', color='type').show()

    return (last_close, yhat, (yhat - last_close) / last_close, variance)


@asset(
    description="Forecasted expected returns for each security",
    io_manager_key="pgio_manager",
)
def expected_returns(
    context,
    fundamentals: pl.DataFrame,
    security_price: pl.DataFrame,
    ) -> None:
    """
    Naive ARIMA model based on security price only

    Args:
        fundamentals (pl.DataFrame): The financial fundamentals for each security
        scores (pl.DataFrame): The Piotroski F-Score for each security
        security_prices (pl.DataFrame): The monthly stock prices for each security

    Returns:
        None - database updated
    """
    # Setup database connection
    pg_config = context.resources.pgio_manager.postgres_config

    # Fetch existing forecasts
    try:
        existing_forecasts = pl.read_database_uri(
            query="SELECT fundamentals_id FROM forecasted_returns",
            uri=pg_config.db_uri(),
        )
    except RuntimeError:
        existing_forecasts = pl.DataFrame(schema={'fundamentals_id': pl.Int64()})

    # Find which fundamentals_id are missing a forecasted value, excluding TTM
    ood_fundamentals = fundamentals.filter(
        ~pl.col('id').is_in(existing_forecasts['fundamentals_id']),
        pl.col('period_type') == '3M',
    )

    # If empty, proceed, nothing to update
    if ood_fundamentals.is_empty():
        return

    # Previous quarter end date
    ood_fundamentals = ood_fundamentals.with_columns(
        last_as_of_date=pl.col('as_of_date').shift(-1).over('symbol')
    )

    # Iterate over fundamentals
    _iter = ood_fundamentals.select(['id', 'symbol', 'last_as_of_date']).iter_rows()
    total = ood_fundamentals['id'].n_unique()
    forecasts = []

    # Process and populate the expected_returns dataframe
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_forecast, id, symbol, last_as_of_date, security_price)
            for id, symbol, last_as_of_date in tqdm(_iter, total=total)
        ]

        for future in tqdm(futures, total=total):
            result = future.result()
            if result:
                forecasts.append(result)


    # Initialize empty dataframe to store forecasts using ExpectedReturns schema
    schema = {k: v.type for k,v in ExpectedReturns.to_schema().dtypes.items()}

    # Create a Polars DataFrame from the list of tuples
    expected_returns = pl.DataFrame(
        forecasts,
        schema=schema,
        orient='row',
    )

    # Drop rows where last_close or forecasted_close is NULL or not finite
    expected_returns_clean = expected_returns.filter(
        pl.col('last_close').is_finite(),
        pl.col('forecasted_close').is_finite()
    )

    # Validate
    expected_returns_clean: pl.DataFrame = ExpectedReturns.validate(
        expected_returns_clean) # type: ignore

    # Update database
    _append_data(
        uri=pg_config.db_uri(),
        table_name="expected_returns",
        new_data=expected_returns_clean,
        pk=["fundamentals_id"],
    )

