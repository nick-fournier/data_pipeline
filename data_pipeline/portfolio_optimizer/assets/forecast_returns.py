
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import numpy as np
import polars as pl
from dagster import asset
from plotly import express as px
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.neural_network import MLPRegressor
from statsmodels.api import WLS

from ..resources.configs import Params

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
    scores: pl.DataFrame,
    stock_prices: pl.DataFrame,
    ) -> pl.DataFrame:
    """Prepare the data for forecasting expected returns.

    Args:
        fundamentals (pl.DataFrame): The financial fundamentals for each security
        scores (pl.DataFrame): The Piotroski F-Score for each security
        stock_prices (pl.DataFrame): The monthly stock prices for each security

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


@asset(
    description="Forecasted expected returns for each security",
    io_manager_key="pgio_manager",
)
def forecasted_returns(
    fundamentals: pl.DataFrame,
    scores: pl.DataFrame,
    security_price: pl.DataFrame,
    ) -> pl.DataFrame:
    """
    Forecasted expected returns for each security based on
    the Piotroski F-Score and financial fundamentals.

    Args:
        fundamentals (pl.DataFrame): The financial fundamentals for each security
        scores (pl.DataFrame): The Piotroski F-Score for each security
        security_prices (pl.DataFrame): The monthly stock prices for each security

    Returns:
        pl.DataFrame: The forecasted expected returns for each security
    """

    # Get the prepared data
    model_data = prepare_data(fundamentals, scores, security_price)

    # Separate the future prediction rows from the training rows
    est_data = model_data.filter(pl.col('next_quarterly_yield').is_not_null())
    pred_data = model_data.filter(pl.col('next_quarterly_yield').is_null())

    # Drop/fill NA and normalize
    # model_data = model_data.drop_nulls()

    # Normalize financial data on z-score
    x_train = est_data.with_columns(
        **{
            k: z_score(pl.col(k)) for k in X_COLS
        }
    )

    x_pred = pred_data.with_columns(
        **{
            k: z_score(pl.col(k)) for k in X_COLS
        }
    ).drop('next_quarterly_yield')


    # Setup test and train data
    x_train = x_train.select(X_COLS).to_pandas()
    # x['intercept'] = 1
    y_train = est_data['next_quarterly_yield'].to_numpy()
    w_train = est_data['r_squared'].to_numpy()


    # Setup test data
    x_pred = pred_data.select(X_COLS).to_pandas()
    w_pred = pred_data['r_squared'].to_numpy()

    # Estimate model
    # model = WLS(y, x, weights=w).fit() # type: ignore
    model = MLPRegressor(
        hidden_layer_sizes=(100, 50),
        activation='tanh',
        solver='sgd',
        alpha=0.01,
        learning_rate_init=0.01,
        max_iter=500,
        random_state=42,
    )

    # Fit the model
    model.fit(x_train, y_train)

    # Create predictions dataframe with fundamentals_id
    predicted = pl.Series(model.predict(x_pred))
    expected_returns = pred_data.with_columns(
        expected_returns=predicted,
    )

    if False:
        # Baseline model
        ols_fit = WLS(y_train, x_train, weights=w_train).fit() # type: ignore
        print(ols_fit.summary())

        # Neural network model
        mlp = MLPRegressor(random_state=42)

        param_distributions = {
            'hidden_layer_sizes': [
                (50, 50), (100, 50), (100, 100), (100, 100, 100), (50, 50, 50), (100, 100, 100)
                ],
            'activation': ['relu', 'tanh'],
            'solver': ['adam', 'sgd'],
            'alpha': [0.0001, 0.001, 0.01],
            'learning_rate_init': [0.001, 0.01, 0.1],
            'max_iter': [200, 500],
        }

        random_search = RandomizedSearchCV(
            estimator=mlp,
            param_distributions=param_distributions,
            n_iter=50,  # Limit number of combinations to test
            cv=3,
            scoring='neg_mean_squared_error',
            n_jobs=-1,
            random_state=42,
        )
        random_search.fit(x_pred, y_train)
        random_search.best_params_

        nn_model = random_search.best_estimator_

        # Scatter plot of actual vs predicted
        compare_ols = pl.DataFrame({'actual': y_train, 'predicted': ols_fit.predict(x_train)})
        px.scatter(compare_ols, x='actual', y='predicted').show()

        compare_nn = pl.DataFrame({'actual': y_train, 'predicted': nn_model.predict(x_train)})
        px.scatter(compare_nn, x='actual', y='predicted').show()

    return expected_returns
