
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import datetime

import polars as pl
from dagster import asset
from pydantic import BaseModel
from pypfopt.discrete_allocation import DiscreteAllocation
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.objective_functions import L2_reg
from pypfopt.risk_models import CovarianceShrinkage


class OptimizeConfigs(BaseModel):
    investment_amount: int = 10000
    threshold: int = 6
    objective: str = 'max_sharpe'
    l2_gamma: int = 2
    risk_aversion: int = 1
    backcast: bool = False

def quarter_dates(year, quarter):
    quarter_dates = {
        1: (
            datetime.date(year, 1, 1),
            datetime.date(year, 3, 31),
            ),
        2: (
            datetime.date(year, 4, 1),
            datetime.date(year, 6, 30),
            ),
        3: (
            datetime.date(year, 7, 1),
            datetime.date(year, 9, 30),
            ),
        4: (
            datetime.date(year, 10, 1),
            datetime.date(year, 12, 31),
            ),
    }

    return quarter_dates[quarter]


@asset(
    description="Optimize portfolio allocation based on expected returns",
    io_manager_key="pgio_manager",
)
def optimize(
    context,
    expected_returns: pl.DataFrame,
    security_price: pl.DataFrame,
    fundamentals: pl.DataFrame,
    ) -> None:

    # Connection to database
    pg_config = context.resources.pgio_manager.postgres_config

    # Load existing portfolio data
    try:
        existing_portfolios = pl.read_database_uri(
            query="SELECT fundamentals_id FROM portfolio",
            uri=pg_config.db_uri(),
        )

    except RuntimeError:
        existing_portfolios = pl.DataFrame(schema={'fundamentals_id': pl.Int64()})

    # Find missing fundamentals_id in portfolio
    new_expected_returns = expected_returns.filter(
        ~pl.col('fundamentals_id').is_in(existing_portfolios['fundamentals_id'])
    ).join(
        fundamentals.select('id', 'as_of_date', 'quarter'),
        left_on='fundamentals_id',
        right_on='id',
        how='inner'
    )

    # Get price data for the target stocks
    security_ids = new_expected_returns['symbol'].unique()
    prices = security_price.filter(
        pl.col('symbol').is_in(security_ids),
    )

    # Process the efficient frontier prices up to current quarter
    for (quarter,), exp_df in new_expected_returns.group_by(['year', 'quarter']):

        # Quarter date range
        _, qdate_end = quarter_dates(quarter, quarter)

        # Skip if current quarter
        if quarter == 1:
            continue

        if not isinstance(qdate_end, datetime.date):
            raise ValueError("as_of_date must be a datetime.date object")

        # Get prices up to the last date of the quarter
        prices = security_price.filter(
            pl.col('symbol').is_in(exp_df['symbol']),
            pl.col('date') <= qdate_end,
        )

        # Pivot prices to wide form for covariance calculation
        prices_wide = prices.pivot(
            on='symbol', values='close', index='date',
        ).drop_nulls().to_pandas().set_index('date')

        # Calculate covariance matrix
        prices_cov = CovarianceShrinkage(prices_wide).ledoit_wolf()

        # Optimize efficient frontier of Mean Variance
        ef = EfficientFrontier(exp_df.to_numpy(), prices_cov)

        # Reduces 0 weights
        ef.add_objective(L2_reg, gamma=OptimizeConfigs.l2_gamma)

        # Get the allocation weights
        if OptimizeConfigs.objective == 'max_sharpe':
            weights = ef.max_sharpe()

        elif OptimizeConfigs.objective == 'max_quadratic_utility':

            weights = ef.max_quadratic_utility(risk_aversion=OptimizeConfigs.risk_aversion)

        else:
            weights = ef.min_volatility()

        # Discrete allocation from portfolio value
        allocator = DiscreteAllocation(
            weights,
            prices['close'],
            total_portfolio_value=OptimizeConfigs.investment_amount,
            short_ratio=None,
        )
        disc_allocation, cash = allocator.greedy_portfolio()

        # Format into dataframe
        # TODO: CHECK THIS OPERATION
        # Want a DF with columns: fundamentals_id/security_id (?), allocation, shares
        allocation = pl.hstack([
            pl.Series(name='allocation', values=weights),
            pl.Series(name='shares', values=disc_allocation),
        ]).fill_nulls(0)

        # Extend portfolio with new allocation
        existing_portfolios.extend(allocation)


    # Update database
    # TODO: Update database with new portfolio allocations
