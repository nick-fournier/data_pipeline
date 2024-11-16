
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
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPRegressor

from ..resources.configs import Params
from ..resources.dbconn import PostgresConfig
from ..resources.models import Scores, SecurityPrices


def pct_change_from_first(x):
    return (x - x.iloc[0])/x.iloc[0]

def rescore(z, mean, sd):
    return z * sd + mean

def zscore(x):
    return (x - x.mean()) / x.std()

def minmax(v):
    return (v - v.min()) / (v.max() - v.min())

def get_analysis_data():
    score_cols = ['security_id', 'date', 'security__symbol', 'fiscal_year',
                  'pf_score', 'pf_score_weighted', 'eps', 'pe_ratio', 'roa', 'cash', 'cash_ratio',
                  'delta_cash', 'delta_roa', 'accruals', 'delta_long_lev_ratio',
                  'delta_current_lev_ratio', 'delta_shares', 'delta_gross_margin', 'delta_asset_turnover']

    # financials = models.Fundamentals.objects.all().values('security_id', 'date')
    prices_qry = SecurityPrice.objects.all().values('security_id', 'date', 'close')
    scores_qry = Scores.objects.all().values(*score_cols)

    # As dataframe
    # financials = pd.DataFrame(financials)
    prices = pd.DataFrame(prices_qry)
    df = pd.DataFrame(scores_qry).rename(columns={'security__fundamentals__fiscal_year': 'year'})

    pd.DataFrame(Scores.objects.all().values('security_id', 'date', 'security__fundamentals__fiscal_year'))

    # update scores
    # pfobject = GetFscore()
    # pfobject.save_scores()

    # Aggregate price data annually
    prices.close = prices.close.astype(float)
    prices.date = pd.to_datetime(prices.date)

    col_names = {'date': 'year', 'last': 'yearly_close', 'var': 'variance'}
    prices_year = prices.groupby([prices.date.dt.year, 'security_id']).close\
        .agg(['last', 'mean', 'var']).reset_index()\
        .rename(columns=col_names)

    # Add year and merge prices
    df.date = pd.to_datetime(df.date)
    df.rename(columns={'fiscal_year': 'year'}, inplace=True)
    df = df.merge(prices_year, on=['security_id', 'year'])

    # df.yearly_close = df.yearly_close.astype(float)
    df.pe_ratio = df.pe_ratio.astype(float)
    df = df.sort_values(['security_id', 'date'])

    return df


def _forecast_returns(
    fundamentals: pl.DataFrame,
    piotroski_scores: pl.DataFrame,
    stock_prices: pl.DataFrame,
    method='nn',
    backcast=False
    ) -> pl.DataFrame:

    # Join scores onto fundamentals
    finscores = piotroski_scores.join(
        fundamentals,
        left_on='fundamentals_id',
        right_on='id',
    )

    x_cols = [
        'roa', 'cash_ratio', 'delta_cash', 'delta_roa', 'accruals', 'delta_long_lev_ratio',
        'delta_current_lev_ratio', 'delta_shares', 'delta_gross_margin', 'delta_asset_turnover'
        ]

    model_df = finscores.set_index(['security_id', 'year'])[x_cols + ['yearly_close']].copy().astype(float)

    # Get lagged value of features from t-1
    model_df['lag_close'] = model_df.groupby('security_id', group_keys=False)['yearly_close'].shift(-1)

    # Normalize close price within group since that's company-level feature, all else are high level
    df_grps = model_df[~model_df.lag_close.isnull()].groupby('security_id', group_keys=False)
    model_df.loc[~model_df.lag_close.isnull(), 'norm_lag_close'] = df_grps['lag_close'].apply(stats.zscore)

    # Drop/fill NA and normalize
    model_df = model_df.dropna(subset=x_cols)
    model_df.loc[:, x_cols] = model_df[x_cols].apply(stats.zscore)

    # Store mean and std dev for later
    grp_stats = df_grps['lag_close'].agg(mean=np.mean, std=np.std)

    # temporary fy column to groupby on easily...
    model_df['fy'] = model_df.index.get_level_values('year')

    # Initalize DF to join to
    i = model_df.fy.min() + 1

    if not backcast:
        i = model_df.fy.max()

    while i <= model_df.fy.max():
        # Model matrix for time<i
        df_t = model_df[model_df.fy < i]
        # Drop any missing years
        df_t = df_t[~df_t.norm_lag_close.isna()]
        # Prediction matrix for time i
        df_t0 = model_df[model_df.fy == i]

        # Assemble matrices
        y = df_t['norm_lag_close']#.to_numpy()
        x = df_t[x_cols].astype(float)#.to_numpy()

        # Estimate model
        if method == 'nn':
            model = MLPRegressor(max_iter=1000).fit(x, y)
        else:
            model = LinearRegression().fit(x, y)
            print(pd.Series(list(model.coef_), index=x_cols))
        print(f'R^2 = {model.score(x, y)}')


        # Make predictions
        yhat = pd.DataFrame(model.predict(df_t0[x_cols]), index=df_t0.index, columns=['yhat']).join(grp_stats)
        yhat['next_close'] = yhat.apply(lambda row: rescore(row['yhat'], row['mean'], row['std']), axis=1)

        # Calculate returns
        model_df.loc[yhat.index, yhat.columns] = yhat
        i += 1

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

    scores = pg_config.tunneled(
        _forecast_returns,
        fundamentals=fundamentals,
        piotroski_scores=piotroski_scores,
        stock_prices=stock_prices,
    )

    return pl.DataFrame()
