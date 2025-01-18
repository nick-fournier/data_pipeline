
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import datetime
import os
from concurrent.futures import ThreadPoolExecutor

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import pymc as pm
import statsmodels.api as sm
from dagster import asset
from plotly import express as px
from sklearn.cluster import DBSCAN, KMeans
from sklearn.decomposition import PCA
from sklearn.model_selection import RandomizedSearchCV
from sklearn.neighbors import NearestNeighbors
from sklearn.neural_network import MLPRegressor
from statsmodels.formula.api import mixedlm
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from tqdm import tqdm

from data_pipeline.portfolio_optimizer.assets.forecast_returns import prepare_data
from data_pipeline.portfolio_optimizer.resources import dbconn

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


if __name__ == '__main__':
    # Development
    pg_config = dbconn.PostgresConfig()

    inputs = {
        "fundamentals": pl.DataFrame(),
        "scores": pl.DataFrame(),
        "security_price": pl.DataFrame(),
        "security_list": pl.DataFrame(),
    }

    for input in inputs.keys():
        if os.path.exists(f'dagster_home/tmp/{input}.parquet'):
            inputs[input] = pl.read_parquet(f'dagster_home/tmp/{input}.parquet')
        else:
            inputs[input] = pl.read_database_uri(
                query=f"SELECT * FROM {input}",
                uri=pg_config.db_uri(),
            )

    fundamentals = inputs['fundamentals']
    scores = inputs['scores']
    security_price = inputs['security_price']
    security_list = inputs['security_list']


    ### Working code -------------------
    # Get the prepared data
    model_data = prepare_data(**inputs)

    # Normalize data
    model_data = model_data.with_columns(
        **{
            k: z_score(pl.col(k)) for k in X_COLS
        }
    )

    # Create a group ID based on security_list industry sector
    # Sector to ID integer mapping
    sector_map = dict(zip(
        security_list['sector'].unique(),
        range(len(security_list['sector'].unique()))
    ))

    # Assign sector ID to security list
    security_list = security_list.with_columns(
        sector_id=security_list['sector'].replace(sector_map).cast(pl.Int64)
    )

    # Assign sector ID to model data
    model_data = model_data.join(
        security_list.select('symbol', 'sector_id'),
        on='symbol',
        how='inner',
    )

    # Check for missing sector IDs
    assert model_data.filter(pl.col('sector_id').is_null()).is_empty()


    # Separate the future prediction rows from the training rows
    train_data = model_data.filter(pl.col('next_quarterly_yield').is_not_null())
    pred_data = model_data.filter(pl.col('next_quarterly_yield').is_null())



    # # Simple OLS model -------------------
    # X = train_data[X_COLS].to_pandas()
    # y = train_data['next_quarterly_yield'].to_numpy()
    # w = train_data['r_squared'].to_numpy()
    # ols_model = sm.WLS(y, X, weights=w).fit()
    # ols_model.summary()

    # # Predictions
    # px.scatter(
    #     pl.DataFrame({'actual': y, 'predicted': ols_model.predict(X)}),
    #     x='actual',
    #     y='predicted'
    # ).show()


    # Mixed-effects model with statsmodels -------------------
    # train_data.group_by('sector_id').count().min()

    # Reduce data with PCA
    # pca = PCA(n_components=2)
    # X = pca.fit_transform(train_data[X_COLS].to_numpy())

    # Model data
    # reduced_data = pl.DataFrame(
    #     {
    #         **{f'pca_{i}': X[:, i] for i in range(X.shape[1])},
    #         'next_quarterly_yield': train_data['next_quarterly_yield'],
    #         'sector_id': train_data['sector_id'],
    #     }
    # )
    # reduced_X_COLS = [f'pca_{i}' for i in range(X.shape[1])]

    # 1. Define the fixed effects
    # This creates the formula for the fixed effects
    # fixed_effects = "next_quarterly_yield ~ " + " + ".join(X_COLS)

    # # 2. Define the random effects (random intercepts for each group)
    # # This specifies random intercepts for each group (sector_id)
    # random_effects = "1 | sector_id"

    # # 3. Fit the mixed-effects model
    # model = mixedlm(
    #     fixed_effects,
    #     train_data.to_pandas(),
    #     groups=train_data['sector_id'].cast(pl.Int64),
    #     re_formula=random_effects
    # )
    # result = model.fit()
    # result.summary()

    # # Predictions
    # px.scatter(
    #     pl.DataFrame(
    #         {
    #             'actual': train_data['next_quarterly_yield'],
    #             'predicted': result.predict(),
    #         }),
    #     x='actual', y='predicted',
    #     range_x=[-2, 2], range_y=[-2, 2]
    # ).show()


    # NN model -------------------
    # # Get the prepared data
    # model_data = prepare_data(fundamentals, scores, security_price)

    # # Separate the future prediction rows from the training rows
    # est_data = model_data.filter(pl.col('next_quarterly_yield').is_not_null())
    # pred_data = model_data.filter(pl.col('next_quarterly_yield').is_null())

    # # Get all previous quarter yield rates as features
    # est_data['quarterly_yield_rate'].shift(-1).over('symbol')
    # est_data.filter(
    #     pl.col('symbol') == 'A'
    # )

    # feature_cols = [*X_COLS, 'quarterly_yield_rate']
    # # Normalize financial data on z-score
    # x_train = est_data.with_columns(
    #     **{
    #         k: z_score(pl.col(k)) for k in feature_cols
    #     }
    # )

    # x_pred = pred_data.with_columns(
    #     **{
    #         k: z_score(pl.col(k)) for k in feature_cols
    #     }
    # ).drop('next_quarterly_yield')


    # # Setup test and train data
    # x_train = x_train.select(X_COLS).to_pandas()
    # # x_train['intercept'] = 1
    # y_train = est_data['next_quarterly_yield'].to_numpy()
    # w_train = est_data['r_squared'].to_numpy()


    # # Setup test data
    # x_pred = pred_data.select(X_COLS).to_pandas()
    # w_pred = pred_data['r_squared'].to_numpy()

    # # Estimate model
    # # model = sm.WLS(y_train, x_train, weights=w_train).fit() # type: ignore
    # # print(model.summary())
    # model = MLPRegressor(
    #     hidden_layer_sizes=(100, 50),
    #     activation='tanh',
    #     solver='sgd',
    #     alpha=0.01,
    #     learning_rate_init=0.01,
    #     max_iter=500,
    #     random_state=42,
    # )

    # # Fit the model
    # model.fit(x_train, y_train)

    # # Create predictions dataframe with fundamentals_id
    # predicted = pl.Series(model.predict(x_pred))
    # expected_returns = pred_data.with_columns(
    #     expected_returns=predicted,
    # )

    # return expected_returns


    # Hierarchical Bayesian model -------------------

    # Subset symbols for dev
    # symbols = train_data['symbol'].unique().sample(20)
    # train_data = train_data.filter(pl.col('symbol').is_in(symbols))

    # Constants
    n_groups = train_data['sector_id'].n_unique()
    n_features = len(X_COLS)
    n_observations = len(train_data)

    # Detect CPU cores
    n_cores = int(0.75 * (os.cpu_count() or 1))

    # Generate group indices, str to index integer
    symbol_idx = {k: v for v, k in enumerate(train_data['symbol'].unique())}
    sector_idx = {k: v for v, k in enumerate(train_data['sector_id'].unique())}
    group_indices = (
        train_data['sector_id']
        .replace(sector_idx)
        .cast(pl.Int32)
        .to_numpy()
    )

    # Feature set
    X = train_data[X_COLS].to_numpy()
    y = train_data['next_quarterly_yield'].to_numpy()

    # Hierarchical Bayesian Model with PyMC
    with pm.Model() as hierarchical_model:
        # Hyperpriors for group-level coefficients
        # Shared mean
        mu_beta = pm.Normal("mu_beta", mu=0, sigma=1, shape=n_features)
        # Shared std
        sigma_beta = pm.HalfNormal("sigma_beta", sigma=5, shape=n_features)

        # Group-specific coefficients
        beta = pm.Normal(
            "beta",
            mu=mu_beta,
            sigma=sigma_beta,
            shape=(n_groups, n_features),
        )

        # Noise
        sigma = pm.HalfNormal("sigma", sigma=1)

        # Expected value for observations
        y_pred = pm.Normal(
            "y_pred",
            mu=pm.math.sum(X * beta[group_indices], axis=1),
            sigma=sigma,
            observed=y,
        )

        # Prior predictive checks
        prior = pm.sample_prior_predictive()

        # Sampling
        trace = pm.sample(
            1000,
            init="jitter+adapt_diag",
            tune=1000,
            target_accept=0.8,
            # cores=n_cores,
            # max_treedepth=15,
        )

    # Save the trace
    az.to_netcdf(trace, "dagster_home/tmp/hierarchical_model_results.nc")

    # Load the trace
    # trace = az.from_netcdf("hierarchical_model_results.nc")

    # Summarize the results
    summary = az.summary(trace, hdi_prob=0.95)
    print(summary)

    # TODO: Make the model an asset to be stored


    # Traceplot
    az.plot_trace(trace, var_names=["mu_beta", "sigma_beta"])
    # Save to image
    plt.savefig('bayes-fit.png')

    # Posterior predictive checks
    with hierarchical_model:
        posterior_predictive = pm.sample_posterior_predictive(trace).posterior_predictive

    # Visualize posterior predictive checks with plotly express
    result = pl.DataFrame(
        {
            "observation_index": np.arange(n_observations),
            "observed": y,
            "predicted_mean": posterior_predictive["y_pred"].mean(axis=(0, 1)).to_numpy(),
        }
    )

    px.scatter(
        result,
        x="observation_index",
        y="observed",
        # color=pl.lit("Observed"),
        title="Posterior Predictive Check",
        labels={"value": "y", "observation_index": "Observation Index"}
    ).add_scatter(
        x=result["observation_index"],
        y=result["predicted_mean"],
        mode="markers",
        name="Predicted",
        marker=dict(color="red", opacity=0.5)
    ).show()

    # Plot prediction against the observed values using plotly express
    result = pl.DataFrame(
        {
            "y": y,
            "y_pred_mean": 5*posterior_predictive["y_pred"].mean(axis=(0,1)).to_numpy(),
            # "y_pred75": np.percentile(posterior_predictive["y_pred"], 75, axis=(0,1)),
            # "y_pred_median": np.median(posterior_predictive["y_pred"], axis=(0,1)),
            # "y_pred25": np.percentile(posterior_predictive["y_pred"], 25, axis=(0,1)),
            "group": train_data['sector_id'].to_numpy(),
        }
    )

    px.scatter(
        result,
        x="y",
        y="y_pred_mean",
        color="group",
        range_x=[-2, 2],
        range_y=[-2, 2],
    ).show()








    ### -------------------------------









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
    # x_train['intercept'] = 1
    y_train = est_data['next_quarterly_yield'].to_numpy()
    w_train = est_data['r_squared'].to_numpy()


    # Setup test data
    x_pred = pred_data.select(X_COLS).to_pandas()
    w_pred = pred_data['r_squared'].to_numpy()

    # Estimate model
    # model = sm.WLS(y_train, x_train, weights=w_train).fit() # type: ignore
    # print(model.summary())
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

    # Baseline model
    ols_fit = sm.WLS(y_train, x_train, weights=w_train).fit() # type: ignore
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

    compare_model = pl.DataFrame({'actual': y_train, 'predicted': model.predict(x_train)})
    px.scatter(compare_model, x='actual', y='predicted', opacity=0.25).show()