from dagster import asset, op, Definitions
from data_pipeline.resources import PostgresConfig, FieldMap
from datetime import datetime, timezone
import polars as pl
import yahooquery as yq

@asset(description="Extract stock symbols from NYSE")
def nyse() -> pl.DataFrame:
    df = (
        pl.read_csv(
            'data_pipeline/fixtures/NYSE_and_NYSE_MKT_Trading_Units_Daily_File.xls',
            separator='\t',
            ignore_errors=True
        )
        .rename(str.strip)
        .rename(str.lower)
        .rename({'company': 'name'})
    )
    return df
     
@asset(description="Extract stock symbols from NASDAQ")   
def nasdaq() -> pl.DataFrame:
    df = (
        pl.read_csv(
            'data_pipeline/fixtures/nasdaq_screener_1718944810995.csv',
        )
        .rename(str.strip)
        .rename(str.lower)
    )
    return df

@asset(
    description="Combine NYSE and NASDAQ stock symbols into a unique dataframe set.",
)
def security_candidates(
    nyse: pl.DataFrame,
    nasdaq: pl.DataFrame
    ) -> pl.DataFrame:
    
    security_candidates = (
        pl.concat(
            [
                nyse.rename({'company': 'name'}).select(['symbol', 'name']),
                nasdaq.select(['symbol', 'name'])
            ]
        )
        .unique('symbol', keep='first')
    )
    
    # Filter out funds and things here...
        
    return security_candidates



@asset(
    description="Combine NYSE and NASDAQ stock symbols, check against existing ticker data",
    required_resource_keys={'postgres'}
)
def update_security_list(
    security_candidates: pl.DataFrame,
) -> pl.DataFrame:
    

    
    # Define functions to be used in the PostgresConfig.tunneled method
    def _existing_securities(uri: str) -> pl.DataFrame:
        query = "SELECT * FROM portfolio_optimizer_webapp_securitylist"
        return pl.read_database_uri(query, uri)

    def _append_new_securities(uri: str, new_securities: pl.DataFrame):
        new_securities.write_database('portfolio_optimizer_webapp_securitylist', uri, if_table_exists='append')
    
    
    # Initialize SSH tunnel to Postgres database
    pg_config = PostgresConfig()

    existing_securities_df = pg_config.tunneled(_existing_securities)
    db_columns = existing_securities_df.columns
    
    candidate_symbols = set(security_candidates['symbol'].to_list())
    existing_symbols = set(existing_securities_df['symbol'].to_list())
    
    new_symbols = list(candidate_symbols - existing_symbols)
    
    if not new_symbols:
        return existing_securities_df

    # Download meta data for new symbols and remove empty results
    stock_data = yq.Ticker(new_symbols)
    new_securities = {k: v for k, v in stock_data.asset_profile.items() if type(v) == dict}
    
    # Rename columns and drop extra columns    
    field_map = {
        k: getattr(FieldMap(), k, k) for k in next(iter(new_securities.values()))
        if getattr(FieldMap(), k, k) in db_columns
        }

    # Convert to DataFrame
    new_securities_df = (
        pl.DataFrame(list(new_securities.values()))
        .with_columns(
            symbol=pl.Series(list(new_securities.keys())),
            last_updated=datetime.now(timezone.utc)
        )
        .join(
            security_candidates.select(['symbol', 'name']),
            on='symbol',
            how='left'
        )
        .rename(field_map)
        .select(db_columns)
    )
        
    # Append new symbols to existing data
    updated_securities_df = pl.concat(
        [existing_securities_df, new_securities_df],
        how='vertical_relaxed'
    )
    pg_config.tunneled(_append_new_securities, new_securities=new_securities_df)
    
    return updated_securities_df


# @op(
#     description="Download meta data for a list of tickers",
# )
# def update_security_list(symbols: pl.Series) -> pl.DataFrame:
            

    # stock_data = yq.Ticker(symbols)
    # meta = stock_data.asset_profile

    # # Remove empty result
    # meta = {k: v for k, v in meta.items() if type(v) == dict}

    # # meta = pd.DataFrame(meta).T.reset_index()
    # # meta = meta.rename(columns=fields_map)

    # # self.set_meta(meta)

    # return meta


# defs = Definitions(
#     assets=[nyse_list, nasdaq_list, symbols],
#     resources={
#         "io_manager": DuckDBPolarsIOManager(database="data_pipeline.db"),
#         "postgres": PostgresConfig()
#     }
# )