from dagster import Config, ConfigurableResource
# from dagster_postgres import PostgresResource
from pydantic import Field
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os


load_dotenv('data_pipeline/.env')


class PostgresConfig(ConfigurableResource):
    pg_host: str  = os.getenv('POSTGRES_HOST', "")
    pg_port: int = int(os.getenv('POSTGRES_PORT', 22))
    pg_database: str = os.getenv('POSTGRES_DB', "")
        
    def connect(self):
        tunnel = SSHTunnelForwarder(
            (os.getenv('SSH_HOST')),
            ssh_username=os.getenv('SSH_USER'),
            ssh_password=os.getenv('SSH_PASS'),
            remote_bind_address=(self.pg_host, self.pg_port)
        )
        return tunnel
    
    def uri(self, tunnel):
        return "postgresql://{}:{}@{}:{}/{}".format(
            os.getenv('POSTGRES_USER'),
            os.getenv('POSTGRES_PASS'),
            getattr(tunnel, 'local_bind_host'),
            getattr(tunnel, 'local_bind_port'),
            self.pg_database
        )
    
    def tunneled(self, fn, **kwargs):
        """
        This function creates a tunnel to the Postgres database and passes the connection URI to the function provided.
        
        By passing the functoin as an argument, we use "with" to ensure the tunnel is closed after the function completes. 

        Args:
            fn (function): The function to be executed with the connection URI as sole argument

        Returns:
            Any: The result of the function provided
        """
        with SSHTunnelForwarder(
            (os.getenv('SSH_HOST')),
            ssh_username=os.getenv('SSH_USER'),
            ssh_password=os.getenv('SSH_PASS'),
            remote_bind_address=(self.pg_host, self.pg_port)
        ) as tunnel:
            conn_uri = self.uri(tunnel)
            return fn(conn_uri, **kwargs)



class Exchange(Config):
    path: str = Field(description="The path to the file containing the list of tickers")
    separator: str = Field(description="The separator used in the file")    


class SecurityList(Config):
    symbol: str = Field(description="The stock symbol")
    name: str = Field(description="The name of the company")
    exchange: str = Field(description="The exchange the stock is listed on")
    country: str = Field(description="The country where the company is based")
    sector: str = Field(description="The sector the company operates in")
    industry: str = Field(description="The industry the company operates in")
    website: str = Field(description="The company's website")
    logo_url: str = Field(description="The URL of the company's logo")
    fulltime_employees: int = Field(description="The number of full-time employees at the company")
    business_summary: str = Field(description="A summary of the company's business")


class FieldMap(Config):
    website: str = 'logo_url'
    fullTimeEmployees: str = 'fulltime_employees'
    longBusinessSummary: str = 'business_summary'
    NetIncome: str = 'net_income'
    NetIncomeCommonStockholders: str = 'net_income_common_stockholders'
    TotalLiabilitiesNetMinorityInterest: str = 'total_liabilities'
    TotalAssets: str = 'total_assets'
    CurrentAssets: str = 'current_assets'
    CurrentLiabilities: str = 'current_liabilities'
    CapitalStock: str = 'shares_outstanding'
    CashAndCashEquivalents: str = 'cash'
    GrossProfit: str = 'gross_profit'
    TotalRevenue: str = 'total_revenue'
