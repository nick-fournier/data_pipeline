#!/usr/bin/env python
"""Module contains database connection resources."""
import os

import polars as pl
import psycopg2
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    IOManager,
    OutputContext,
)
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv("data_pipeline/.env")

# TODO: Merge the PostgresConfig and PostgresIOManager classes into a single class
# so that tunneling is handled by the IOManager

class PostgresConfig(ConfigurableResource):
    """Resource for connecting to a Postgres database.

    This resource is used to connect to a Postgres database using an SSH tunnel.
    The connection details are read from environment variables.

    Args:
    ----
        ConfigurableResource (_type_): _description_

    Returns:
    -------
        _type_: _description_

    """

    pg_host: str  = os.getenv("POSTGRES_HOST", "")
    pg_port: int = int(os.getenv("POSTGRES_PORT", 22))  # noqa: PLW1508
    pg_database: str = os.getenv("POSTGRES_DB", "")


    def db_uri(self) -> str:
        return "postgresql://{}:{}@{}:{}/{}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASS"),
            self.pg_host,
            self.pg_port,
            self.pg_database,
        )

    def connect(self) -> SSHTunnelForwarder:
        """Create an SSH tunnel to the Postgres database.

        This function creates an SSH tunnel to the Postgres database using the connection details
        provided in the environment variables.

        Returns
        -------
            SSHTunnelForwarder: The SSH tunnel to the Postgres database

        """
        return SSHTunnelForwarder(
            (os.getenv("SSH_HOST")),
            ssh_username=os.getenv("SSH_USER"),
            ssh_password=os.getenv("SSH_PASS"),
            remote_bind_address=(self.pg_host, self.pg_port),
        )

    def tunneled_uri(self, tunnel: SSHTunnelForwarder) -> str:
        """Create a connection URI to the Postgres database.

        This function creates a connection URI to the Postgres database using the connection details

        Args:
        ----
            tunnel (SSHTunnelForwarder): The SSH tunnel to the Postgres database

        Returns:
        -------
            str: The connection URI to the Postgres database

        """
        return "postgresql://{}:{}@{}:{}/{}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASS"),
            tunnel.local_bind_host,
            tunnel.local_bind_port,
            self.pg_database,
        )

    def tunneled(self, fn, **kwargs):
        """Run a function with a tunnel to the Postgres database.

        This function creates a tunnel to the Postgres database and passes the
        connection URI to the function provided. By passing the functoin as an argument,
        we use "with" to ensure the tunnel is closed after the function completes.

        Args:
        ----
            fn (function): The function to be executed with the connection URI as sole argument
            **kwargs: Additional keyword arguments to be passed to the function

        Returns:
        -------
            Any: The result of the function provided

        """
        with SSHTunnelForwarder(
            (os.getenv("SSH_HOST")),
            ssh_username=os.getenv("SSH_USER"),
            ssh_password=os.getenv("SSH_PASS"),
            remote_bind_address=(self.pg_host, self.pg_port),
        ) as tunnel:
            if not isinstance(tunnel, SSHTunnelForwarder):
                msg = "Expected tunnel to be an instance of SSHTunnelForwarder"
                raise TypeError(msg)
            conn_uri = self.tunneled_uri(tunnel)
            return fn(conn_uri, **kwargs)


class PostgresPolarsIOManager(IOManager):

    def __init__(self, postgres_config: PostgresConfig):
        self.postgres_config = postgres_config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):

        metadata: dict = getattr(context, "metadata", {})

        method = metadata.get("method", "fail")
        if method == "skip":
            return

        # Connect to PostgreSQL and insert data
        obj.write_database(
            table_name=context.step_key,
            connection=self.postgres_config.db_uri(),
            if_table_exists=method,
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # Connect to PostgreSQL and read data into a Polars DataFrame
        df = pl.read_database_uri(
            query=f"SELECT * FROM {context.name}",
            uri=self.postgres_config.db_uri()
        )

        return df
