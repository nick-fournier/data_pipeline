#!/usr/bin/env python
"""Module contains database connection resources."""
import os

import polars as pl
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    OutputContext,
)
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import scoped_session, sessionmaker
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


class PostgresPolarsIOManager(ConfigurableIOManager):
    username: str = os.getenv("POSTGRES_USER", "")
    password: str = os.getenv("POSTGRES_PASS", "")
    host: str = os.getenv("POSTGRES_HOST", "")
    port: str = os.getenv("POSTGRES_PORT", "")
    database: str = os.getenv("POSTGRES_DB", "")

    def __init__(self):
        for attr in ["username", "password", "host", "port", "database"]:
            assert getattr(self, attr) != "", f"Missing {attr} environment variable"


        self.connection_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(self.connection_url)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        assert context.metadata is not None, "No metadata found"
        model_class = context.metadata.get('write', None)
        if model_class is None:
            raise Exception("No write metadata found")

        # Convert Polars DataFrame to a list of dictionaries
        data = obj.to_dicts()

        with self.Session() as session:
            try:
                session.execute(insert(model_class), data)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()

    def load_input(self, context: InputContext) -> pl.DataFrame:
        assert context.metadata is not None, "No metadata found"
        model_class = context.metadata.get('read', None)
        if model_class is None:
            raise Exception("No read metadata found")

        with self.Session() as session:
            try:
                result = session.query(model_class).all()
            finally:
                session.close()

        # Convert the result to a Polars DataFrame
        df = pl.DataFrame(result)
        return df

