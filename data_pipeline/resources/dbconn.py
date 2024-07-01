#!/usr/bin/env python
"""Module contains database connection resources."""
import os

from dagster import ConfigurableResource
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv("data_pipeline/.env")


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

    def connect(self) -> SSHTunnelForwarder:  # noqa: ANN101
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

    def uri(self, tunnel: SSHTunnelForwarder) -> str:  # noqa: ANN101
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

    def tunneled(self, fn, **kwargs):  # noqa: ANN001, ANN003, ANN101, ANN201
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
            conn_uri = self.uri(tunnel)
            return fn(conn_uri, **kwargs)
