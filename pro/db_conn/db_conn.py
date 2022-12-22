""" Database Connection module

This module is responsible for manage the snowflake and postgres database connection.
"""

import snowflake.connector
import psycopg2
import sys
sys.path.append('/home/sonu/workspace/pro/component/')
sys.path.append('/home/sonu/workspace/pro/utils/')
from common import read_yaml
from static_path import SECRETS_PATH
from warnings import filterwarnings

import logging

logger = logging.getLogger(__name__)

snowflake.connector.paramstyle = "qmark"
filterwarnings("ignore")


class DatabaseConnection:
    def __init__(self):
        self.config = read_yaml(SECRETS_PATH)
        self.snowflake_connection = None
        self.postgres_connection = None
        

    def get_snowflake_connection(
        self,
        sf_database=None,
        sf_schema=None,
        sf_warehouse=None,
    ):
        """
        This method is used to connect to the snowflake database.


        """
        try:
            logger.info("Connecting to Snowflake database...")

            sf_secrets = self.config.snowflake_credentials

            if self.snowflake_connection is not None:
                self.snowflake_connection.close()
            self.snowflake_connection = snowflake.connector.Connect(
                user=sf_secrets.user,
                password=sf_secrets.password,
                account=sf_secrets.account,
                database=sf_secrets.default_database
                if sf_database is None
                else sf_database,
                schema=sf_secrets.default_schema if sf_schema is None else sf_schema,
                warehouse=sf_secrets.default_warehouse
                if sf_warehouse is None
                else sf_warehouse,
                role=sf_secrets.role,
                # ocsp_fail_open=False,
                client_session_keep_alive=True,
            )

            connection_cursor = self.snowflake_connection.cursor()
            results = connection_cursor.execute("select current_version()").fetchone()
            logger.info(
                f"Successfully excuted query :[Snowflake Version:{str(results)}]"
            )

            logger.info(
                "connection established snowflake with [warehouse: {warehouse}, database: {database}, schema: {schema}, role: {role}]".format(
                    warehouse=sf_secrets.default_warehouse
                    if sf_warehouse is None
                    else sf_warehouse,
                    database=sf_secrets.default_database
                    if sf_database is None
                    else sf_database,
                    schema=sf_secrets.default_schema
                    if sf_schema is None
                    else sf_schema,
                    role=sf_secrets.role,
                )
            )
            return self.snowflake_connection
        except Exception as e:
            logger.exception("Could not connect snowflake_db: " + str(e))

    def get_postgres_connection(
        self,
        postgres_database=None,
    ):
        """This method is used to connect to the snowflake database."""
        try:
            postgres_secrets = self.config.postgres_credentials
            logger.info("Connecting to postgres database...")
            if self.postgres_connection is not None:
                self.postgres_connection.close()
            self.postgres_connection = psycopg2.connect(
                user=postgres_secrets.user,
                password=postgres_secrets.password,
                host=postgres_secrets.host,
                port=postgres_secrets.port,
                database=postgres_secrets.default_database
                if postgres_database is None
                else postgres_database,
            )

            connection_cursor = self.postgres_connection.cursor()
            connection_cursor.execute("select version()")
            results = connection_cursor.fetchone()
            logger.info(
                f"Successfully excuted query :[postgres Version:{str(results)}]"
            )

            logger.info(
                "connection established postgres with :[host: {host}, port: {port}, database: {database}]".format(
                    host=postgres_secrets.host,
                    port=postgres_secrets.port,
                    database=postgres_secrets.default_database
                    if postgres_database is None
                    else postgres_database,
                )
            )
            return self.postgres_connection
        except Exception as e:
            logger.exception("Could not connect to postgres: " + str(e))
