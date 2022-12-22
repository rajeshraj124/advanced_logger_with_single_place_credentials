import os
import yaml
import json
import joblib
import logging
import logging.config
from box.exceptions import BoxValueError
from ensure import ensure_annotations
from box import ConfigBox
from pathlib import Path
from typing import Any
import pandas as pd
from subprocess import call
from configparser import ConfigParser
import sys
sys.path.append('/home/sonu/workspace/pro/configs/')
from static_path import SECRETS_PATH, LOG_CONFIG_PATH
logger = logging.getLogger(__name__)


@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """reads yaml file and returns

    Args:
        path_to_yaml (str): path like input

    Raises:
        ValueError: if yaml file is empty
        e: empty file

    Returns:
        ConfigBox: ConfigBox type
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e


@ensure_annotations
def create_directories(path_to_directories: list, verbose=True):
    """create list of directories

    Args:
        path_to_directories (list): list of path of directories
        verbose (bool, optional): ignore if multiple dirs is to be created. Defaults to False.
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logger.info(f"created directory at: {path}")


@ensure_annotations
def save_json(path: Path, data: dict):
    """save json data

    Args:
        path (Path): path to json file
        data (dict): data to be saved in json file
    """
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

    logger.info(f"json file saved at: {path}")


@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """load json files data

    Args:
        path (Path): path to json file

    Returns:
        ConfigBox: data as class attributes instead of dict
    """
    with open(path) as f:
        content = json.load(f)

    logger.info(f"json file loaded successfully from: {path}")
    return ConfigBox(content)


@ensure_annotations
def save_bin(data: Any, path: Path):
    """save binary file

    Args:
        data (Any): data to be saved as binary
        path (Path): path to binary file
    """
    joblib.dump(value=data, filename=path)
    logger.info(f"binary file saved at: {path}")


@ensure_annotations
def load_bin(path: Path) -> Any:
    """load binary data

    Args:
        path (Path): path to binary file

    Returns:
        Any: object stored in the file
    """
    data = joblib.load(path)
    logger.info(f"binary file loaded from: {path}")
    return data


@ensure_annotations
def get_size(path: Path) -> str:
    """get size in KB

    Args:
        path (Path): path of the file

    Returns:
        str: size in KB
    """
    size_in_kb = round(os.path.getsize(path) / 1024)
    return f"~ {size_in_kb} KB"


def read_sql(path: Path) -> str:
    """load sql file

    Args:
        path (Path): path to sql file

    Returns:
        str: string query in the file
    """

    with open(path, "r") as sql_file:
        data = sql_file.read()
    logger.info(f"Sql file loaded from: {path}")
    return data


@ensure_annotations
def fetch_data(database_connection, sql_query: str) -> pd.DataFrame:
    """
    This function is used to fetch data from the database and return a data frame.

    Args:
        database_connection (_type_): it will be the snowflake database_connection or postgres_database connection.
        sql_query (str): it will be sql query.

    Returns:
        pd.DataFrame:  return pandas DataFrame.
    """
    try:
        logger.info("Fetching data from database >>>>")
        connection_cursor = database_connection.cursor()
        connection_cursor.execute(sql_query)
        result = connection_cursor.fetchall()
        column_filed = [i[0] for i in connection_cursor.description]
        dataset = pd.DataFrame(result, columns=column_filed)

        logger.info(
            "Data fetched from respective database connection."
            f"[fetched rows : {dataset.shape[0]}, fetched columns : {dataset.shape[1]}]."
        )

        return dataset

    except Exception as e:
        logger.exception("Could not fetch_data: " + str(e))


@ensure_annotations
def get_log_dataframe(file_path: Path) -> pd.DataFrame:
    """
    This function returns log data to pandas.DataFrame.

    Args:
        file_path (Path): It will log file Path.

    Returns:
        pd.DataFrame: Returns log data to pandas.DataFrame
    """
    data = []
    with open(file_path) as log_file:
        for line in log_file.readlines():
            data.append(line.split("^;"))

    log_df = pd.DataFrame(data)
    columns = [
        "Time_stamp",
        "Log_Level",
        "line_number",
        "module_name",
        "function_name",
        "message",
    ]
    log_df.columns = columns

    # log_df["log_message"] = log_df["Time stamp"].astype(str) + ":$" + log_df["message"]

    return log_df


def fetch_data_as_pandas_df(database_connection, query: str):
    """
    This function is used to fetch data from the Snowflake database and return a data frame.

    Args:
        database_connection: It will be the Snowflake database connection.
        query (str): it will be sql query string.

    Returns:
        _type_: it will be returned as type of the data frame.
    """
    try:
        logger.info("Fetching data from snowflake database...")
        connection_cursor = database_connection.cursor()
        connection_cursor.execute(query)
        dataset = connection_cursor.fetch_pandas_all()
        logger.info(
            "Data fetched from respective database connection."
            f"[fetched rows - {dataset.shape[0]}, fetched columns - {dataset.shape[1]}]."
        )
        return dataset
    except Exception as e:
        logger.exception("Could not fetch data from snowflake db: " + str(e))


def push_in_s3(upload_file_path: str, s3_path: str):
    """
    this method is called when data to be pushed into S3.

    Args:
        upload_file_path (str): it will be file path which to push into S3.
        s3_path (str): it will be s3 path which to push into S3.
    """
    try:
        logger.info("Pushing in S3 from local..")
        call(["aws", "s3", "cp", upload_file_path, s3_path])
        logger.info(f"successfully pushed in S3 from local:{s3_path}")
    except Exception as e:
        logger.exception("Could not push in S3: " + str(e))


def execute_sql(database_connection, sql_query: str):
    """
    This function is used to execute a SQL statement.

    Args:
        database_connection (_type_): it will be active in the database connection.
        sql_query (str): it will be sql query string.
    """
    try:

        connection_cursor = database_connection.cursor()
        connection_cursor.execute(sql_query)
        connection_cursor.close()
    except Exception as e:
        logger.exception(f"Query not executed. {str(e)}")


def read_ini(path: Path):
    """load ini file

    Args:
        path (Path): path to sql file

    Returns:
        str: string query in the file
    """
    try:
        config = ConfigParser()
        with open(path) as config_file:
            config.read_file(config_file)
        logger.info(f"ini file loaded from: {path}")
        return config
    except Exception as e:
        logger.exception(f"ini file load error {str(e)}")


def push_in_snowflake(
    sf_database_connection,
    sf_database: str,
    sf_warehouse: str,
    sf_table: str,
    aws_s3_path: str,
):

    """
    This function is used to push data from the S3 bucket to Snowflake table.

    Args:
        sf_database_connection: It will be the Snowflake database connection.
        sf_database (str): the name of the database.
        sf_warehouse (str): the name of the data warehouse.
        sf_table (str): the name of the target table.
                    Example-:QL2_TMP.PUBLIC.ANALYTICS_TEST
        aws_s3_path (str) : it will be s3_path string.
                        Example-s3://analytics-retail/analytics_test/upload_file_retail_michaels.csv.
    """

    try:
        secret_config = read_yaml(SECRETS_PATH)
        aws_key_id = secret_config.aws_credentials.access_key_id
        aws_secret_key = secret_config.aws_credentials.secret_access_key
        set_db_query = f"USE {sf_database};"
        resume_warehouse_query = (
            f"ALTER WAREHOUSE IF EXISTS {sf_warehouse} RESUME IF SUSPENDED;"
        )
        set_warehouse_query = f"USE WAREHOUSE {sf_warehouse};"
        s3_path = aws_s3_path

        connection_cursor = sf_database_connection.cursor()

        logger.info("Setting Database..")
        connection_cursor.execute(set_db_query)
        logger.info("Set Database.")

        logger.info("resuming warehouse..")
        connection_cursor.execute(resume_warehouse_query)
        logger.info("resumed warehouse data loader is ready.")

        logger.info("Setting warehouse..")
        connection_cursor.execute(set_warehouse_query)
        logger.info("Set warehouse.")

        logger.info("Data pushing started on target table.")
        connection_cursor.execute(
            """
                    COPY INTO {0} FROM {1}
                    CREDENTIALS = (AWS_KEY_ID='{2}',AWS_SECRET_KEY='{3}')
                    FILE_FORMAT=(TYPE="csv" FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                    PATTERN = '.*.csv'
                    ON_ERROR = 'continue';
                    """.format(
                sf_table, s3_path, aws_key_id, aws_secret_key
            )
        )

        logger.info(
            "successfully pushed from S3 bucket:{0} to snowflake table: {1}".format(
                s3_path, sf_table
            )
        )
    except Exception as e:
        logger.exception("Could not push data in snowflake: " + str(e))


def get_logger(logger_name: str):
    """This function return logger object

    Args:
        logger_name (str): It will logger name

    """
    log_config_path = LOG_CONFIG_PATH
    with open(log_config_path, "r") as f:
        yaml_config = yaml.safe_load(f.read())
        logging.config.dictConfig(yaml_config)
        logger = logging.getLogger(logger_name)
        return logger
    