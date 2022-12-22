# This is a sample Python script.

#########################################: Please Don't Change :#######################################
import logging
import os
import sys
from datetime import datetime

sys.path.append(
    "/home/sonu/workspace/pro/component/"
)
sys.path.append(
    "/home/sonu/workspace/pro/utils/"
)
sys.path.append(
    "/home/sonu/workspace/pro/db_conn/"
)
from common import get_logger
from db_conn import DatabaseConnection


def log_setup():
    """This funtion is require for log_confi.yaml file."""
    path = os.path.dirname(os.path.realpath(__file__))
    log_dir = os.path.join(path, "log")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(path, log_dir, "running_log.log")
    filelog = logging.handlers.TimedRotatingFileHandler(
        log_path, when="midnight", backupCount=5
    )
    return filelog


#########################################: Please Code write Below :#######################################

STAGE_01 = "Connection Establish"
STAGE_02 = ""
STAGE_03 = ""
STAGE_04 = ""


def main():
    logger = get_logger(logger_name="sample")
    logger.info("main logging initialized")
    try:
        start_time = datetime.now()
        logger.info(f"<<<<<<< The start of {STAGE_01} has begun. >>>>>>>")
        database_connection = DatabaseConnection()
        snowflake_connection = database_connection.get_snowflake_connection()
        logger.info(f"<<<<<<< {STAGE_01} has been completed. >>>>>>>")

        cux = snowflake_connection.cursor()
        cux.execute("select current_timestamp();")
        result = cux.fetchone()
        logger.info(f"test connection succeed at {str(result)}")

        end_time = datetime.now()
        logger.info(
            "The project has been successfully executed, with a runtime of {0}.".format(
                end_time - start_time
            )
        )

    except Exception as e:
        logger.exception(f"getting error message {str(e)}")


if __name__ == "__main__":
    main()
