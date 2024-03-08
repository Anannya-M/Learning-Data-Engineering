# IMPORTING NECESSARY LIBRARIES AND FUNCTIONS
import logging
import logging.config

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Spark_object_create")


def create_spark_object(env, appName):
    try:
        if env == 'DEV':
            master = 'local'

        else:
            master = 'yarn'
        logger.info(f"Master is {master}")

        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
        return spark

    except Exception as e:
        logger.error("An error occured while executing the create_spark_object()...Find trace here====", str(e))
        raise

