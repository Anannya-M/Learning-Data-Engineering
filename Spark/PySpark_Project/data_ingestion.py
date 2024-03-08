import logging

from pyspark.sql.functions import *
from pyspark.sql.types import *

import logging.config

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_ingestion")


def load_file(spark, file_dir, file_format, header, inferSchema):
    global data
    try:
        logger.warning("Started executing load_file() method....")

        if file_format == 'parquet':
            data = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            data = spark.read.format(file_format).option('header',header).option('inferSchema',inferSchema).load(file_dir)

    except Exception as e:
        logger.error("An error occured at load_file() method.View trace===>", str(e))
        raise
    else:
        logger.warning("Dataframe created successfully....")

    return data


def display_data(dataframe):
    logger.info("Displaying data...")
    show_data = dataframe.show()

    return show_data
