import logging
import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Persist")


def hive_data_persist(spark, data, dataname, partitionBy, mode):
    try:
        logger.warning("Persisting the data into hive table....")

        logger.warning("Creating hive database...")
        spark.sql(""" create database if not exists cities """)
        spark.sql("""use cities """)

        logger.warning("Wrtiting into the hive table")
        data.write.mode(mode).partitionBy(partitionBy).saveAsTable(dataname)

    except Exception as e:
        logger.error("An error occured while executing hive_data_persist() method.See traces ==>", str(e))
        raise
    else:
        logger.warning("Data successfully persisted into hive table...")


def hive_data_persist_for_presc(spark, data, dataname, partitionBy, mode):
    try:
        logger.warning("Persisting the data into hive table for presc data....")

        logger.warning("Creating hive database...")
        spark.sql(""" create database if not exists prescribers """)
        spark.sql("""use prescribers """)

        logger.warning("Writing into the hive table")
        data.write.mode(mode).partitionBy(partitionBy).saveAsTable(dataname)

    except Exception as e:
        logger.error("An error occured while executing hive_data_persist() method.See traces ==>", str(e))
        raise
    else:
        logger.warning("Data successfully persisted into hive table...")


def load_data_mysql(spark, data, dataname, url, dbtable, mode, user, password):
    try:
        logger.warning("Executing the load_data_mysql() method...")
        data.write.format("jdbc") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("spark.jars", "D:\Clg_Projects\Spark\Playing-with-Data\Spark\PySpark_Project\mysql-connector-j-8.3.0.jar") \
            .save()

    except Exception as e:
        logger.error("An error occured while executing load_data_mysql() method.See trace ==> ", str(e))
    else:
        logger.warning("load_data_mysql() method executed successfully....")
