# IMPORTING NECESSARY LIBRARIES AND FUNCTIONS
import logging

import env_variables as env
from spark_object_create import create_spark_object
import logging.config

logging.config.fileConfig("Properties\\configuration\\logging.config")


# D:\Clg_Projects\Spark\Playing-with-Data\Spark\PySpark_Project\Properties\configuration\logging.config


def main():
    logging.info("Let's get started....")

    logging.info("Creating spark object...")
    spark = create_spark_object(env.env, env.appName)
    logging.info("Spark object created successfully.....")


if __name__ == "__main__":
    main()
    logging.info("Application done....")
