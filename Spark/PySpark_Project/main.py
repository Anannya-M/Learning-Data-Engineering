# IMPORTING NECESSARY LIBRARIES AND FUNCTIONS
import logging
import os

import logging.config
import env_variables as env
from spark_object_create import create_spark_object
from data_ingestion import load_file, display_data
from data_preprocessing import process_data

logging.config.fileConfig("Properties/configuration/logging.config")


# D:\Clg_Projects\Spark\Playing-with-Data\Spark\PySpark_Project\Properties\configuration\logging.config


def main():
    global file_format, file_dir, header, inferSchema
    try:
        logging.info("Let's get started....")

        logging.info("Creating spark object...")
        spark = create_spark_object(env.env, env.appName)
        logging.info("Spark object created successfully.....")

        for item in os.listdir(env.source_olap):
            print(f"File is {item}")

            file_dir = env.source_olap + '\\' + item
            print(f"File is in ==> {file_dir}")

            if item.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif item.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferSchema = env.inferSchema

        logging.info(f"Reading a {file_format} file....")
        city_data = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
        display_data(city_data)

        for file in os.listdir(env.source_oltp):
            print(f"File is {file}")

            file_dir = env.source_oltp + '\\' + file
            print(f"File is in ==> {file_dir}")

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferSchema = env.inferSchema
        logging.info(f"Reading a {file_format} file....")
        fact_data = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
        display_data(fact_data)

        logging.info("Starting the processing of data....")
        city_data_sel, presc_data_sel = process_data(city_data,fact_data)

        logging.info("CITY DATA ======>")
        display_data(city_data_sel)

        logging.info("FACT DATA ======>")
        display_data(presc_data_sel)






    except Exception as e:
        logging.error("An error occured while running the main() function====", str(e))


if __name__ == "__main__":
    main()
    logging.info("Application done....")
