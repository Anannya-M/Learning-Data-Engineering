# IMPORTING NECESSARY LIBRARIES AND FUNCTIONS
import logging
import os

import logging.config
import env_variables as env
from spark_object_create import create_spark_object
from data_ingestion import load_file, display_data
from data_preprocessing import process_data, print_schema, check_for_nulls
from data_transformation import data_report, data_report2
from extraction import extract_files
from persist import hive_data_persist, hive_data_persist_for_presc, load_data_mysql

from pyspark.sql.functions import *

logging.config.fileConfig("Properties/configuration/logging.config")


# D:\Clg_Projects\Spark\Playing-with-Data\Spark\PySpark_Project\Properties\configuration\logging.config


def main():
    global file_format, file_dir, header, inferSchema, city_data_sel
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
        city_data = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                              inferSchema=inferSchema)
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
        fact_data = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                              inferSchema=inferSchema)
        display_data(fact_data)

        logging.info("Starting the processing of data....")
        city_data_sel, presc_data_sel = process_data(city_data, fact_data)

        logging.info("CITY DATA ======>")
        display_data(city_data_sel)

        logging.info("FACT DATA ======>")
        display_data(presc_data_sel)

        logging.info("Printing the schema of the fact data...")
        print_schema(presc_data_sel)

        logging.info("Checking for null values in dataframe after processing....")
        check_nulls_df = check_for_nulls(presc_data_sel)
        display_data(check_nulls_df)

        logging.info("Filling the null values in 'total_claim_count' using fillna()...")
        mean_claim = presc_data_sel.select(mean(col('total_claim_count'))).collect()[0][0]
        presc_data_sel = presc_data_sel.fillna(mean_claim, "total_claim_count")

        logging.info("Checking for null values in dataframe after using fillna()....")
        check_nulls_df = check_for_nulls(presc_data_sel)
        display_data(check_nulls_df)

        logging.info("Starting the process of data transformation....")
        final_data = data_report(city_data_sel, presc_data_sel)

        logging.info("Now, diplaying the data report (tranformed data)....")
        display_data(final_data)

        logging.info("Displaying a second report on the fact data....")
        report2 = data_report2(presc_data_sel)
        display_data(report2)

        logging.info("Extracting the reports generated....")
        hive_data_persist(spark=spark, data=final_data, dataname="city_data", partitionBy="State", mode='append')
        hive_data_persist_for_presc(spark=spark, data=report2, dataname="presc_data", partitionBy="presc_state",
                                    mode='append')

        logging.info("Loading the data into MySQL....")
        load_data_mysql(spark=spark, data=final_data, dataname="city_data",
                        url="jdbc:mysql://localhost:3306/pysparkproj", dbtable="city_data", mode='append',
                        user=env.user, password=env.password)


    except Exception as e:
        logging.error("An error occured while running the main() function====", str(e))
    else:
        logging.warning("main() method executed successfully....")


if __name__ == "__main__":
    main()
    logging.info("Application done....")
