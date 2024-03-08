import logging

import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_ingestion import display_data

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_preprocessing")


def process_data(data1, data2):
    try:
        logger.warning("Starting process_data() method....")

        logger.warning("Converting certain columns to upper case...")
        city_data_sel = data1.select(upper(data1.city).alias("City"),
                                     data1.state_id,
                                     upper(data1.state_name).alias("State"),
                                     upper(data1.county_name).alias("County"),
                                     data1.population,
                                     data1.zips)

        logger.warning("Renaming certain columns in fact_data...")
        presc_data_sel = data2.select(data2.npi.alias("presc_id"),
                                      data2.nppes_provider_last_org_name.alias("presc_lname"),
                                      data2.nppes_provider_first_name.alias("presc_fname"),
                                      data2.nppes_provider_city.alias("presc_city"),
                                      data2.nppes_provider_state.alias("presc_state"),
                                      data2.specialty_description.alias("presc_specialist"),
                                      data2.drug_name,
                                      data2.total_claim_count,
                                      data2.total_day_supply,
                                      data2.total_drug_cost,
                                      data2.years_of_exp)

        logger.warning("Adding a new column to the fact data....")
        presc_data_sel = presc_data_sel.withColumn("Country", lit("USA"))

        logger.warning("COnverting the 'years_of_exp' column to a integer_format...")
        pattern = '\d+'
        idx = 0
        presc_data_sel = presc_data_sel.withColumn('years_of_exp', regexp_extract(col('years_of_exp'), pattern, idx))
        presc_data_sel = presc_data_sel.withColumn("years_of_exp", col("years_of_exp").cast('int'))

        logger.warning("Concatenating first & last name...")
        presc_data_sel = presc_data_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))

        logger.warning("Dropping 'presc_fname' & 'presc_lname'...")
        presc_data_sel = presc_data_sel.drop("presc_lname", "presc_fname")

        # logger.warning("Checking fo null values...")
        # null_value_count = presc_data_sel.select([count(when (isnan(c) | col(c).isNull(), c)).alias(c) for c in presc_data_sel.columns])
        # display_data(null_value_count)

        logger.info("Dropping the null values columns 'presc_id' & 'drug_name'....")
        presc_data_sel = presc_data_sel.dropna(subset='presc_id')
        presc_data_sel = presc_data_sel.dropna(subset='drug_name')
        # display_data(null_value_count)

    except Exception as e:
        logger.error("An error occured while executing process_data() method.See trace here ==>", str(e))
        raise
    else:
        logger.warning("process_data() method executed successfully....")

    return city_data_sel, presc_data_sel


def print_schema(dataframe):
    try:
        logger.warning("Executing printSchema() method....")
        schema = dataframe.schema.fields

        for i in schema:
            print(f'\t{i}')

    except Exception as e:
        logger.error("An error occured while executing printSchema() method.See trace  ==> ", str(e))


def check_for_nulls(dataframe):
    global check_null_data
    try:
        logger.info(f"Checking for null values in {dataframe}...")

        check_null_data = dataframe.select(
            [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns])

    except Exception as e:
        logger.warning(f"An error occured while executing check_for_nulls() method.See trace ===>", str(e))
    else:
        logger.warning("Check_for_nulls() method executed successfully...")
    return check_null_data
