import logging

import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_preprocessing")

def process_data(data1, data2):
    try:
        logger.warning("Starting process_data() method....")

        logger.warning("Converting certain columns to upper case...")
        city_data_sel = data1.select(upper(col('city').alias("City")),
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

    except Exception as e:
        logger.error("An error occured while executing process_data() method.See trace here ==>", str(e))
        raise
    else:
        logger.warning("process_data() method executed successfully....")

    return city_data_sel,presc_data_sel
