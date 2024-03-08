import findspark

findspark.init()
import logging
import logging.config

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_transformation")

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import row_number



def data_report(city_data, presc_data):
    logger.info(f"Calculating total zip counts in {city_data}")
    city_data_split = city_data.withColumn('zipcounts', size(split(col('zips'), ' ')))

    logger.info("Calculating distinct prescribers and total_claim_counts...")
    presc_data_grp = presc_data.groupBy(presc_data.presc_state, presc_data.presc_city).agg(
        countDistinct("presc_id").alias("presc_counts"), sum("total_claim_count").alias("total_claim_counts"))

    logger.warning("Not reporting a city if there is no prescriber assigned...")
    df_city_join = city_data_split.join(presc_data_grp, (city_data.state_id == presc_data_grp.presc_state) & (
            city_data.City == presc_data_grp.presc_city), 'inner')

    final_data = df_city_join.select("City", "State", "population", "zipcounts", "presc_counts")

    return final_data


def data_report2(presc_data_sel):
    logger.warning("Executing another data report on presc_data(fact_data)....")

    logger.warning("Selecting only the those data with prescribers between 20 & 50  and ranking them based on the "
                   "total_claim_counts")

    window_spec = Window.partitionBy("presc_state").orderBy(col("total_claim_count").desc())
    # print(window_spec)
    # print("Problem is not the window_spec")
    presc_report = presc_data_sel.select("presc_id", "presc_fullname", "presc_state",
                                         "Country", "years_of_exp", "total_claim_count",
                                         "total_day_supply", "total_drug_cost").filter(
        (presc_data_sel.years_of_exp >= 20) & (presc_data_sel.years_of_exp <= 50)).withColumn("dense_rank", dense_rank().over(window_spec)).filter(col("dense_rank") <= 5)
    if presc_report.count() == 0:  # Check if presc_report is empty
        print("No records found matching the criteria.")
        return None  # Return None if no records found

    print("The criteria are matching....there is no problem with that")
    return presc_report