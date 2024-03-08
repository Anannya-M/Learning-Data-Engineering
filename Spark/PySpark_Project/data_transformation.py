import findspark

findspark.init()
import logging
import logging.config

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Data_transformation")

from pyspark.sql.functions import *
from pyspark.sql.types import *


def data_report(city_data, presc_data):
        logger.info(f"Calculating total zip counts in {city_data}")
        city_data_split = city_data.withColumn('zipcounts', size(split(col('zips'), ' ')))

        logger.info("Calculating distinct prescribers and total_claim_counts...")
        presc_data_grp = presc_data.groupBy(presc_data.presc_state, presc_data.presc_city).agg(
            countDistinct("presc_id").alias("presc_counts"), sum("total_claim_count").alias("total_claim_counts"))

        logger.warning("Not reporting a city if there is no prescriber assigned...")
        df_city_join = city_data_split.join(presc_data_grp, (city_data.state_id == presc_data_grp.presc_state) & (city_data.City == presc_data_grp.presc_city), 'inner')

        final_data = df_city_join.select("City", "State", "population", "zipcounts", "presc_counts")

        return final_data
