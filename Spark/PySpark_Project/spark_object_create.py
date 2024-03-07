from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_object(env, appName):
    try:
        if env == 'DEV':
            master = 'local'
        
        else:
            master = 'yarn'
    
        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    
    except Exception as e:
        print(str(e))
        raise
    
    return spark
