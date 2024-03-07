#IMPORTING NECESSARY LIBRARIES AND FUNCTIONS
import env_variables as env
from spark_object_create import create_spark_object



def main():
    print("Let's get started....")

    spark = create_spark_object(env.env, env.appName)
    print("Spark object created successfully.....")
    


if __name__ == "__main__":
    main()