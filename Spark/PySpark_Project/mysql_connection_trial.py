from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("MySQLConnectionExample") \
    .config('spark.driver.extraclasspath', 'mysql-connector-j-8.3.0.jar') \
    .getOrCreate()

# MySQL database connection properties
url = "jdbc:mysql://localhost:3306/emp_details"
user = "root"
password = "root"
dbtable = "emp_personal_info"

# Read data from MySQL table into a DataFrame
df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .load()

# Display DataFrame schema and content
df.printSchema()
df.show()

# Stop SparkSession
spark.stop()
