import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Configure Hadoop home directory for Windows compatibility
hadoop_dir = os.path.abspath(os.path.join(project_root, "hadoop"))
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["hadoop.home.dir"] = hadoop_dir
os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildCustomerDimension")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


stg = spark.read.jdbc(
    url=JDBC_URL,
    table="stg_supply_chain",
    properties=CONNECTION_PROPERTIES
)


dim_customer = (
    stg.select(
        "customer_id",
        col("customer_fname").alias("first_name"),
        col("customer_lname").alias("last_name"),
        col("customer_segment").alias("segment"),
        col("customer_city").alias("city"),
        col("customer_state").alias("state"),
        col("customer_country").alias("country"),
        col("customer_zipcode").alias("zipcode")
    )
    .dropDuplicates(["customer_id"])
)


print("Customer dimension rows:", dim_customer.count())


dim_customer.write \
    .option("truncate", "true") \
    .jdbc(
        JDBC_URL,
        "dim_customer",
        mode="overwrite",
        properties=CONNECTION_PROPERTIES
    )

print("dim_customer loaded successfully")


spark.stop()