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
from pyspark.sql.functions import *
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildDateDimension")
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


dim_date = (
    stg
    .withColumn(
        "order_ts",
        to_timestamp(
            col("order_date"),
            "M/d/yyyy H:mm"
        )
    )
    .select(
        to_date("order_ts").alias("full_date")
    )
    .dropDuplicates()
    .dropna()
)


dim_date = (
    dim_date
    .withColumn("year", year("full_date"))
    .withColumn("quarter", quarter("full_date"))
    .withColumn("month", month("full_date"))
    .withColumn("day", dayofmonth("full_date"))
)


print("Date dimension rows:", dim_date.count())


dim_date.write.jdbc(
    JDBC_URL,
    "dim_date",
    mode="overwrite",
    properties=CONNECTION_PROPERTIES
)


print("dim_date loaded successfully")


spark.stop()