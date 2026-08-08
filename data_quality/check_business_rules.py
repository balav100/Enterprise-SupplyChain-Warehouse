import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Configure Hadoop home directory for Windows compatibility
hadoop_dir = os.path.abspath(os.path.join(project_root, "hadoop"))
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["hadoop.home.dir"] = hadoop_dir
os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from database.config import CONNECTION_PROPERTIES, JDBC_URL
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("BusinessRuleValidation")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


fact = spark.read.jdbc(
    JDBC_URL,
    "fact_orders",
    properties=CONNECTION_PROPERTIES
)


print("\nBUSINESS RULE CHECK")
print("-" * 40)


negative_sales = (
    fact
    .filter(col("sales") < 0)
    .count()
)

print(
    f"Negative sales records: {negative_sales}"
)


negative_quantity = (
    fact
    .filter(col("quantity") < 0)
    .count()
)

print(
    f"Negative quantity records: {negative_quantity}"
)


invalid_profit_ratio = (
    fact
    .filter(
        (col("profit_ratio") < -5) |
        (col("profit_ratio") > 5)
    )
    .count()
)

print(
    f"Invalid profit ratio records: {invalid_profit_ratio}"
)


invalid_dates = (
    fact
    .filter(
        col("order_date").isNull()
    )
    .count()
)

print(
    f"Invalid dates: {invalid_dates}"
)


spark.stop()