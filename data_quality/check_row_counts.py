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


spark = (
    SparkSession.builder
    .appName("RowCountValidation")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


tables = [
    "stg_supply_chain",
    "dim_customer",
    "dim_category",
    "dim_product",
    "dim_shipping",
    "dim_region",
    "dim_date",
    "fact_orders"
]


print("\nROW COUNT CHECK")
print("-" * 40)


for table in tables:

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=table,
        properties=CONNECTION_PROPERTIES
    )

    print(f"{table}: {df.count()}")


spark.stop()