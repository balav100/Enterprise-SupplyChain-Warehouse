from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SupplyChainWarehouse")
    .getOrCreate()
)

df = spark.read.csv(
    "data/DataCoSupplyChainDataset.csv",
    header=True,
    inferSchema=True
)

df.select(
    "order date (DateOrders)",
    "shipping date (DateOrders)"
).show(5, False)

spark.stop()