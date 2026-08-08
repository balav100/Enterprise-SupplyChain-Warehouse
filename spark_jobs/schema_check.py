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

df.printSchema()

spark.stop()