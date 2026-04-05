from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzePipeline").getOrCreate()

    df = spark.createDataFrame(
        data = [
            (1, "Alice", 30),
            (2, "Bob", 25),
            (3, "Charlie", 35)
        ],
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
    )

    df.show()

    df.writeTo("argos_finance_catalog.bronze.example").createOrReplace()
