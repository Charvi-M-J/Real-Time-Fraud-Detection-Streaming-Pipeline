from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType


transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_number", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_type", StringType()),
    StructField("payment_channel", StringType()),
    StructField("device_id", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("transaction_timestamp", StringType()),
    StructField("is_international", BooleanType()),
    StructField("status", StringType()),
])


@dp.table(
    name="finguard.silver.transactions",
    comment="Parsed and cleaned transactions data from Kafka bronze source"
)
def transactions_silver() -> DataFrame:

    return (
        spark.readStream.table("finguard.bronze.transactions")
        .withColumn("data", F.from_json(F.col("value"), transaction_schema))
        .select(
            F.col("data.transaction_id"),
            F.col("data.customer_id"),
            F.col("data.card_number"),
            F.col("data.merchant_id"),
            F.col("data.merchant_name"),
            F.col("data.merchant_category"),
            F.col("data.amount").cast("decimal(18,2)").alias("amount"),
            F.col("data.currency"),
            F.col("data.transaction_type"),
            F.col("data.payment_channel"),
            F.col("data.device_id"),
            F.col("data.city"),
            F.col("data.country"),
            F.to_timestamp(F.col("data.transaction_timestamp")).alias("transaction_timestamp"),
            F.col("data.is_international"),
            F.col("data.status"),
            F.col("key"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp"),
        )
    )