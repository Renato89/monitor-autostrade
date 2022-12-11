from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.functions import (
    concat,
    expr,
    first,
    last,
    round,
    col,
    from_csv,
    lit,
    sum,
    unix_timestamp,
    window,
    avg,
    rint,
    when,
    count,
    current_timestamp
)


spark = SparkSession.builder.appName("VehiclesCount").getOrCreate()


spark.sparkContext.setLogLevel('WARN')

# Stream
dfstream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ultimi-avvistamenti")
    .load()
)

options = {"sep": ","}
schema = "targa INT, ingresso INT, uscita INT, lunghezza DOUBLE, partenza TIMESTAMP, arrivo TIMESTAMP, avvistamenti INT"

dfstream = (
    dfstream.selectExpr("CAST(value AS STRING)")
    .select(from_csv(col("value"), schema, options).alias("data"))
    .select("data.*")
)


df_count = dfstream \
    .withWatermark("arrivo", "20 minutes") \
    .dropDuplicates(["targa", "ingresso", "uscita", "partenza", "arrivo"]) \
    .withColumn('presenza', when(col('avvistamenti')==2, -1).otherwise(1)) \
    .groupBy(window('arrivo', "20 minutes", "10 minutes"), \
        dfstream.ingresso, dfstream.uscita) \
    .sum('presenza')


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id))).write.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "conteggio-per-tratto").save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = (
    df_count
    .withColumn("process_time", current_timestamp() - col("timestamp")) \
    .select(
        concat(
            "ingresso", lit(","),
            "uscita", lit(","),
            "sum(presenza)", lit(","),
            "process_time"
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .outputMode("complete")
    .start()
)

query.awaitTermination()
query.stop()
