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
schema = "targa INT, ingresso INT, uscita INT, partenza TIMESTAMP, arrivo TIMESTAMP, avvistamenti INT"

dfstream = (
    dfstream.selectExpr("CAST(value AS STRING)")
    .select(from_csv(col("value"), schema, options).alias("data"))
    .select("data.*")
)

# Immette i tratti autostradali
tratti = [
    (27, 9, 8.48),
    (9, 26, 17.42),
    (26, 10, 6.0),
    (10, 18, 12.3),
    (18, 23, 14.0),
    (23, 15, 17.6),
    (15, 5, 7.7),
    (5, 8, 10.9),
    (8, 3, 6.9),
    (3, 13, 9.8),
    (22, 1, 10.6),
    (1, 12, 10.9),
    (12, 25, 7.7),
    (25, 20, 17.7),
    (20, 2, 13.8),
    (2, 16, 14.1),
    (16, 4, 14.0),
    (4, 21, 25.7),
]

tratti_schema = StructType(
    [
        StructField("ingresso", IntegerType()),
        StructField("uscita", IntegerType()),
        StructField("lunghezza", DoubleType())
    ]
)

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()


df_count = dfstream \
    .join(df_tratti, (dfstream.ingresso == df_tratti.ingresso), 'left') \
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
    .withColumn("process_timestamp", current_timestamp()) \
    .select(
        concat(
            "ingresso", lit(","),
            "uscita", lit(","),
            "sum(presenza)", lit(","),
            "process_timestamp"
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .outputMode("complete")
    .start()
)

query.awaitTermination()
query.stop()
