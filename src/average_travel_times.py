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
)

spark = SparkSession.builder.appName("LastSeen").getOrCreate()

# Stream
dfstream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "rilevamenti-targa")
    .load()
)

options = {"sep": ","}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

dfstream = (
    dfstream.selectExpr("CAST(value AS STRING)")
    .select(from_csv(col("value"), schema, options).alias("data"))
    .select("data.*")
)

# Immette i tratti autostradali
tratti = [
    (1, 27, 9, 8.48),
    (2, 9, 26, 17.42),
    (3, 26, 10, 6.0),
    (4, 10, 18, 12.3),
    (5, 18, 23, 14.0),
    (6, 23, 15, 17.6),
    (7, 15, 5, 7.7),
    (8, 5, 8, 10.9),
    (9, 8, 3, 6.9),
    (10, 3, 13, 9.8),
    (11, 22, 1, 10.6),
    (12, 1, 12, 10.9),
    (13, 12, 25, 7.7),
    (14, 25, 20, 17.7),
    (15, 20, 2, 13.8),
    (16, 2, 16, 14.1),
    (17, 16, 4, 14.0),
    (18, 4, 21, 25.7),
]

tratti_schema = StructType(
    [
        StructField("tratto", IntegerType()),
        StructField("ingresso", IntegerType()),
        StructField("uscita", IntegerType()),
        StructField("lunghezza", DoubleType()),
    ]
)

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()


# Tempi di percorrenza e velocità
# partizione_per_targa = Window.partitionBy("targa").orderBy("timestamp")

tempi_di_percorrenza = (
    dfstream.join(
        df_tratti,
        [(dfstream.varco == df_tratti.ingresso) | (dfstream.varco == df_tratti.uscita)],
        "inner",
    )
    .select("targa", "tratto", "timestamp", "lunghezza")
    .groupBy("targa", "tratto")
    .agg(
        last("timestamp"),
        first("timestamp"),
        first("lunghezza").alias("lunghezza"),
    )
    .withColumn(
        "percorrenza",
        unix_timestamp("last(timestamp)") - unix_timestamp("first(timestamp)"),
    )
    .where(col("percorrenza") > 0)
    .where(col("timestamp") > col("last(timestamp)") - expr("INTERVAL 2 minutes"))
    .withColumn("velocità", (col("lunghezza") / col("percorrenza")) * 3600)
    .select(
        "targa", "tratto", "lunghezza", "last(timestamp)", "percorrenza", "velocità"
    )
)


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id))).write.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "tempi-di-percorrenza").save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = (
    tempi_di_percorrenza.select(
        concat(
            "targa",
            lit(","),
            "tratto",
            lit(","),
            "lunghezza",
            lit(","),
            "last(timestamp)",
            lit(","),
            "percorrenza",
            lit(","),
            "velocità",
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .trigger(processingTime="5 seconds")
    .outputMode("update")
    .start()
)

# Test
""" query_console = ultimi_avvistamenti\
    .select(concat(
        "targa", lit(","),
        "timestamp", lit(","),
        "tratto", lit(","),
        "ingresso", lit(","),
        "avvistamenti", lit(","),
        "nazione").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .format("console") \
    .option("numRows", 200) \
    .outputMode("complete") \
    .start()
 """
""" query_console.stop()
 """

query.awaitTermination()
