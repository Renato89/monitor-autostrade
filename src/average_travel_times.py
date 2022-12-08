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
    current_timestamp
)

spark = SparkSession.builder.appName("LastSeen").getOrCreate()

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

# Velocità media per targa

df_average = dfstream \
    .join(df_tratti, (dfstream.ingresso == df_tratti.ingresso ), 'left') \
    .filter(col('avvistamenti') == 2) \
    .dropDuplicates(["targa", "ingresso", "uscita", "partenza", "arrivo"]) \
    .withColumn('velocità', rint(( (col('lunghezza') * 1000) / (unix_timestamp(col('arrivo')) - unix_timestamp(col('partenza')))) * 3.6)) \
    .groupBy('targa') \
    .agg(
        last(dfstream.ingresso).alias('ingresso'),
        last(dfstream.uscita).alias('uscita'),
        avg('velocità').alias('velocità_media')
    ) \
    .select('targa', 'ingresso', 'uscita', 'velocità_media')


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id))).write.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "velocita-media").save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = (
    df_average
    .withColumn("process_timestamp", current_timestamp()) \
    .select(
        concat(
            "targa", lit(","),
            "ingresso", lit(","),
            "uscita", lit(","),
            "velocità_media", lit(","),
            "process_timestamp"
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .outputMode("update")
    .start()
)

# Test
""" query = df_average\
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
