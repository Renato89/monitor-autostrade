from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql.functions import (
    concat,
    count,
    col,
    from_csv,
    lit,
    count,
    max,
    first,
    last,
    struct,
    window,
    current_timestamp
)

spark = SparkSession.builder.appName("LastSeen").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

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


# ultimi avvistamenti:  per ogni tratto si cercano le targhe che sono avvistate sia in entrata che in uscita,
#                       i record con questa caratteristica avranno conteggio 2
ultimi_avvistamenti = (
    dfstream.join(df_tratti, (dfstream.varco == df_tratti.ingresso) | (dfstream.varco == df_tratti.uscita), 'left')
    .withWatermark("timestamp", "20 minutes") \
    .groupBy( window('timestamp', "20 minutes", "10 minutes"),  \
        'targa', 'ingresso', 'lunghezza', 'uscita') \
    .agg(
        first('timestamp').alias('partenza'), 
        last('timestamp').alias('arrivo'),
        count('timestamp').alias('avvistamenti')
    )
)
# viene usata una finestra temporale per evitare conteggi superiori a 2
# i conteggi a 1 vengono filtrati successivamente dai processi successivi


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id))) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "ultimi-avvistamenti").save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = (
    ultimi_avvistamenti
    .select(
        concat(
            "targa", lit(","),
            "ingresso", lit(","),
            "uscita", lit(","),
            "lunghezza", lit(","),
            "partenza", lit(","),
            "arrivo", lit(","),
            "avvistamenti"
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .outputMode("update")
    .start()
)

# Test
# query = ultimi_avvistamenti\
#     .writeStream \
#     .format("console") \
#     .option("numRows", 200) \
#     .outputMode("complete") \
#     .start()



query.awaitTermination()
