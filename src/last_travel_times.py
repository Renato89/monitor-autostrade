from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    concat,
    last,
    rint,
    col,
    from_csv,
    lit,
    unix_timestamp,
    current_timestamp,
)

spark = SparkSession.builder.appName("LastTravelTimes").getOrCreate()

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


# Velocità ultimo avvistamento per targa

df_speed = dfstream \
    .filter(col('avvistamenti') == 2) \
    .dropDuplicates(["targa", "ingresso", "uscita", "partenza", "arrivo"]) \
    .groupBy(dfstream.targa) \
    .agg(
        last('arrivo').alias('arrivo'),
        last('partenza').alias('partenza'),
        last('lunghezza').alias('lunghezza'),
        last(dfstream.ingresso).alias('ingresso'),
        last(dfstream.uscita).alias('uscita')
    ) \
    .withColumn('percorrenza', unix_timestamp(col('arrivo')) - unix_timestamp(col('partenza'))) \
    .withColumn('velocità', rint(((col('lunghezza') * 1000) / (unix_timestamp(col('arrivo')) - unix_timestamp(col('partenza')))) * 3.6) ) \
    .select('targa', col('arrivo').alias('timestamp'), 'ingresso', 'uscita', 'percorrenza', 'velocità')

def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id))).write.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "velocita-per-targa").save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = (
    df_speed
    .withColumn("process_time", unix_timestamp(current_timestamp()) - unix_timestamp(col("timestamp"))) \
    .select(
        concat(
            "targa", lit(","),
            "timestamp", lit(","),
            "ingresso", lit(","),
            "uscita", lit(","),
            "percorrenza", lit(","),
            "velocità", lit(","),
            "process_time"
        ).alias("value")
    )
    .writeStream.foreachBatch(foreach_batch_id)
    .outputMode("update")
    .start()
)

# Test
# query = df_speed\
#     .writeStream \
#     .format("console") \
#     .option("numRows", 200) \
#     .outputMode("update") \
#     .start()

query.awaitTermination()
