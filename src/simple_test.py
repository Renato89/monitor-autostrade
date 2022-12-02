# Esempio base di utilizzo di pyspark: riporta i dati in uscita 
# così come sono recuperati da Kafka

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import (
    concat,
    count,
    col,
    from_csv,
    lit,
    count,
    max,
    first,
    struct
)

spark = SparkSession.builder.appName("TestDataReception").getOrCreate()
address = "localhost:9092"
topic = "rilevamenti-targa"

# Stream
dfstream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", address)
    .option("subscribe", topic)
    .load()
)


# il record in recezione da kafka è contenuto in un unico
# campo chiamato "value"
options = {"sep": ","}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"
dfstream = (
    dfstream.selectExpr("CAST(value AS STRING)")
    .select(from_csv(col("value"), schema, options).alias("data"))
    .select("data.*")
)

# Output in Kafka
print("\n\n\nStarting...\n\n\n")

query = (
    dfstream.writeStream.format("console")
    .start()
)

query.awaitTermination()

query.stop()
