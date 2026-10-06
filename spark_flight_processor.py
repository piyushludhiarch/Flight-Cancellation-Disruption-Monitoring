import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import happybase

schema = StructType([
    StructField("flight_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("delay_minutes", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("gate", StringType(), True)
])

def process_batch(df, batch_id):
    records = df.collect()
    if not records:
        return

    try:
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table('flight_status')

        for row in records:
            flight_id = row['flight_id']
            event_type = row['event_type']
            delay = row['delay_minutes'] or 0
            weather = row['weather_condition'] or "Normal"
            gate = row['gate'] or "N/A"

            data_to_store = {
                b'info:event_type': str(event_type).encode('utf-8'),
                b'info:delay': str(delay).encode('utf-8'),
                b'info:weather': str(weather).encode('utf-8'),
                b'info:gate': str(gate).encode('utf-8')
            }
            table.put(flight_id.encode('utf-8'), data_to_store)

            if (delay >= 180 and weather.lower() == "severe") or event_type == "FLIGHT_CANCELLED":
                print(f"\n🚨 [DISRUPTION ALERT] Flight {flight_id}: Event={event_type}, Delay={delay}m, Weather={weather}\n")
            else:
                print(f"ℹ️ [STATUS UPDATE] Flight {flight_id} updated in HBase successfully.")

        connection.close()
    except Exception as e:
        print(f"Error writing to HBase: {e}")

spark = SparkSession.builder \
    .appName("FlightDisruptionMonitor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_val") \
    .select(from_json(col("json_val"), schema).alias("data")) \
    .select("data.*")

query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
