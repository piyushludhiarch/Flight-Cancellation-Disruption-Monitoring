from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    coalesce,
    lit,
    when,
    max as spark_max
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
import happybase


spark = (
    SparkSession.builder
    .appName("FlightDisruptionMonitoring")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("flight", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("delay_minutes", DoubleType(), True),
    StructField("severity", StringType(), True),
    StructField("new_gate", StringType(), True),
    StructField("reason", StringType(), True)
])


def read_kafka_topic(topic):

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
    )


delay_stream = read_kafka_topic("FLIGHT_DELAY")

weather_stream = read_kafka_topic("WEATHER_ALERT")

cancel_stream = read_kafka_topic("FLIGHT_CANCELLED")

gate_stream = read_kafka_topic("GATE_CHANGED")


def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    connection = happybase.Connection("localhost")

    table = connection.table("flight_status")

    rows = batch_df.collect()

    for row in rows:

        flight = row["flight"]

        if not flight:
            continue

        delay = row["delay_minutes"]
        weather = row["severity"]
        gate = row["new_gate"]
        reason = row["reason"]

        delay = delay if delay is not None else 0
        weather = weather if weather else "Unknown"
        gate = gate if gate else "Unknown"
        reason = reason if reason else "None"

        alert = "NONE"

        if delay >= 120 and weather.lower() == "severe":
            alert = "DISRUPTION_ALERT"

        elif delay >= 120:
            alert = "DELAY_ALERT"

        elif weather.lower() == "severe":
            alert = "WEATHER_ALERT"

        status = "ON_TIME"

        if delay >= 120:
            status = "DELAYED"

        if reason != "None":
            status = "CANCELLED"

        table.put(
            flight.encode(),
            {
                b"info:flight": flight.encode(),
                b"info:delay_minutes": str(delay).encode(),
                b"info:weather": weather.encode(),
                b"info:gate": gate.encode(),
                b"info:reason": reason.encode(),
                b"info:status": status.encode(),
                b"info:alert": alert.encode()
            }
        )

        print(
            f"[HBASE] {flight} | "
            f"delay={delay} | "
            f"weather={weather} | "
            f"gate={gate} | "
            f"status={status} | "
            f"alert={alert}"
        )

    connection.close()


delay_query = (
    delay_stream
    .select(
        "flight",
        "timestamp",
        "delay_minutes"
    )
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/flight_delay_checkpoint")
    .start()
)


weather_query = (
    weather_stream
    .select(
        "flight",
        "timestamp",
        "severity"
    )
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/weather_checkpoint")
    .start()
)


cancel_query = (
    cancel_stream
    .select(
        "flight",
        "timestamp",
        "reason"
    )
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/cancel_checkpoint")
    .start()
)


gate_query = (
    gate_stream
    .select(
        "flight",
        "timestamp",
        "new_gate"
    )
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/gate_checkpoint")
    .start()
)


spark.streams.awaitAnyTermination()
