from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

flights = ["AI501", "AI502", "6E123", "SG456"]

def send_event(topic, flight, data):
    event = {
        "flight": flight,
        "timestamp": time.time(),
        **data
    }

    producer.send(topic, value=event)
    producer.flush()
    print(f"[KAFKA] {topic} -> {event}")


while True:
    flight = random.choice(flights)

    send_event(
        "FLIGHT_DELAY",
        flight,
        {"delay_minutes": random.choice([30, 60, 120, 180])}
    )

    send_event(
        "WEATHER_ALERT",
        flight,
        {"severity": random.choice(["Normal", "Moderate", "Severe"])}
    )

    send_event(
        "GATE_CHANGED",
        flight,
        {"new_gate": f"G{random.randint(1, 20)}"}
    )

    if random.random() < 0.15:
        send_event(
            "FLIGHT_CANCELLED",
            flight,
            {"reason": "Severe Weather"}
        )

    time.sleep(5)
