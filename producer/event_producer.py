import json
import random
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from schemas.event_schema import EcommerceEvent

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


EVENT_TYPES = ["view", "add_to_cart", "purchase"]


def generate_event():
    """
    Generates a single e-commerce event.
    Occasionally produces invalid data on purpose.
    """

    event_type = random.choice(EVENT_TYPES)

    event_data = {
        "event_type": event_type,
        "user_id": f"user_{random.randint(1, 1000)}",
        "product_id": f"product_{random.randint(1, 500)}",
        "event_timestamp": datetime.utcnow(),
        "ingestion_timestamp": datetime.utcnow(),
    }

    # Price logic
    if event_type == "purchase":
        event_data["price"] = round(random.uniform(10, 500), 2)
    else:
        event_data["price"] = None

    # Inject bad data ~10% of the time
    if random.random() < 0.1:
        event_data["event_type"] = "invalid_event"

    try:
        event = EcommerceEvent(**event_data)
        return event.model_dump(mode="json")
    
    except Exception as e:
        print(f"[INVALID EVENT] {e}")
        return None


def run_producer():
    while True:
        event = generate_event()
        if event:
            producer.send("ecommerce-events", event)
            print("[SENT]", event)
        time.sleep(1)



if __name__ == "__main__":
    run_producer()
