#!/usr/bin/env python3
"""
producer.py
- Continuously emits Protobuf messages to multiple topics
- Uses confluent-kafka Producer
- Config-driven topic->schema mapping
- Emits poison-pill messages periodically
"""

import random
import time
import uuid
import yaml
from confluent_kafka import Producer
from google.protobuf.json_format import MessageToJson
from datetime import datetime
import os
import sys

# Import generated protobuf modules (ensure Python path includes producer/generated)
sys.path.append(os.path.join(os.path.dirname(__file__), "generated"))

from business_event_pb2 import BusinessEvent
from customer_event_pb2 import CustomerEvent
from account_event_pb2 import AccountEvent
from trade_event_pb2 import TradeEvent
from payment_event_pb2 import PaymentEvent
from poison_event_pb2 import PoisonEvent

# ---------------------------
# Load producer config
# ---------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

TOPIC_SCHEMA_MAP = CONFIG.get("topic_schema_map", {
    "business-events": "BusinessEvent",
    "customer-events": "CustomerEvent",
    "account-events": "AccountEvent",
    "trade-events": "TradeEvent",
    "payment-events": "PaymentEvent",
})
POISON_INTERVAL = CONFIG.get("poison_pill_interval_messages", 200)
EMIT_INTERVAL_MS = CONFIG.get("emit_interval_ms", 200)
BOOTSTRAP = CONFIG.get("bootstrap_servers", "localhost:9092")

# ---------------------------
# Kafka Producer setup
# ---------------------------
producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 10,
}
producer = Producer(producer_conf)


# ---------------------------
# Random data generators per schema
# ---------------------------
def random_business_event():
    return BusinessEvent(
        event_id=str(uuid.uuid4()),
        business_unit=random.choice(["BU1", "BU2", "BU3", None]),
        description="Business event example",
        timestamp=int(time.time())
    )

def random_customer_event():
    return CustomerEvent(
        customer_id=str(uuid.uuid4()),
        name=random.choice(["Alice", "Bob", "Charlie"]),
        email=f"{uuid.uuid4().hex[:8]}@example.com",
        business_unit=random.choice(["BU1", "BU2", None]),
        registered_at=int(time.time())
    )

def random_account_event():
    return AccountEvent(
        account_id=str(uuid.uuid4()),
        customer_id=str(uuid.uuid4()),
        business_unit=random.choice(["BU1", "BU2", "BUX", None]),
        balance=round(random.uniform(100, 20000), 2),
        updated_at=int(time.time())
    )

def random_trade_event():
    return TradeEvent(
        trade_id=str(uuid.uuid4()),
        account_id=str(uuid.uuid4()),
        business_unit=random.choice(["BU1", "BU2", None]),
        amount=round(random.uniform(10, 100000), 2),
        symbol=random.choice(["AAPL", "GOOGL", "MSFT", "AMZN"]),
        traded_at=int(time.time())
    )

def random_payment_event():
    return PaymentEvent(
        payment_id=str(uuid.uuid4()),
        account_id=str(uuid.uuid4()),
        business_unit=random.choice(["BU1", "BU2", None]),
        amount=round(random.uniform(1, 5000), 2),
        method=random.choice(["CARD", "BANK", "CRYPTO"]),
        processed_at=int(time.time())
    )

EVENT_BUILDERS = {
    "BusinessEvent": random_business_event,
    "CustomerEvent": random_customer_event,
    "AccountEvent": random_account_event,
    "TradeEvent": random_trade_event,
    "PaymentEvent": random_payment_event,
}

def poison_event():
    return PoisonEvent(reason="Simulated poison pill")

# ---------------------------
# Produce loop
# ---------------------------
def main():
    print("Starting Producer → bootstrap:", BOOTSTRAP)
    counter = 0
    emit_interval = EMIT_INTERVAL_MS / 1000.0

    topics = list(TOPIC_SCHEMA_MAP.keys())
    if not topics:
        print("No topics configured. Exiting.")
        return

    try:
        while True:
            topic = random.choice(topics)
            schema_name = TOPIC_SCHEMA_MAP.get(topic)

            # periodically send a poison-pill to simulate bad message
            if counter > 0 and (counter % POISON_INTERVAL == 0):
                msg_obj = poison_event()
                print(f"[poison] -> {topic}")
            else:
                # build normal message
                builder = EVENT_BUILDERS.get(schema_name)
                if not builder:
                    print(f"No builder for schema {schema_name} (topic {topic})")
                    time.sleep(emit_interval)
                    counter += 1
                    continue
                msg_obj = builder()

            serialized = msg_obj.SerializeToString()
            producer.produce(topic, value=serialized)
            producer.poll(0)  # serve delivery callbacks
            print(f"[{topic}] → {schema_name} {MessageToJson(msg_obj)}")

            counter += 1
            time.sleep(emit_interval)

    except KeyboardInterrupt:
        print("Stopping producer... flushing")
    finally:
        producer.flush(10)
        print("Producer stopped.")

if __name__ == "__main__":
    main()