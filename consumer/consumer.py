#!/usr/bin/env python3

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
GENERATED_PATH = os.path.join(BASE_DIR, "producer", "generated")
sys.path.append(GENERATED_PATH)
from confluent_kafka import Consumer
from google.protobuf.json_format import MessageToJson

# add generated protobuf path
# sys.path.append(os.path.join(os.path.dirname(__file__), "generated"))

from business_event_pb2 import BusinessEvent
from customer_event_pb2 import CustomerEvent
from account_event_pb2 import AccountEvent
from trade_event_pb2 import TradeEvent
from payment_event_pb2 import PaymentEvent
from poison_event_pb2 import PoisonEvent


# ---------------------------
# Kafka Consumer config
# ---------------------------
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "demo-consumer-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_conf)

topics = [
    "business-events",
    "customer-events",
    "account-events",
    "trade-events",
    "payment-events",
]

consumer.subscribe(topics)

# ---------------------------
# Topic → Protobuf mapping
# ---------------------------
TOPIC_PROTO_MAP = {
    "business-events": BusinessEvent,
    "customer-events": CustomerEvent,
    "account-events": AccountEvent,
    "trade-events": TradeEvent,
    "payment-events": PaymentEvent,
}


def main():
    print("Consumer started... listening to topics")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("Consumer error:", msg.error())
                continue

            topic = msg.topic()
            value = msg.value()

            # Poison pill handling
            if topic not in TOPIC_PROTO_MAP:
                poison = PoisonEvent()
                poison.ParseFromString(value)
                print(f"[POISON] {MessageToJson(poison)}")
                continue

            # Normal message
            proto_cls = TOPIC_PROTO_MAP[topic]
            event = proto_cls()
            event.ParseFromString(value)

            print(f"[{topic}] {MessageToJson(event)}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
