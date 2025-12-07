# Kafka + Protobuf + Python ETL Challenge
### Data Engineering Bootcamp – Graduate Assignment

**Objective:**  
Build a modular, scalable ETL consumer in Python that consumes Protobuf messages from Kafka, applies SMT-style transformations, and writes valid records into Snowflake. A fully working Producer is provided so you can focus on the consumer / ETL design and implementation.

---

## 📚 Recommended Online Courses (Complete These Before Attempting the Challenge)

### **1. Apache Kafka Foundations (Start Here)**

Build a solid baseline of Kafka concepts: producers, consumers, partitions, brokers, offsets, delivery guarantees.

* **Apache Kafka Series – Learn Apache Kafka for Beginners v3**
  [https://avaloq.udemy.com/course/apache-kafka/](https://avaloq.udemy.com/course/apache-kafka/)

* **Apache Kafka Crash Course for Java and Python Developers**
  [https://avaloq.udemy.com/course/apache-kafka-crash-course-for-java-and-python-developers/](https://avaloq.udemy.com/course/apache-kafka-crash-course-for-java-and-python-developers/)

---

### **2. Serialization & Protocol Buffers (Required for Producer Schema)**

Understand `.proto` files, message contracts, backward compatibility, code generation, and multi-language serialization.

* **Complete Guide to Protocol Buffers 3 – Java, Golang, Python**
  [https://avaloq.udemy.com/course/protocol-buffers/](https://avaloq.udemy.com/course/protocol-buffers/)

---

### **3. Schema-Based Pipelines, Schema Registry & Data Contracts**

Learn how modern event-driven architectures enforce compatibility, validation, and structured data contracts.

* **Kafka for Developers – Data Contracts using Schema Registry**
  [https://avaloq.udemy.com/course/kafka-for-developers-data-contracts-using-schema-registry/](https://avaloq.udemy.com/course/kafka-for-developers-data-contracts-using-schema-registry/)

**OR**

* **Apache Kafka Series – Confluent Schema Registry & REST Proxy**
  [https://avaloq.udemy.com/course/confluent-schema-registry/](https://avaloq.udemy.com/course/confluent-schema-registry/)

---

### **4. Kafka Connect & ETL Workflows**

(Useful for understanding how your ETL pipeline challenge maps to real-world patterns)

* **Apache Kafka Series – Kafka Connect Hands-On Learning**
  [https://avaloq.udemy.com/course/kafka-connect/](https://avaloq.udemy.com/course/kafka-connect/)

---

### **5. Optional: Kafka Application Development**

(Not mandatory but helpful for understanding end-to-end pipelines)

* **Apache Kafka for Developers using Spring Boot**
  [https://avaloq.udemy.com/course/apache-kafka-for-developers-using-springboot/](https://avaloq.udemy.com/course/apache-kafka-for-developers-using-springboot/)
---

## Project Directory Structure (full)
```
kafka-etl-challenge/
│
├── producer/
│   ├── producer.py
│   ├── config.yaml
│   ├── schemas/                # .proto files (source)
│   │   ├── business_event.proto
│   │   ├── customer_event.proto
│   │   ├── account_event.proto
│   │   ├── trade_event.proto
│   │   ├── payment_event.proto
│   │   └── poison_event.proto
│   └── generated/              # Python protobuf outputs (from protoc)
│       ├── business_event_pb2.py
│       ├── customer_event_pb2.py
│       ├── account_event_pb2.py
│       ├── trade_event_pb2.py
│       ├── payment_event_pb2.py
│       └── poison_event_pb2.py
│
├── consumer/
│   ├── consumer.py             # Implement main orchestration here
│   ├── config.yaml             # consumer config (kafka, smt, snowflake)
│   ├── etl/
│   │   ├── pipeline.py         # KafkaETLPipeline (skeleton with comments)
│   │   ├── deserializer.py     # ProtobufDeserializer (skeleton with comments)
│   │   ├── sink/
│   │   │   └── snowflake_sink.py  # SnowflakeSink skeleton (comments)
│   │   ├── smt/
│   │   │   ├── base_smt.py     # BaseSMT (ABC) skeleton (comments)
│   │   │   └── bu_filter_smt.py# BusinessUnitFilterSMT skeleton (comments)
│   │   └── utils/
│   │       └── exceptions.py   # Optional: custom exceptions
│   └── models/                 # optional domain models
│
├── README.md                   # <- this file
└── requirements.txt

```

---

## Requirements
Place this in `requirements.txt`:

```

confluent-kafka>=2.0.0
protobuf>=3.20.0
googleapis-common-protos
snowflake-connector-python>=3.0.0
PyYAML

````

> The Producer uses `confluent-kafka` (Confluent's high-performance Python client). The consumer may use `confluent-kafka` as well, but you are free to implement using other Python Kafka clients if you prefer (the skeleton expects raw bytes from Kafka).

---

## Protobuf Schemas (put in `producer/schemas/`)

### `business_event.proto`
```proto
syntax = "proto3";

package dataflow;

message BusinessEvent {
  string event_id = 1;
  string business_unit = 2;
  string description = 3;
  int64 timestamp = 4;
}
````

### `customer_event.proto`

```proto
syntax = "proto3";

package dataflow;

message CustomerEvent {
  string customer_id = 1;
  string name = 2;
  string email = 3;
  string business_unit = 4;
  int64 registered_at = 5;
}
```

### `account_event.proto`

```proto
syntax = "proto3";

package dataflow;

message AccountEvent {
  string account_id = 1;
  string customer_id = 2;
  string business_unit = 3;
  double balance = 4;
  int64 updated_at = 5;
}
```

### `trade_event.proto`

```proto
syntax = "proto3";

package dataflow;

message TradeEvent {
  string trade_id = 1;
  string account_id = 2;
  string business_unit = 3;
  double amount = 4;
  string symbol = 5;
  int64 traded_at = 6;
}
```

### `payment_event.proto`

```proto
syntax = "proto3";

package dataflow;

message PaymentEvent {
  string payment_id = 1;
  string account_id = 2;
  string business_unit = 3;
  double amount = 4;
  string method = 5;
  int64 processed_at = 6;
}
```

### `poison_event.proto`

```proto
syntax = "proto3";

package dataflow;

message PoisonEvent {
  string reason = 1;
}
```

**After adding the `.proto` files**, generate Python classes using `protoc`:

```bash
protoc --python_out=./producer/generated producer/schemas/*.proto
```

That will create the `*_pb2.py` files under `producer/generated/`. The provided `producer.py` imports those generated modules.

---

## Producer (complete, ready-to-run)

Place the file at `producer/producer.py`.

```python
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
```

Also include `producer/config.yaml`:

```yaml
bootstrap_servers: "localhost:9092"
topic_schema_map:
  business-events: "BusinessEvent"
  customer-events: "CustomerEvent"
  account-events: "AccountEvent"
  trade-events: "TradeEvent"
  payment-events: "PaymentEvent"
poison_pill_interval_messages: 200
emit_interval_ms: 200
```

---

## Consumer-side skeletons (You must implement these; **NO implementation code** inside — only commented skeletons)

> These skeletons are intentionally minimal. They show the interface and detailed comments that explain exactly what you must implement.

### `consumer/etl/deserializer.py` – ProtobufDeserializer skeleton

```python
class ProtobufDeserializer:
    """
    Responsibilities:
    - Maintain a mapping (schema registry) from topic -> Python Protobuf class.
    - Given (topic, raw_bytes) return a Python dict representing the message.
      e.g., {"field1": "value", "business_unit": "BU1", ...}
    - Detect poison messages or decode failures and surface them as controlled exceptions
      so the pipeline can either send to DLQ or skip.
    - Should be generic: adding a new topic + proto should not require code changes,
      only updating the mapping (config).
    """

    def __init__(self, schema_registry: dict):
        """
        schema_registry: dict mapping topic -> protobuf class
        Example:
        {
            "business-events": business_event_pb2.BusinessEvent,
            "customer-events": customer_event_pb2.CustomerEvent,
            ...
        }
        """
        pass

    def deserialize(self, topic: str, raw_bytes: bytes):
        """
        Given a topic and raw bytes:
        - lookup the protobuf class
        - parse raw_bytes into message instance
        - convert message -> dict (JSON-like)
        - return dict on success
        - on poison / decode error: either return None or raise a dedicated exception
        """
        pass
```

---

### `consumer/etl/smt/base_smt.py` – Base SMT (ABC)

```python
from abc import ABC, abstractmethod

class BaseSMT(ABC):
    """
    Abstract Base Class (ABC):
    - 'ABC' is Python's Abstract Base Class mechanism (from `abc` module).
      It allows defining abstract methods that must be implemented by subclasses.
    - Use BaseSMT as the interface for all single-message transforms (SMTs).
    - Each SMT must implement `apply(record: dict) -> dict | None`.
      - Return a dict to pass downstream.
      - Return None to drop the record.
    """

    @abstractmethod
    def apply(self, record: dict) -> dict | None:
        """
        Apply transform to the record.
        - record: dict representation of the message
        - return: transformed record (dict) or None to drop
        """
        pass
```

---

### `consumer/etl/smt/bu_filter_smt.py` – Business Unit Filter SMT skeleton

```python
class BusinessUnitFilterSMT(BaseSMT):
    """
    Business Unit Filter SMT:
    - Config-driven allowed business units.
    - Drops records where:
      - business_unit field is missing OR
      - business_unit not in allowed set
    - Expected config example:
      {"allowed_business_units": ["BU1", "BU2"]}
    """

    def __init__(self, config: dict):
        """
        Store allowed BUs from config into a set for O(1) lookup.
        """
        pass

    def apply(self, record: dict):
        """
        Steps to implement:
        1. read bu = record.get("business_unit") or record.get("business_unit_id")
        2. if bu is None: return None  # drop
        3. if bu not in allowed set: return None  # drop
        4. else: return record  # pass downstream
        """
        pass
```

---

### `consumer/etl/sink/snowflake_sink.py` – Snowflake sink skeleton

```python
class SnowflakeSink:
    """
    Snowflake Sink responsibilities:
    - Initialize a Snowflake connection using configuration
    - Provide a write(table: str, record: dict) method to insert records
    - Support batching for performance (optional/bonus)
    - Provide retry logic for transient failures (optional/bonus)
    - Should not hardcode SQL table columns; use either a JSON column or
      a configurable column mapping in config.yaml
    """

    def __init__(self, connection_config: dict):
        """
        connection_config examples:
        {
          "user": "...",
          "password": "...",
          "account": "...",
          "warehouse": "...",
          "database": "...",
          "schema": "..."
        }
        Initialize the connector here.
        """
        pass

    def write(self, table: str, record: dict):
        """
        Insert a single record (or a batch) into Snowflake.
        - Convert record dict -> appropriate SQL/JSON payload
        - Use parametrized queries to avoid SQL injection
        """
        pass
```

---

### `consumer/etl/pipeline.py` – KafkaETLPipeline skeleton

```python
class KafkaETLPipeline:
    """
    Central pipeline orchestration:
    Responsibilities:
    - Wrap Kafka consumer polling loop
    - For each message:
       1. Deserialize using ProtobufDeserializer
       2. If deserialization fails or is a poison message -> handle (DLQ/skip/log)
       3. Apply SMTs in configured order (each SMT may drop the record)
       4. If record passes all SMTs -> write to SnowflakeSink
    - Provide graceful shutdown, commit offsets, and metrics hooks
    - Must be config-driven (topics, SMT chain, sink mapping)
    """

    def __init__(self, consumer_config: dict, deserializer, smt_chain: list, sink):
        """
        consumer_config: Kafka consumer configuration dict
        deserializer: instance of ProtobufDeserializer
        smt_chain: list of BaseSMT instances (in order)
        sink: instance of SnowflakeSink (or other sink)
        """
        pass

    def start(self):
        """
        - Start polling loop
        - Process messages as described above
        - Implement offset commit strategy (auto or manual)
        - Implement poison message policy (skip or DLQ)
        """
        pass
```

---

## Sample `consumer/config.yaml` (example, you may extend)

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  group_id: "bootcamp-consumer-group"
  topics:
    - business-events
    - customer-events
    - account-events
    - trade-events
    - payment-events

smt:
  bu_filter:
    allowed_business_units:
      - BU1
      - BU2

snowflake:
  user: "SF_USER"
  password: "SF_PASS"
  account: "SF_ACCOUNT"
  warehouse: "COMPUTE_WH"
  database: "PROD_DB"
  schema: "PUBLIC"
```

---

## How graduates should work (instructions / expectations)

1. Clone repo skeleton and generated protobuf modules.
2. Run Producer to generate test traffic:

   * `cd producer && python3 producer.py`
3. Implement consumer pieces:

   * `deserializer.py`, `etl/pipeline.py`, `smt/bu_filter_smt.py`, `sink/snowflake_sink.py`
4. Start consumer (you provide your `consumer.py` orchestration).
5. Validate:

   * Records with allowed `business_unit` flow to Snowflake (or printed sink)
   * Records missing or outside allowed BUs are dropped
   * Poison-pill messages are handled (DLQ or logged)
6. Bonus work: batching to Snowflake, DLQ topic, retries, metrics.

---

## Acceptance criteria (example rubric)

* Producer emits valid Protobuf messages and poison messages (10 pts)
* Consumer correctly deserializes messages (20 pts)
* BusinessUnitFilterSMT drops/inherits correctly (20 pts)
* Snowflake sink writes records and handles failures (20 pts)
* Robustness: poison handling, retries, clean shutdown (15 pts)
* Clean code, tests, README updates, and documentation (15 pts)

---

## Notes for instructors

* Provide generated `*_pb2.py` files in `producer/generated/` to avoid forcing grads to install `protoc`.
* If you prefer a hands-off consumer, ask grads to print to console or write to a local file instead of actual Snowflake (use Snowflake sink skeleton to show where to integrate real connector).
* Encourage you to write unit tests for SMT logic and deserializer.

---

## Quick commands recap

* Install deps:

  ```bash
  python3 -m pip install -r requirements.txt
  ```
* Generate protos (if not included):

  ```bash
  protoc --python_out=./producer/generated producer/schemas/*.proto
  ```
* Run producer:

  ```bash
  cd producer
  python3 producer.py
  ```
* You run consumer after implementing skeletons.

---

🚀 Good luck — build something you would proudly deploy in production!