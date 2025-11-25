import os
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

def _make_bootstrap():
    host = os.environ.get("KAFKA_HOST", os.environ.get("KAFKA_BROKER", "kafka"))
    port = os.environ.get("KAFKA_PORT", "9092")
    return f"{host}:{port}"

class KafkaCentralProducer:
    def __init__(self):
        bs = _make_bootstrap()
        self.producer = KafkaProducer(
            bootstrap_servers=bs,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        # topics used
        self.topic_cp_engine = os.environ.get("KAFKA_TOPIC_CP", "cp_engine")
        self.topic_driver_updates = os.environ.get("KAFKA_TOPIC_DRIVER", "driver_updates")
        self.topic_monitor = os.environ.get("KAFKA_TOPIC_MONITOR", "central_monitor")

    def send_to_cp_engine(self, cp_id, payload):
        msg = {"cp_id": cp_id, "payload": payload}
        self._send(self.topic_cp_engine, msg)

    def send_to_driver(self, driver_id, payload):
        msg = {"driver_id": driver_id, "payload": payload}
        self._send(self.topic_driver_updates, msg)

    def publish_monitor(self, snapshot):
        self._send(self.topic_monitor, snapshot)

    def _send(self, topic, obj):
        try:
            self.producer.send(topic, obj)
            # optional: don't flush each time for performance, but flush occasionally
        except KafkaError as e:
            print("[KAFKA PRODUCER] Error enviando mensaje:", e)

    def flush(self):
        try:
            self.producer.flush()
        except Exception as e:
            print("[KAFKA PRODUCER] Flush error:", e)
