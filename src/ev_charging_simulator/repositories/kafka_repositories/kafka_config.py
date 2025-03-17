from kafka import KafkaAdminClient, KafkaClient, KafkaProducer
import json

class KafkaConfig:
    def __init__(self, kafka_bootsrap_Server: str):
        self.client = KafkaClient(bootstrap_servers=kafka_bootsrap_Server)
        self.admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootsrap_Server)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootsrap_Server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )