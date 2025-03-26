import json
from kafka import KafkaAdminClient, KafkaClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, UnknownTopicOrPartitionError

from repositories.kafka_repositories.kafka_config import KafkaConfig
from entities.charging_message import ChargingMessage

class KafkaRepository:
    
    def __init__(self, kafka_config: KafkaConfig):
        self.kafka_config = kafka_config

    def ensure_topic_exists(
        self, 
        topic: str, 
        num_partitions: int = 1, 
        replication_factor: int = 1
        ) -> None:
        """
        Check if topic exists, create it if it doesn't
        """
        print(f"Ensuring topic {topic} exists...")
        
        try:
            
            # Check if topic exists
            topics = self.kafka_config.admin_client.list_topics()
            if topic not in topics:
                print(f"Topic {topic} does not exist. Creating...")
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                )
                self.kafka_config.admin_client.create_topics([new_topic])
                print(f"Topic {topic} created successfully")
        except TopicAlreadyExistsError:
            print(f"Topic {topic} already exists")
        except Exception as e:
            print(f"Error checking/creating topic: {e}")
            raise
        finally:
            self.kafka_config.admin_client.close()
            
    def publish(self, message: ChargingMessage, topic: str) -> None:
        """
        Publishes a charging station event message to Kafka
        """
        try:
            # Create message dictionary
            message_dict = {
                "session_id": message.session_id,
                "session_number": message.session_number,
                "station_id": message.station_id,
                "ev_id": message.ev_id,
                "event_type": message.event_type,
                "payload": message.payload
            }
            # Let the producer's value_serializer handle the serialization
            self.kafka_config.producer.send(topic, value=message_dict)
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise