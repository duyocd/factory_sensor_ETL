from typing import Any, Dict
from confluent_kafka import Consumer, KafkaError, Producer
import json
import base64
import logging
from config import KAFKA_CONFIG, TOPICS

logger = logging.getLogger('mappingdata')

class DataMapper:
    def __init__(self, kafka_config: Dict[str, Any], input_topic: str, output_topic: str):
        consumer_config = kafka_config.copy()
        consumer_config['group.id'] = 'mapping-group'
        
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(kafka_config)
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer.subscribe([input_topic])

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    @staticmethod
    def decode_from_base64(encoded_data: str) -> Dict:
        decoded_bytes = base64.b64decode(encoded_data)
        decoded_str = decoded_bytes.decode('utf-8')
        return json.loads(decoded_str)

    def process_message(self, data: Dict) -> Dict:
        """Override this method to implement custom mapping logic"""
        mapping_data = ' '.join(str(x) for x in data.values())
        data['STD_ID'] = mapping_data
        return data

    def run(self):
        logger.info("Starting data mapper...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    break

                try:
                    encoded_data = msg.value().decode('utf-8')
                    data = self.decode_from_base64(encoded_data)
                    if data:
                        mapped_data = self.process_message(data)
                        self.producer.produce(
                            self.output_topic,
                            value=json.dumps(mapped_data),
                            callback=self.delivery_report
                        )
                        self.producer.poll(0)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Stopping data mapper...")
        finally:
            self.consumer.close()
            self.producer.flush()
