from typing import Any, Dict
import pandas as pd
import itertools
import time
import schedule
from confluent_kafka import Producer
import json
import base64
import logging
from config import KAFKA_CONFIG, TOPICS, SAMPLE_DATA

logger = logging.getLogger('gendata')

class DataGenerator:
    def __init__(self, kafka_config: Dict[str, Any], topic: str):
        self.producer = Producer(kafka_config)
        self.topic = topic
        self.combinations = itertools.product(
            SAMPLE_DATA['business_unit'],
            SAMPLE_DATA['system'],
            SAMPLE_DATA['facility'],
            SAMPLE_DATA['attributes'],
            SAMPLE_DATA['instrument_type'],
            SAMPLE_DATA['serial_number']
        )
        
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    @staticmethod
    def encode_to_base64(data: Dict) -> str:
        json_data = json.dumps(data)
        encoded_bytes = base64.b64encode(json_data.encode('utf-8'))
        return encoded_bytes.decode('utf-8')

    def send_to_kafka(self):
        try:
            combination = next(self.combinations)
            data = {
                'Business Unit': combination[0],
                'System': combination[1],
                'Facility': combination[2],
                'Additional Attribute': combination[3],
                'Instrument Type': combination[4],
                'Serial Number': combination[5]
            }
            encoded_data = self.encode_to_base64(data)
            self.producer.produce(self.topic, value=encoded_data, 
                                callback=self.delivery_report)
            self.producer.poll(0)
        except StopIteration:
            logger.info("No more data to send.")
            schedule.clear()

    def run(self):
        logger.info("Starting data generator...")
        schedule.every(0.05).seconds.do(self.send_to_kafka)
        while schedule.get_jobs():
            schedule.run_pending()
            time.sleep(0.1)
        self.producer.flush()
