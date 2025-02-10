from typing import Any, Dict
from confluent_kafka import Consumer, KafkaError
import redis
import json
import logging
from config import KAFKA_CONFIG, REDIS_CONFIG, TOPICS

logger = logging.getLogger('redis_consumer')

class RedisConsumer:
    def __init__(self, kafka_config: Dict[str, Any], redis_config: Dict[str, Any], topic: str):
        self.redis_config = redis_config
        self.redis_client = redis.Redis(**{k: v for k, v in redis_config.items() 
                                         if k != 'expire_time'})
        
        consumer_config = kafka_config.copy()
        consumer_config['group.id'] = 'redis-group'
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([topic])
        self.expire_time = redis_config.get('expire_time', 86400)  # Default 1 day

    def create_key(self, data: Dict[str, Any]) -> str:
        """Create Redis key from data fields"""
        return f"sensor:{data['Business Unit']}:{data['System']}:" \
               f"{data['Facility']}:{data['Additional Attribute']}:" \
               f"{data['Instrument Type']}:{data['Serial Number']}"

    def store_in_redis(self, data: Dict[str, Any]):
        try:
            key = self.create_key(data)
            self.redis_client.hmset(key, data)
            self.redis_client.expire(key, self.expire_time)
        except Exception as e:
            logger.error(f"Error storing data in Redis: {e}")
            raise

    def run(self):
        logger.info("Starting Redis consumer...")
        try:
            self.redis_client.ping()
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
                    data = json.loads(msg.value().decode('utf-8'))
                    self.store_in_redis(data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
        except KeyboardInterrupt:
            logger.info("Stopping Redis consumer...")
        finally:
            self.consumer.close()
            self.redis_client.close()