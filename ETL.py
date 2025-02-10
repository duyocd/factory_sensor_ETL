import threading
from config import KAFKA_CONFIG, REDIS_CONFIG, TOPICS
import logging

logger = logging.getLogger('etl')

def run_etl():
    from gendata import DataGenerator
    from mappingdata import DataMapper
    from redis_consumer import RedisConsumer

    try:
        # Initialize components
        generator = DataGenerator(KAFKA_CONFIG, TOPICS['raw_data'])
        mapper = DataMapper(KAFKA_CONFIG, TOPICS['raw_data'], TOPICS['mapped_data'])
        redis_consumer = RedisConsumer(KAFKA_CONFIG, REDIS_CONFIG, TOPICS['mapped_data'])

        # Create threads
        gen_thread = threading.Thread(target=generator.run)
        map_thread = threading.Thread(target=mapper.run)
        redis_thread = threading.Thread(target=redis_consumer.run)

        # Start threads
        for thread in (gen_thread, map_thread, redis_thread):
            thread.start()

        # Wait for threads to complete
        for thread in (gen_thread, map_thread, redis_thread):
            thread.join()

    except Exception as e:
        logger.error(f"Error in ETL process: {e}")
        raise

if __name__ == "__main__":
    run_etl()