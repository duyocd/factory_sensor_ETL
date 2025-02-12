import logging
from typing import Dict, Any

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'etl-group',
}

# Redis configuration
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': True,
    'expire_time': 60*60*24  # 1 day in seconds
}

# Topic configuration
TOPICS = {
    'raw_data': 'ooo', # your first topic
    'mapped_data': 'sss' # your second topic 
}

# Sample data configuration
SAMPLE_DATA = {
    'business_unit': ['AA10', 'AA11', 'AA20'],
    'system': ['BM', 'FW', 'CW', 'CH'],
    'facility': ['GEN', 'BLR', 'TBN', 'BOP', 'ESP'],
    'attributes': ['WTR', 'OIL', 'STE', 'GAS', 'ASH'],
    'instrument_type': ['PT', 'TT', 'FT', 'LT', 'FR'],
    'serial_number': [1230, 1231]
}