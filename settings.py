
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
URLS_FILE = os.getenv('URLS_FILE', 'urls.yaml')
DATABASE_URL = os.getenv('DATABASE_URL')
DATABASE_TABLE = os.getenv('DATABASE_TABLE', 'checks')
WARM_UP_PARTITIONS = 3
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'topic1')
KAFKA_CAFILE = os.getenv('KAFKA_CAFILE', 'ca.pem')
KAFKA_CERTFILE = os.getenv('KAFKA_CERTFILE', 'service.cert')
KAFKA_KEYFILE = os.getenv('KAFKA_KEYFILE', 'service.key')
