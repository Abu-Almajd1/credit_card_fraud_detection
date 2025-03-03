from confluent_kafka import Producer
import csv
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
KAFKA_BROKER = 'host.docker.internal:9092'
KAFKA_TOPIC = 'fraud-detection'
CSV_FILE_PATH = r'C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\data\onlinefraud.csv'

config = {
    'bootstrap.servers': KAFKA_BROKER,
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 1048576,
    'batch.num.messages': 10000,
    'linger.ms': 1000,
    'message.max.bytes': 1048576,
    'retries': 3,
    'retry.backoff.ms': 500,
    'compression.codec': 'gzip',
}

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_data_to_kafka(csv_file_path, batch_size=1000):
    """
    Reads data from the CSV file and sends it to the specified Kafka topic.
    
    Args:
        csv_file_path (str): Path to the CSV file to read data from
        batch_size (int): Number of messages to process before forcing a flush
    """
    producer = Producer(config)
    messages_sent = 0
    
    try:
        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            
            for row in csv_reader:
                message = ','.join(row)
                
                try:
                    producer.produce(
                        KAFKA_TOPIC,
                        value=message.encode('utf-8'),
                        callback=delivery_report
                    )
                    messages_sent += 1
                    
                    # Flush every batch_size messages
                    if messages_sent % batch_size == 0:
                        producer.flush()
                        logging.info(f"Processed {messages_sent} messages")
                        
                except BufferError:
                    logging.warning("Local queue full, waiting for space...")
                    producer.flush()
                    time.sleep(0.1)
                    
                producer.poll(0)
                
        # Final flush to ensure all messages are sent
        producer.flush()
        logging.info(f"Successfully sent {messages_sent} messages to topic '{KAFKA_TOPIC}'")
        
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
    finally:
        # Allow time for any remaining messages to be delivered
        producer.flush(timeout=5)
        # No need for producer.close() as it's not available in confluent-kafka

if __name__ == "__main__":
    send_data_to_kafka(CSV_FILE_PATH)