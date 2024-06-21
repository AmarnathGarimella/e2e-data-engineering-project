from kafka import KafkaConsumer

# Define the configuration for the consumer
bootstrap_servers = 'localhost:9092'  # Kafka broker address
topic = 'create_user'                      # Kafka topic name
group_id = 'mygroup'                   # Consumer group ID

# Create the Consumer instance
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    auto_offset_reset='earliest'  # Start reading at the earliest offset
)

# Function to process messages
def process_message(msg):
    print(f"Received message: {msg.value.decode('utf-8')}")

# Consume messages in a loop
try:
    for message in consumer:
        process_message(message)
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()