# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
from camel_scale.kafka_server.consumer import Consumer
from camel_scale.kafka_server.producer import Producer


def run_kafka_example():
    # Define the topic
    topic = "test_topic"

    # Create a producer instance
    producer = Producer(bootstrap_servers="localhost:9092", topic=topic)
    print("Producer created.")

    # Produce messages
    messages_to_send = [{"id": i, "message": f"Hello Kafka {i}"} for i in range(5)]
    for message in messages_to_send:
        producer.produce(value=message, topic=topic)
        print(f"Produced: {message}")
        # producer.poll(0)  # regularly poll the delivery report callbacks

    # Wait for all messages to be sent
    producer.flush()
    print("All messages produced.")
    producer.close()
    print("Producer closed.")

    # Create a consumer instance
    consumer = Consumer(bootstrap_servers="localhost:9092", group_id="example_group")
    consumer.subscribe([topic])
    print("Consumer subscribed to topic.")

    # Start consuming
    try:
        print("Starting to consume messages...")
        while True:
            message = consumer.consume(timeout=1.5)  # Increase timeout if needed
            if message is not None:
                print(f"Consumed: {message}")
            else:
                print("No more messages to consume.")
                break
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run_kafka_example()
