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

import time

import pytest

from camel_scale.kafka_server.consumer import Consumer
from camel_scale.kafka_server.producer import Producer


@pytest.fixture(scope="module")
def kafka_producer():
    producer = Producer(bootstrap_servers="localhost:9092", topic="test_topic")
    yield producer
    producer.close()


@pytest.fixture(scope="module")
def kafka_consumer():
    consumer = Consumer(
        bootstrap_servers="localhost:9092",
        group_id="test_group",
        auto_offset_reset="earliest",
    )
    consumer.subscribe(["test_topic"])
    yield consumer
    consumer.close()


def test_producer_consumer_integration(kafka_producer, kafka_consumer):
    # Produce a message
    test_message = {"key": "value"}  # suppose this is a message sent
    kafka_producer.produce(test_message)
    kafka_producer.flush()  # ensure the message is sent

    # Consume the message
    start_time = time.time()
    max_wait_time = 10  # seconds
    received_message = None

    while time.time() - start_time < max_wait_time:
        message = kafka_consumer.consume(timeout=1.0)
        if message:
            received_message = message
            break

    # Assert that the message was received and matches the sent message
    assert received_message is not None, "No message received"
    assert (
        received_message == test_message
    ), "Received message does not match sent message"
