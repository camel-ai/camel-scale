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

import pytest
from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import]

# Kafka configuration
KAFKA_BROKER_URL = "localhost:9092"  # default Kafka broker URL
TEST_TOPIC = "test-topic"


@pytest.fixture(scope="module")
def producer():
    r"""Fixture to create a Kafka producer"""
    _producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])
    yield _producer
    _producer.close()


@pytest.fixture(scope="module")
def consumer():
    r"""Fixture to create a Kafka consumer"""
    _consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    yield _consumer
    _consumer.close()


def test_send_receive_message(producer, consumer):
    r"""Test to send and then receive a message from Kafka"""
    message = b"Hello World"  # message to send
    producer.send(TEST_TOPIC, message)
    producer.flush()  # make sure the message has been sent

    # Consume the message
    for msg in consumer:
        assert msg.value == message
        break
    else:
        pytest.fail("Message was not received by the consumer")
