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

import json
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaError  # type: ignore[import]

from camel_scale.kafka_server.consumer import Consumer


@pytest.fixture
def mock_confluent_consumer():
    with patch("camel_scale.kafka_server.consumer.ConfluentConsumer") as mock:
        yield mock


@pytest.fixture
def consumer(mock_confluent_consumer):
    return Consumer()


def test_consumer_init(consumer):
    r"""Test that Consumer initializes with correct default values."""
    assert consumer.consumer is not None


def test_subscribe(consumer, mock_confluent_consumer):
    r"""Test subscribe method."""
    topics = ["topic1", "topic2"]
    consumer.subscribe(topics)
    mock_confluent_consumer.return_value.subscribe.assert_called_once_with(topics)


def test_consume_no_message(consumer, mock_confluent_consumer):
    r"""Test consume method when no message is available."""
    mock_confluent_consumer.return_value.poll.return_value = None
    result = consumer.consume()
    assert result is None


def test_consume_with_message(consumer, mock_confluent_consumer):
    r"""Test consume method with a valid message."""
    mock_message = Mock()
    mock_message.error.return_value = None
    mock_message.value.return_value = json.dumps({"key": "value"}).encode("utf-8")
    mock_confluent_consumer.return_value.poll.return_value = mock_message

    result = consumer.consume()
    assert result == {"key": "value"}


def test_consume_with_error(consumer, mock_confluent_consumer):
    r"""Test consume method when an error occurs."""
    mock_message = Mock()
    mock_error = Mock()
    mock_error.code.return_value = KafkaError._PARTITION_EOF
    mock_message.error.return_value = mock_error
    mock_message.topic.return_value = "test_topic"
    mock_message.partition.return_value = 0
    mock_confluent_consumer.return_value.poll.return_value = mock_message

    result = consumer.consume()
    assert result is None


def test_consume_with_kafka_error(consumer, mock_confluent_consumer):
    r"""Test consume method when a KafkaError occurs."""
    mock_message = Mock()
    mock_error = KafkaError("Test Kafka Error")
    mock_message.error.return_value = mock_error
    mock_confluent_consumer.return_value.poll.return_value = mock_message

    with pytest.raises(KafkaError, match="Test Kafka Error"):
        consumer.consume()


def test_consume_with_json_decode_error(consumer, mock_confluent_consumer):
    r"""Test consume method when JSON decoding fails."""
    mock_message = Mock()
    mock_message.error.return_value = None
    mock_message.value.return_value = "Invalid JSON".encode("utf-8")
    mock_confluent_consumer.return_value.poll.return_value = mock_message

    with pytest.raises(ValueError, match="Failed to decode message value as JSON"):
        consumer.consume()


def test_commit(consumer, mock_confluent_consumer):
    r"""Test commit method."""
    consumer.commit()
    mock_confluent_consumer.return_value.commit.assert_called_once()


def test_close(consumer, mock_confluent_consumer):
    r"""Test close method."""
    consumer.close()
    mock_confluent_consumer.return_value.close.assert_called_once()


def test_context_manager(mock_confluent_consumer):
    r"""Test Consumer as a context manager."""
    with Consumer() as consumer:
        assert consumer.consumer is not None
    mock_confluent_consumer.return_value.close.assert_called_once()
