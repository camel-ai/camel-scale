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

from camel_scale.kafka_server.producer import Producer


@pytest.fixture
def mock_confluent_producer():
    with patch("camel_scale.kafka_server.producer.ConfluentProducer") as mock:
        yield mock


@pytest.fixture
def producer(mock_confluent_producer):
    return Producer()


def test_producer_init(producer):
    r"""Test that Producer initializes with correct default values."""
    assert producer.topic == "default_topic"
    assert producer.producer is not None


def test_delivery_report_success(producer):
    r"""Test delivery_report method with successful delivery."""
    mock_msg = Mock()
    mock_msg.topic.return_value = "test_topic"
    mock_msg.partition.return_value = 0

    producer.delivery_report(None, mock_msg)
    # This should not raise an exception


def test_delivery_report_failure(producer):
    r"""Test delivery_report method with failed delivery."""
    mock_error = Mock()
    mock_error.__str__ = Mock(return_value="Test error")

    with pytest.raises(ValueError, match="Message delivery failed: Test error"):
        producer.delivery_report(mock_error, None)


def test_produce_success(producer, mock_confluent_producer):
    r"""Test successful message production."""
    test_value = {"key": "value"}
    test_key = "test_key"
    test_topic = "test_topic"

    producer.produce(test_value, key=test_key, topic=test_topic)

    mock_confluent_producer.return_value.produce.assert_called_once_with(
        test_topic,
        key=test_key,
        value=json.dumps(test_value),
        callback=producer.delivery_report,
    )
    mock_confluent_producer.return_value.poll.assert_called_once_with(0)


def test_produce_default_topic(producer, mock_confluent_producer):
    r"""Test message production with default topic."""
    test_value = {"key": "value"}

    producer.produce(test_value)

    mock_confluent_producer.return_value.produce.assert_called_once_with(
        "default_topic",
        key=None,
        value=json.dumps(test_value),
        callback=producer.delivery_report,
    )


def test_produce_non_serializable(producer):
    r"""Test producing a non-JSON-serializable message."""
    test_value = set([1, 2, 3])  # sets are not JSON serializable

    with pytest.raises(ValueError, match="Value must be JSON serializable"):
        producer.produce(test_value)


def test_flush(producer, mock_confluent_producer):
    r"""Test flush method."""
    producer.flush()
    mock_confluent_producer.return_value.flush.assert_called_once()


def test_close(producer, mock_confluent_producer):
    r"""Test close method."""
    producer.close()
    mock_confluent_producer.return_value.flush.assert_called_once()
