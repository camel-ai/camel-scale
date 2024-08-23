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
from typing import Optional

from confluent_kafka import Consumer as ConfluentConsumer  # type: ignore[import]
from confluent_kafka import KafkaError  # type: ignore[import]


class Consumer:
    r"""Kafka consumer model using confluent_kafka.

    Args:
        bootstrap_servers (str): Kafka broker(s). (default: :obj:`"localhost:9092"`)
        group_id (str): Consumer group ID. (default: :obj:`"my-consumer-group"`)
        auto_offset_reset (str): Where to start reading messages. (default: :obj:`"earliest"`)
        enable_auto_commit (bool): Whether to auto-commit offsets. (default: :obj:`False`)
        **kwargs: Additional configuration parameters for confluent_kafka.Consumer.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "my-consumer-group",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        **kwargs,
    ):
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
        }
        config.update(kwargs)
        self.consumer = ConfluentConsumer(config)

    def subscribe(self, topics: list[str]):
        r"""Subscribe to the given list of topics.

        Args:
            topics (list[str]): List of topics to subscribe to.
        """
        self.consumer.subscribe(topics)

    def consume(self, timeout: Optional[float] = 1.0) -> Optional[dict]:
        r"""Consume messages from subscribed topics.

        Args:
            timeout (float, optional): The maximum time to block waiting for a message. (default: :obj:`1.0`)

        Returns:
            dict: Deserialized message value, or None if no message was available.
        """
        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                raise ValueError(
                    f"Reached end of partition: {msg.topic()} [{msg.partition()}]"
                )
            else:
                raise KafkaError(msg.error())

        try:
            value = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError(f"Failed to decode message value as JSON: {msg.value()}")

        return value

    def commit(self):
        r"""Commit current offsets for all assigned partitions."""
        self.consumer.commit()

    def close(self):
        r"""Close the consumer"""
        self.consumer.close()

    def __enter__(self):
        r"""Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        r"""Exit the runtime context related to this object."""
        self.close()
