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

import unittest
from unittest.mock import MagicMock, patch

from camel_scale.kafka_server.producer.producer import kafka_producer


class TestKafkaProducer(unittest.TestCase):
    @patch("producer.Producer")
    def test_send_message(self, mocked_producer):
        # Create a mock producer instance
        mock_producer_instance = MagicMock()
        mocked_producer.return_value = mock_producer_instance

        # Define the test data
        brokers = "localhost:9092"
        topic = "test-topic"
        test_message = "Hello, Kafka!"

        # Initialize the send_message function
        send_message_fn = kafka_producer(brokers, topic)

        send_message_fn(test_message)

        mock_producer_instance.produce.assert_called_once()

        mock_producer_instance.flush.assert_called_once()

        # Check the arguments passed to the produce function
        args, kwargs = mock_producer_instance.produce.call_args
        self.assertEqual(args[0], topic)
        self.assertEqual(args[1], test_message.encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
