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
from datetime import datetime

from camel_scale.utils.commons import deserialize_message, serialize_message


def test_serialize_message():
    message_id = 123
    content = "Hello, world!"
    result = serialize_message(message_id, content)
    expected_output = json.loads(result)

    assert expected_output["message_id"] == message_id
    assert expected_output["content"] == content
    assert "timestamp" in expected_output


def test_deserialize_message():
    json_string = '{"message_id": 123, "content": "Hello, world!", "timestamp": "2021-10-10T14:20:00.000000"}'
    result = deserialize_message(json_string)

    assert result["message_id"] == 123
    assert result["content"] == "Hello, world!"
    assert isinstance(result["timestamp"], datetime)
