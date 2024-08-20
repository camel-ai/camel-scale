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


def serialize_message(message_id, content):
    r"""Serialize the message data into JSON format.

    Args:
        message_id (int): The unique identifier for the message.
        content (str): The content of the message.

    Returns:
        str: A JSON string representing the message.
    """
    message = {
        "message_id": message_id,
        "content": content,
        "timestamp": datetime.utcnow().isoformat(),  # ISO 8601 format
    }
    return json.dumps(message)


def deserialize_message(json_string):
    r"""Deserialize a JSON string back into a Python dictionary.

    Args:
        json_string (str): The JSON string to be deserialized.

    Returns:
        dict: A dictionary representing the deserialized message.
    """
    data = json.loads(json_string)
    data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return data
