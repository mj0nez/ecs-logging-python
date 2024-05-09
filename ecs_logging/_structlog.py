# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import functools

import time
import datetime
from ._meta import ECS_VERSION
from ._utils import json_dump_bytes, normalize_dict, TYPE_CHECKING, collections_abc, get_stdlib_json_serializer

if TYPE_CHECKING:
    from typing import Any, Dict, Union


class StructlogFormatter:
    """ECS formatter for the ``structlog`` module"""

    __slots__ = ("format_as_binary",)

    def __init__(self, format_as_binary:bool=False) -> None:
        self.format_as_binary = format_as_binary


    def __call__(self, _, name, event_dict):
        # type: (Any, str, Dict[str, Any]) -> Union[str,bytes]

        # Handle event -> message now so that stuff like `event.dataset` doesn't
        # cause problems down the line
        event_dict["message"] = str(event_dict.pop("event"))
        event_dict = normalize_dict(event_dict)
        event_dict.setdefault("log", {}).setdefault("level", name.lower())
        event_dict = self.format_to_ecs(event_dict)

        if self.format_as_binary:
            return json_dump_bytes(event_dict)
        else:
            return json_dump_bytes(event_dict).decode()
        

    def format_to_ecs(self, event_dict):
        # type: (Dict[str, Any]) -> Dict[str, Any]
        if "@timestamp" not in event_dict:
            event_dict["@timestamp"] = (
                datetime.datetime.fromtimestamp(
                    time.time(), tz=datetime.timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z"
            )

        if "exception" in event_dict:
            stack_trace = event_dict.pop("exception")
            if "error" in event_dict:
                event_dict["error"]["stack_trace"] = stack_trace
            else:
                event_dict["error"] = {"stack_trace": stack_trace}

        event_dict.setdefault("ecs", {}).setdefault("version", ECS_VERSION)
        return event_dict
