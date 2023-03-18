# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2023 Ericsson AB

from enum import Enum, auto


class ServerFeature(Enum):
    CONFIG_FILE = auto()
    RESOURCE_LIMITS = auto()
    MULTI_SOCKET_DOMAIN = auto()
    HOOK = auto()
    ACCESS_CONTROL = auto()
    PROTO_V3 = auto()
