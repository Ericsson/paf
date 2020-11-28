# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB


import logging
import enum


class LogCategory(enum.Enum):
    SECURITY = 'security'
    PROTOCOL = 'protocol'
    CORE = 'core'
    INTERNAL = 'internal'


logger = logging.getLogger()


def _log(log_fun, msg, category):
    # 'msg_id' as per RFC 5424
    extra = {'msg_id': category.name}
    log_fun(msg, extra=extra)


def debug(msg, category):
    logger.debug(msg, {'msg_id': category})


def info(msg, category):
    logger.info(msg, {'msg_id': category})


def warning(msg, category):
    logger.warning(msg, {'msg_id': category})


def exception(msg):
    logger.exception({'msg_id': LogCategory.INTERNAL})
