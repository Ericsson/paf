# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB


import logging
import logging.handlers
import enum


class LogCategory(enum.Enum):
    SECURITY = 'security'
    PROTOCOL = 'protocol'
    CORE = 'core'
    INTERNAL = 'internal'


logger = logging.getLogger()


def configure(console=True, syslog=True, syslog_ident=None,
              syslog_facility=logging.handlers.SysLogHandler.LOG_DAEMON,
              filter_level=logging.INFO):
    logging.basicConfig(level=filter_level)
    if not console:
        logger.handlers = []
    if syslog:
        syslog = logging.handlers.SysLogHandler(address='/dev/log',
                                                facility=syslog_facility)
        if syslog_ident is not None:
            syslog.ident = syslog_ident
        add_handler(syslog)


def add_handler(handler):
    logger.handlers.append(handler)


def debug(msg, category):
    logger.debug(msg, {'msg_id': category})


def info(msg, category):
    logger.info(msg, {'msg_id': category})


def warning(msg, category):
    logger.warning(msg, {'msg_id': category})


def exception(msg):
    logger.exception({'msg_id': LogCategory.INTERNAL})
