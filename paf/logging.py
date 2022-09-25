# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB


import enum
import errno
import logging
import logging.handlers
import os


class LogCategory(enum.Enum):
    SECURITY = 'security'
    PROTOCOL = 'protocol'
    CORE = 'core'
    INTERNAL = 'internal'


LOG_DEV = '/dev/log'

logger = logging.getLogger()


def configure(console=True, syslog=True, syslog_ident=None,
              syslog_facility=logging.handlers.SysLogHandler.LOG_DAEMON,
              filter_level=logging.INFO):
    logging.basicConfig(level=filter_level)
    if not console:
        logger.handlers = []
    if syslog:
        # In containers, the log device file may not exist. This is a reason
        # to fail early.
        if not os.path.exists(LOG_DEV):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                    LOG_DEV)
        syslog = logging.handlers.SysLogHandler(address=LOG_DEV,
                                                facility=syslog_facility)
        if syslog_ident is not None:
            syslog.ident = syslog_ident
        add_handler(syslog)


def add_handler(handler):
    logger.handlers.append(handler)


def _extra(category):
    # 'msg_id' as per RFC 5424
    return {'msg_id': category.value}


def debug(msg, category):
    logger.debug(msg, extra=_extra(category))


def info(msg, category):
    logger.info(msg, extra=_extra(category))


def warning(msg, category):
    logger.warning(msg, extra=_extra(category))


def exception(msg):
    logger.exception(msg, extra=_extra(LogCategory.INTERNAL))
