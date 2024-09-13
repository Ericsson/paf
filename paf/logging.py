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


logger = logging.getLogger()


def configure(console=True, log_file=None, log_file_backup=0,
              log_file_max_size=0, syslog=True, syslog_socket='/dev/log',
              syslog_ident=None,
              syslog_facility=logging.handlers.SysLogHandler.LOG_DAEMON,
              filter_level=logging.INFO):
    logging.basicConfig(level=filter_level)
    if not console:
        logger.handlers = []
    if log_file is not None:
        file_handler = \
            logging.handlers.RotatingFileHandler(log_file,
                                                 backupCount=log_file_backup,
                                                 maxBytes=log_file_max_size)
        add_handler(file_handler)
    if syslog:
        # In containers, the log device file may not exist. This is a reason
        # to fail early.
        if not os.path.exists(syslog_socket):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                    syslog_socket)
        syslog = logging.handlers.SysLogHandler(address=syslog_socket,
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
