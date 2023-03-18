# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import logging

import paf.sd as sd

DEFAULT_LOG_CONSOLE = False
DEFAULT_LOG_FILE = None
DEFAULT_LOG_FILE_BACKUP = 0
DEFAULT_LOG_FILE_MAX_SIZE = 1000000
DEFAULT_LOG_SYSLOG = True
DEFAULT_LOG_FACILITY = logging.handlers.SysLogHandler.LOG_DAEMON
DEFAULT_LOG_FILTER = logging.INFO
DEFAULT_MAX_IDLE_TIME = 30


class Error(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class MissingFieldError(Error):
    def __init__(self, dict_path, dict_key):
        Error.__init__(self, "required parameter '%s' is missing" %
                       path(dict_path, dict_key))


class FormatError(Error):
    def __init__(self, field_name, illegal_value, valid_values=None):
        message = "invalid %s: '%s'" % (field_name, illegal_value)
        if valid_values is not None:
            message += " (valid values: %s)" % " ".join(valid_values)
        Error.__init__(self, message)


LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

FACILITY_NAMES = logging.handlers.SysLogHandler.facility_names


def path(*args):
    return ".".join([arg for arg in args if len(arg) > 0])


class LogConf:
    def __init__(self):
        self.console = DEFAULT_LOG_CONSOLE
        self.log_file = DEFAULT_LOG_FILE
        self.log_file_backup = DEFAULT_LOG_FILE_BACKUP
        self.log_file_max_size = DEFAULT_LOG_FILE_MAX_SIZE
        self.syslog = DEFAULT_LOG_SYSLOG
        self.facility = DEFAULT_LOG_FACILITY
        self.filter = DEFAULT_LOG_FILTER

    def set_console(self, console):
        self.console = console

    def set_log_file(self, log_file):
        self.log_file = log_file

    def set_log_file_backup(self, log_file_backup):
        self.log_file_backup = log_file_backup

    def set_log_file_max_size(self, log_file_max_size):
        self.log_file_max_size = log_file_max_size

    def set_syslog(self, syslog):
        self.syslog = syslog

    def set_filter(self, level_name):
        try:
            self.filter = LOG_LEVELS[level_name]
        except KeyError:
            raise FormatError("filter level", level_name, LOG_LEVELS.keys())

    def set_facility(self, facility):
        try:
            self.facility = FACILITY_NAMES[facility]
        except KeyError:
            raise FormatError("log facility", facility, FACILITY_NAMES.keys())

    def filter_name(self):
        for name, code in LOG_LEVELS.items():
            if code == self.filter:
                return name

    def facility_name(self):
        for name, code in FACILITY_NAMES.items():
            if code == self.facility:
                return name

    def __str__(self):
        if self.log_file is None:
            log_file_s = "-"
        else:
            log_file_s = "%s, log_file_backup: %d" % \
                (self.log_file, self.log_file_backup)
            if self.log_file_backup > 0:
                log_file_s += ", log_file_max_size: %d" % \
                    self.log_file_max_size

        return "{ console: %s, log_file: %s, syslog: %s, filter: %s, " \
            "facility: %s }" % (str(self.console).lower(), log_file_s,
                                str(self.syslog).lower(), self.filter_name(),
                                self.facility_name())


class ResourcesClassConf:
    def __init__(self, max_clients=None):
        self.resources = sd.resources(clients=max_clients)

    def set_limit(self, name, value):
        if not isinstance(value, int) or value < 0:
            raise FormatError("resource limit", value)
        if name == "clients":
            self.resources[sd.ResourceType.CLIENT] = value
        elif name == "services":
            self.resources[sd.ResourceType.SERVICE] = value
        elif name == "subscriptions":
            self.resources[sd.ResourceType.SUBSCRIPTION] = value
        else:
            raise FormatError("resource type", name)

    def clear_limit(self, name):
        self.set_limit(self, name, None)

    def get_client_limit(self):
        return self.resources[sd.ResourceType.CLIENT]

    def has_limits(self):
        for value in self.resources.values():
            if value is not None:
                return True
        return False

    def __str__(self):
        limits = []
        for resource_type in sd.ResourceType:
            value = self.resources[resource_type]
            if value is not None:
                limits.append("%s: %d" %
                              (resource_type.name.lower(), value))
        return "{ %s }" % ", ".join(limits)


class ResourcesConf:
    def __init__(self):
        self.user = \
            ResourcesClassConf()
        self.total = \
            ResourcesClassConf()

    def has_limits(self):
        return self.total.has_limits() or self.user.has_limits()

    def __str__(self):
        sections = []
        for name in ("user", "total"):
            class_resources = getattr(self, name)
            if class_resources.has_limits():
                sections.append("%s: %s" % (name, class_resources))
        return "{ %s }" % ", ".join(sections)


class SocketConf:
    def __init__(self, addr, tls_attrs):
        self.addr = addr
        self.tls_attrs = tls_attrs

    def __str__(self):
        s = {"addr": self.addr}
        if len(self.tls_attrs) > 0:
            s["tls"] = self.tls_attrs
        return str(s)

    def __repr__(self):
        return str(self)


class DomainConf:
    def __init__(self, name, max_idle_time):
        self.name = name
        self.max_idle_time = max_idle_time
        self.sockets = []

    def add_socket(self, addr, tls_attrs={}):
        self.sockets.append(SocketConf(addr, tls_attrs))

    def __str__(self):
        s = {}
        if self.name is not None:
            s["name"] = self.name
        s["sockets"] = self.sockets

        s["max_idle_time"] = self.max_idle_time

        return str(s)


class Conf:
    def __init__(self):
        self.log = LogConf()
        self.domains = []
        self.resources = ResourcesConf()

    def add_domain(self, name=None, max_idle_time=DEFAULT_MAX_IDLE_TIME):
        domain_conf = DomainConf(name, max_idle_time)
        self.domains.append(domain_conf)
        return domain_conf

    def set_domains(self, domains):
        self.domains = []
        for domain in domains:
            domain_conf = self.add_domain()
            for socket in domain:
                domain_conf.add_socket(socket)

    def __str__(self):
        sections = []
        sections.append("domains: [%s]" %
                        ", ".join([str(domain) for domain in self.domains]))

        sections.append("log: %s" % self.log)

        if self.resources.has_limits():
            sections.append("resources: %s" % self.resources)

        return ", ".join(sections)


def assure_type(value, value_type, path):
    if not isinstance(value, value_type):
        raise Error("parameter '%s' has invalid value type: '%s' (expected "
                    "'%s')" % (path, type(value), value_type))


def dict_lookup(dict_value, dict_key, value_type, dict_path, required=False,
                default=None):
    value = dict_value.get(dict_key)
    if value is None:
        if required:
            assert default is None
            raise MissingFieldError(dict_path, dict_key)
        return default

    assure_type(value, value_type, path(dict_path, dict_key))

    return value


def dict_copy(dict_value, dict_key, value_type, dict_path, set_value,
              required=False):
    value = dict_lookup(dict_value, dict_key, value_type, dict_path,
                        required=required)
    if value is not None:
        set_value(value)


def log_populate(conf, log, path):
    if log is None:
        return
    dict_copy(log, "console", bool, path, conf.log.set_console)
    dict_copy(log, "log_file", str, path, conf.log.set_log_file)
    dict_copy(log, "log_file_backup", int, path, conf.log.set_log_file_backup)
    dict_copy(log, "log_file_max_size", int, path,
              conf.log.set_log_file_max_size)
    dict_copy(log, "syslog", bool, path, conf.log.set_syslog)
    dict_copy(log, "facility", str, path, conf.log.set_facility)
    dict_copy(log, "filter", str, path, conf.log.set_filter)


def assure_tls_addr(field_name, addr):
    proto = addr.split(":")[0]
    if proto != "tls" and proto != "utls":
        raise FormatError(field_name, proto, ["tls", "utls"])


def domains_populate(conf, domains, path):
    if domains is None:
        return
    for domain_num, domain in enumerate(domains):
        domain_path = "%s[%d]" % (path, domain_num)
        assure_type(domain, dict, domain_path)

        name = dict_lookup(domain, "name", str, domain_path, required=False)

        max_idle_time = dict_lookup(domain, "max_idle_time", int,
                                    domain_path, default=DEFAULT_MAX_IDLE_TIME,
                                    required=False)

        # 'addrs' is an alternative name, supported for backward
        # compatibility reasons
        sockets = dict_lookup(domain, "addrs", list, domain_path,
                              required=False)

        if sockets is None:
            sockets = dict_lookup(domain, "sockets", list, domain_path,
                                  required=True)

        domain_conf = conf.add_domain(name, max_idle_time)

        for socket_num, socket in enumerate(sockets):
            socket_path = "%s.sockets[%d]" % (domain_path, socket_num)
            if isinstance(socket, str):
                domain_conf.add_socket(socket, {})
            elif isinstance(socket, dict):
                if "addr" not in socket:
                    raise MissingFieldError(socket_path, "addr")

                addr = socket["addr"]

                if "tls" in socket:
                    assure_tls_addr("%s.addr" % socket_path, addr)
                    tls_attrs = socket["tls"]
                else:
                    tls_attrs = {}

                domain_conf.add_socket(addr, tls_attrs)
            else:
                raise FormatError("domain address", socket)


def resource_class_copy(resource_class, set_limit):
    if resource_class is None:
        return
    for resource_name, resource_value in resource_class.items():
        set_limit(resource_name, resource_value)


def resources_populate(conf, resources, path):
    if resources is None:
        return

    user = dict_lookup(resources, "user", dict, path)
    resource_class_copy(user, conf.resources.user.set_limit)

    total = dict_lookup(resources, "total", dict, path)
    resource_class_copy(total, conf.resources.total.set_limit)


def populate(conf, source):
    assure_type(source, dict, "")
    log_populate(conf, dict_lookup(source, "log", dict, ""), "log")
    domains_populate(conf, dict_lookup(source, "domains", list, ""), "domains")
    resources_populate(conf, dict_lookup(source, "resources", dict, ""),
                       "resources")


def default():
    return Conf()


def load(conf_file):
    conf = Conf()
    import yaml
    source = yaml.safe_load(open(conf_file).read())
    populate(conf, source)
    return conf
