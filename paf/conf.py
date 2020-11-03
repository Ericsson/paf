# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import logging

import paf.sd as sd

DEFAULT_LOG_CONSOLE = False
DEFAULT_LOG_SYSLOG = True
DEFAULT_LOG_FACILITY = logging.handlers.SysLogHandler.LOG_DAEMON
DEFAULT_LOG_FILTER = logging.INFO


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
        self.syslog = DEFAULT_LOG_SYSLOG
        self.facility = DEFAULT_LOG_FACILITY
        self.filter = DEFAULT_LOG_FILTER

    def set_console(self, console):
        self.console = console

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
        return "{ console: %s, syslog: %s, filter: %s, facility: %s }" % \
            (str(self.console).lower(), str(self.syslog).lower(),
             self.filter_name(), self.facility_name())


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


class DomainConf:
    def __init__(self, addrs):
        self.addrs = []
        for addr in addrs:
            self.add_addr(addr)

    def add_addr(self, addr):
        if not isinstance(addr, str):
            raise FormatError("domain address", addr)
        self.addrs.append(addr)

    def __str__(self):
        return "%s" % self.addrs


class Conf:
    def __init__(self):
        self.log = LogConf()
        self.domains = []
        self.resources = ResourcesConf()

    def add_domain(self, domain):
        self.domains.append(DomainConf(domain))

    def set_domains(self, domains):
        self.domains = []
        for domain in domains:
            self.add_domain(domain)

    def __str__(self):
        sections = []
        sections.append("domains: [ %s ]" %
                        ", ".join(["%s" % domain for domain in self.domains]))

        sections.append("log: %s" % self.log)

        if self.resources.has_limits():
            sections.append("resources: %s" % self.resources)

        return ", ".join(sections)


def assure_type(value, value_type, path):
    if not isinstance(value, value_type):
        raise Error("parameter '%s' has invalid value type: '%s' (expected "
                    "'%s')" % (path, type(value), value_type))


def dict_lookup(dict_value, dict_key, value_type, dict_path, required=False):
    value = dict_value.get(dict_key)
    if value is None:
        if required:
            raise MissingFieldError(dict_path, dict_key)
        return None

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
    dict_copy(log, "syslog", bool, path, conf.log.set_syslog)
    dict_copy(log, "facility", str, path, conf.log.set_facility)
    dict_copy(log, "filter", str, path, conf.log.set_filter)


def domains_populate(conf, domains, path):
    if domains is None:
        return
    for domain_num, domain in enumerate(domains):
        domain_path = "%s[%d]" % (path, domain_num)
        assure_type(domain, dict, domain_path)

        addrs = dict_lookup(domain, "addrs", list, domain_path, required=True)

        conf.add_domain(addrs)


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
