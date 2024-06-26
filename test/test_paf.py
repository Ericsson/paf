# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

#
# Test suite where a Pathfinder Server is the system under test (SUT)
#
# The tests supports both the Python-based server hosted in this repo,
# and the C-based tpafd implementation.
#

import collections
import cpu_speed
import fcntl
import json
import multiprocessing
import os
import pytest
import queue
import random
import select
import shutil
import signal
import string
import subprocess
import sys
import threading
import time
import yaml

from enum import Enum

from feature import ServerFeature

import paf.client as client
import paf.proto as proto
import paf.xcm as xcm
import paf.server

BASE_DIR = os.getcwd()

SERVER_CERT = "%s/cert/server" % BASE_DIR

NUM_CLIENT_CERTS = 3
CLIENT_CERTS = ["%s/cert/client%d" % (BASE_DIR, n)
                for n in range(NUM_CLIENT_CERTS)]

os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[0]

random.seed()

spawn_mp = multiprocessing.get_context('spawn')


def random_bool():
    return random.random() < 0.5


def random_name(min_len=1):
    len = random.randint(min_len, max(min_len, 32))
    name = ""
    while len > 0:
        name += random.choice(string.ascii_letters)
        len -= 1
    return name


def random_ux_addr():
    return "ux:%s" % random_name(min_len=6)


def random_port():
    return random.randint(2000, 32000)


def random_octet():
    return random.randint(1, 254)


def random_lo_ip():
    return "127.%d.%d.%d" % (random_octet(), random_octet(), random_octet())


def random_tcp_addr():
    return "tcp:%s:%d" % (random_lo_ip(), random_port())


def random_tls_addr():
    return "tls:%s:%d" % (random_lo_ip(), random_port())


def random_addr():
    addr_fun = \
        random.choice([random_ux_addr, random_tcp_addr, random_tls_addr])
    while True:
        addr = addr_fun()
        try:
            # This is an attempt to make sure the address is
            # free. It's a little racey, but should be good enough.
            server = xcm.server(addr)
            server.close()
            return addr
        except xcm.error:
            # Socket likely in use - regenerate address
            pass


DOMAINS_DIR = 'domains.d'
CONFIG_FILE = 'pafd-test.conf'


class Domain:
    def __init__(self, name, addrs, proto_version_min, proto_version_max,
                 idle_min, idle_max):
        self.name = name
        self.addrs = addrs
        self.proto_version_min = proto_version_min
        self.proto_version_max = proto_version_max
        self.idle_min = idle_min
        self.idle_max = idle_max
        self.file = "%s/%s" % (DOMAINS_DIR, self.name)
        self.set_mapped_addr(self.default_addr())

    def set_mapped_addr(self, addr):
        assert addr in self.addrs
        with open(self.file, "w") as f:
            f.write(addr)
        self.mapped_addr = addr

    def random_addr(self):
        return random.choice(self.addrs)

    def default_addr(self):
        return self.addrs[0]

    def __del__(self):
        os.system("rm -f %s" % self.file)


def is_tls_addr(addr):
    return addr.split(":")[0] == "tls"


class BaseServer:
    def __init__(self, program_name, debug, in_valgrind):
        self.program_name = program_name
        self.debug = debug
        self.in_valgrind = in_valgrind
        self.domains = []
        self.process = None
        self.crl = None
        self.resources = None
        self.hook = None

        os.environ['PAF_DOMAINS'] = DOMAINS_DIR
        os.system("mkdir -p %s" % DOMAINS_DIR)

    def is_available(self):
        return shutil.which(self.program_name) is not None

    def max_or_many_clients(self):
        if self.supports(ServerFeature.RESOURCE_LIMITS):
            return self.max_clients
        else:
            return 200

    def max_proto_version(self):
        if self.supports(ServerFeature.PROTO_V3):
            return 3
        else:
            return 2

    def max_domain_proto_version(self, domain):
        max_version = self.max_proto_version()

        if domain.proto_version_max is not None:
            max_version = min(max_version, domain.proto_version_max)

        return max_version

    def random_domain(self):
        return random.choice(self.domains)

    def default_domain(self):
        return self.domains[0]

    def configure_domain(self, name, addrs, proto_version_min=None,
                         proto_version_max=None, idle_min=None,
                         idle_max=None):
        if isinstance(addrs, str):
            addrs = [addrs]

        if proto_version_min is not None or proto_version_max is not None or \
           idle_max is not None or idle_min is not None:
            self.use_config_file = True

        domain = Domain(name, addrs, proto_version_min, proto_version_max,
                        idle_min, idle_max)
        self.domains.append(domain)
        return domain

    def is_addr_used(self, addr):
        for domain in self.domains:
            if addr in domain.addrs:
                return True
        return False

    def is_name_used(self, name):
        for domain in self.domains:
            if domain.name == name:
                return True
        return False

    def get_random_name(self):
        while True:
            name = random_name()
            if not self.is_name_used(name):
                return name

    def configure_random_domain(self, num_addrs, addr_fun=random_addr,
                                **kwargs):
        name = self.get_random_name()
        addrs = []
        while (len(addrs) < num_addrs):
            addr = addr_fun()
            if not self.is_addr_used(addr):
                addrs.append(addr)
        return self.configure_domain(name, addrs, **kwargs)

    def set_resources(self, resources):
        self.resources = resources
        self.use_config_file = True

    def revoke_client0(self):
        self.crl = "%s/empty-crl.pem" % SERVER_CERT
        self.use_config_file = True

    def _write_config_file(self, use_tls_attrs):
        conf = {}
        if self.debug:
            log_conf = {}
            log_conf["filter"] = "debug"
            conf["log"] = log_conf
        domains_conf = []
        for domain in self.domains:
            if random_bool():
                sockets_name = "sockets"
            else:
                sockets_name = "addrs"  # old name
            sockets = []
            for addr in domain.addrs:
                if use_tls_attrs and is_tls_addr(addr):
                    tls_attrs = {
                        "cert": "%s/cert.pem" % SERVER_CERT,
                        "key": "%s/key.pem" % SERVER_CERT,
                        "tc": "%s/tc.pem" % SERVER_CERT
                    }

                    if self.crl is not None:
                        tls_attrs["crl"] = self.crl
                    elif random_bool():
                        tls_attrs["crl"] = "%s/empty-crl.pem" % SERVER_CERT

                    sockets.append({"addr": addr, "tls": tls_attrs})
                else:
                    if random_bool():
                        sockets.append({"addr": addr})
                    else:
                        sockets.append(addr)

            domain_conf = {sockets_name: sockets}

            if random_bool():
                domain_conf["name"] = "domain-%d" % random.randint(0, 10000)

            proto_version = {}

            if domain.proto_version_min is not None:
                proto_version["min"] = domain.proto_version_min
            if domain.proto_version_max is not None:
                proto_version["max"] = domain.proto_version_max
            if len(proto_version) > 0:
                domain_conf["protocol_version"] = proto_version

            if domain.idle_min is not None or domain.idle_max is not None:
                if random_bool() and domain.idle_min is None:
                    domain_conf["max_idle_time"] = domain.idle_max
                else:
                    idle = {
                    }
                    if domain.idle_min is not None:
                        idle["min"] = domain.idle_min
                    if domain.idle_min is not None:
                        idle["max"] = domain.idle_max
                    domain_conf["idle"] = idle

            domains_conf.append(domain_conf)
        conf["domains"] = domains_conf

        if self.resources is not None:
            conf["resources"] = self.resources
        else:
            conf["resources"] = {"total": {"clients": self.max_clients}}

        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(conf, f)

    def cmdline(self):
        line = self.cmd()
        if self.hook is not None:
            line.extend(["-r", self.hook])
        if self.use_config_file:
            line.extend(["-f", CONFIG_FILE])
        else:
            if self.supports(ServerFeature.RESOURCE_LIMITS):
                line.extend(["-c", str(self.max_clients)])
            if self.debug:
                line.extend(["-l", "debug"])
            for domain in self.domains:
                if self.supports(ServerFeature.MULTI_SOCKET_DOMAIN):
                    line.extend(["-m", "%s" % "+".join(domain.addrs)])
                else:
                    assert len(domain.addrs) == 1
                    line.extend([domain.addrs[0]])
        return line

    def _assure_up(self):
        # assumes the last address in the last domain is bound last
        domain = self.domains[-1]
        addr = domain.addrs[-1]
        conn = None
        while True:
            try:
                conn = client.connect(addr)
                conn.close()
                return
            except client.Error:
                if conn is not None:
                    conn.close()
                time.sleep(0.05)

    def start(self, python_path=None):
        if self.process is not None:
            return

        if self.use_config_file:
            if self.crl is not None:
                use_tls_attrs = True
            else:
                use_tls_attrs = random_bool()
            self._write_config_file(use_tls_attrs)
        else:
            use_tls_attrs = False

        cmdline = self.cmdline()
        pafd_env = os.environ.copy()
        if use_tls_attrs:
            pafd_env.pop('XCM_TLS_CERT')
        else:
            pafd_env['XCM_TLS_CERT'] = SERVER_CERT

        if python_path is not None:
            pafd_env['PYTHONPATH'] = "%s:%s" % \
                (python_path, pafd_env['PYTHONPATH'])
        self.process = subprocess.Popen(cmdline, env=pafd_env)
        self._assure_up()

    def stop(self, signo=signal.SIGTERM):
        if self.process is None:
            return
        self.process.send_signal(signo)
        self.process.wait()
        if self.use_config_file:
            os.remove(CONFIG_FILE)
        self.process = None


class PafServer(BaseServer):
    def __init__(self, debug, in_valgrind):
        BaseServer.__init__(self, "pafd", debug, in_valgrind)
        self.max_clients = 250
        self.use_config_file = random_bool()

    def cmd(self):
        return [self.program_name]

    @staticmethod
    def supports(feature):
        # you enumuerate it, we got it
        return True


class TpafServer(BaseServer):
    def __init__(self, debug, in_valgrind):
        BaseServer.__init__(self, "tpafd", debug, in_valgrind)
        self.use_config_file = False

    def cmd(self):
        if self.in_valgrind:
            return [
                "valgrind", "--tool=memcheck", "--leak-check=full",
                "--error-exitcode=1", "-q", self.program_name
            ]
        else:
            return ["tpafd"]

    @staticmethod
    def supports(feature):
        return False


SERVER_NAME_TO_CLASS = {"paf": PafServer, "tpaf": TpafServer}


def server_supports(server_name, feature):
    server_class = SERVER_NAME_TO_CLASS[server_name]
    return server_class.supports(feature)


def server_by_name(server_name, server_conf):
    server_class = SERVER_NAME_TO_CLASS[server_name]
    return server_class(**server_conf)


def request_to_conf(request):
    return {
        "debug": request.config.option.server_debug,
        "in_valgrind": request.config.option.server_valgrind
    }


def server_by_request(request):
    server_name = request.config.option.server
    server_conf = request_to_conf(request)
    return server_by_name(server_name, server_conf)


def random_server(server_name, server_conf, min_domains, max_domains,
                  min_addrs_per_domain, max_addrs_per_domain):
    server = server_by_name(server_name, server_conf)

    num_domains = random.randint(min_domains, max_domains)

    for i in range(num_domains):
        if server.supports(ServerFeature.MULTI_SOCKET_DOMAIN):
            num_addrs = random.randint(min_addrs_per_domain,
                                       max_addrs_per_domain)
        else:
            num_addrs = 1

        server.configure_random_domain(num_addrs)

    server.start()
    return server


def random_std_server(server_name, server_conf):
    return random_server(server_name, server_conf, 1, 4, 1, 4)


@pytest.fixture(scope='function')
def server(request):
    server = random_std_server(request.config.option.server,
                               request_to_conf(request))
    yield server
    server.stop()


@pytest.fixture(scope='function')
def v3_server(request):
    server = random_std_server(request.config.option.server,
                               request_to_conf(request))
    if not server.supports(ServerFeature.PROTO_V3):
        pytest.skip("Server does not support protocol version 3")
    yield server
    server.stop()


@pytest.fixture(scope='function')
def md_server(request):
    server = random_server(request.config.option.server,
                           request_to_conf(request), 8, 16, 1, 4)
    yield server
    server.stop()


@pytest.fixture(scope='function')
def ms_server(request):
    server = random_server(request.config.option.server,
                           request_to_conf(request), 1, 4, 16, 32)
    yield server
    server.stop()


@pytest.fixture(scope='function')
def tls_server(request):
    server = server_by_request(request)
    server.configure_random_domain(1, addr_fun=random_tls_addr)
    server.start()
    yield server
    server.stop()


MAX_USER_CLIENTS = 3
MAX_TOTAL_CLIENTS = 5
MAX_USER_SERVICES = 128
MAX_TOTAL_SERVICES = 192
MAX_USER_SUBSCRIPTIONS = 99
MAX_TOTAL_SUBSCRIPTIONS = 100


def limited_server(server_name, server_conf, resources):
    server = server_by_name(server_name, server_conf)

    server.configure_random_domain(1, addr_fun=random_tls_addr)
    server.configure_random_domain(1, addr_fun=random_ux_addr)
    server.set_resources(resources)

    server.start()

    return server


@pytest.fixture(scope='function')
def limited_clients_server(request):
    resources = {
        "user": {"clients": MAX_USER_CLIENTS},
        "total": {"clients": MAX_TOTAL_CLIENTS}
    }
    server = limited_server(request.config.option.server,
                            request_to_conf(request), resources)
    yield server
    server.stop()


@pytest.fixture(scope='function')
def limited_services_server(request):
    resources = {
        "user": {"services": MAX_USER_SERVICES},
        "total": {"services": MAX_TOTAL_SERVICES}
    }
    server = limited_server(request.config.option.server,
                            request_to_conf(request), resources)
    yield server
    server.stop()


@pytest.fixture(scope='function')
def limited_subscriptions_server(request):
    resources = {
        "user": {"subscriptions": MAX_USER_SUBSCRIPTIONS},
        "total": {"subscriptions": MAX_TOTAL_SUBSCRIPTIONS}
    }
    server = limited_server(request.config.option.server,
                            request_to_conf(request), resources)
    yield server
    server.stop()


IMPATIENT_IDLE_MIN = 2
IMPATIENT_IDLE_MAX = 4


@pytest.fixture(scope='function')
def impatient_server(request):
    server = server_by_request(request)
    if not server.supports(ServerFeature.PROTO_V3):
        pytest.skip("Server does not support protocol version 3")
    server.configure_random_domain(1, idle_min=IMPATIENT_IDLE_MIN,
                                   idle_max=IMPATIENT_IDLE_MAX)
    server.start()
    yield server
    server.stop()


def vn_only_server(request, proto_version):
    server = server_by_request(request)
    if server.supports(ServerFeature.PROTO_V3):
        server.configure_random_domain(1, proto_version_min=proto_version,
                                       proto_version_max=proto_version)
    else:
        if proto_version == 3:
            pytest.skip("Server does not support protocol version 3")
        server.configure_random_domain(1)

    return server


@pytest.fixture(scope='function')
def v2_only_server(request):
    server = vn_only_server(request, 2)
    server.start()
    yield server
    server.stop()


@pytest.fixture(scope='function')
def v3_only_server(request):
    server = vn_only_server(request, 3)
    server.start()
    yield server
    server.stop()


def set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def wait(conns, criteria=lambda: False, timeout=None):
    if isinstance(conns, client.Client):
        conns = [conns]

    rfd = None
    wfd = None
    old_wakeup_fd = None
    try:
        rfd, wfd = os.pipe()
        set_nonblocking(rfd)
        set_nonblocking(wfd)
        old_wakeup_fd = signal.set_wakeup_fd(wfd)
        if timeout is not None:
            deadline = time.time() + timeout

        poll = select.poll()
        poll.register(rfd, select.EPOLLIN)
        for conn in conns:
            poll.register(conn.fileno(), select.EPOLLIN)

        while not criteria():
            if timeout is not None:
                time_left = deadline - time.time()
                if time_left <= 0:
                    break
            else:
                time_left = None
            poll.poll(time_left)
            for conn in conns:
                conn.process()
    finally:
        if rfd is not None:
            os.close(rfd)
        if wfd is not None:
            os.close(wfd)
        if old_wakeup_fd is not None:
            signal.set_wakeup_fd(-1)


def delayed_close(conn):
    # always wait a little, to allow trailing messages to arrive, which
    # might sent in error, and thus should be detected
    wait(conn, timeout=0.01)
    conn.close()


class Recorder:
    def __init__(self):
        self.replies = []
        self.ta_id = None

    def __call__(self, ta_id, event, *args, **optargs):
        assert self.ta_id is not None
        assert self.ta_id == ta_id
        reply = [event]
        reply.extend(args)
        if len(optargs) > 0:
            reply.append(optargs)
        self.replies.append(tuple(reply))

    def get_replies(self, t):
        return list(filter(lambda reply: reply[0] == t, self.replies))


class TransactionState(Enum):
    REQUESTING = 0
    ACCEPTED = 1
    FAILED = 2
    COMPLETED = 3


class ResponseRecorderBase(Recorder):
    def __init__(self):
        Recorder.__init__(self)

    def get_complete(self):
        assert self.completed()
        return self.replies[-1]

    def get_fail(self):
        assert self.failed()
        return self.replies[-1]

    def get_fail_reason(self):
        return self.get_fail()[1][proto.FIELD_FAIL_REASON.python_name()]


class SingleResponseRecorder(ResponseRecorderBase):
    def __init__(self):
        self.state = TransactionState.REQUESTING
        ResponseRecorderBase.__init__(self)

    def __call__(self, ta_id, event, *args, **optargs):
        Recorder.__call__(self, ta_id, event, *args, **optargs)
        self.handle_state(event)

    def handle_state(self, event):
        assert self.state == TransactionState.REQUESTING
        if event == client.EventType.COMPLETE:
            self.state = TransactionState.COMPLETED
        elif event == client.EventType.FAIL:
            self.state = TransactionState.FAILED
        else:
            assert 0

    def completed(self):
        assert self.state != TransactionState.FAILED
        return self.state == TransactionState.COMPLETED

    def failed(self):
        assert self.state != TransactionState.COMPLETED
        return self.state == TransactionState.FAILED


class MultiResponseRecorder(ResponseRecorderBase):
    def __init__(self):
        self.state = TransactionState.REQUESTING
        ResponseRecorderBase.__init__(self)

    def __call__(self, ta_id, event, *args, **optargs):
        Recorder.__call__(self, ta_id, event, *args, **optargs)
        self.handle_state(event)

    def handle_state(self, event):
        if event == client.EventType.ACCEPT:
            assert self.state == TransactionState.REQUESTING
            self.state = TransactionState.ACCEPTED
        elif event == client.EventType.NOTIFY:
            assert self.state == TransactionState.ACCEPTED
        elif event == client.EventType.COMPLETE:
            assert self.state == TransactionState.ACCEPTED
            self.state = TransactionState.COMPLETED
        elif event == client.EventType.FAIL:
            assert self.state == TransactionState.ACCEPTED or \
                self.state == TransactionState.REQUESTING
            self.state = TransactionState.FAILED
        else:
            assert 0

    def accepted(self):
        assert self.state != TransactionState.FAILED
        return self.state == TransactionState.ACCEPTED

    def completed(self):
        assert self.state != TransactionState.FAILED
        return self.state == TransactionState.COMPLETED

    def failed(self):
        assert self.state != TransactionState.COMPLETED
        return self.state == TransactionState.FAILED

    def count_notifications(self):
        return len(self.get_notifications())

    def get_notifications(self):
        return self.get_replies(client.EventType.NOTIFY)

    def get_accept(self):
        return self.get_replies(client.EventType.ACCEPT)[0]


@pytest.mark.fast
def test_hello(server):
    domain = server.random_domain()
    conn = client.connect(domain.random_addr())
    assert conn.proto_version == server.max_domain_proto_version(domain)


def client_connect(server, proto_version):
    conf = client.ServerConf(server.random_domain().random_addr(),
                             proto_version_min=proto_version,
                             proto_version_max=proto_version)
    conn = client.connect(conf)
    assert conn.proto_version == proto_version

    return conn


@pytest.mark.fast
def test_hello_client_v2_only(server):
    client_connect(server, 2)


@pytest.mark.fast
def test_hello_client_v3_only(v3_server):
    client_connect(v3_server, 3)


@pytest.mark.fast
def test_server_force_v2(v2_only_server):
    client_connect(v2_only_server, 2)

    with pytest.raises(client.ProtocolError,
                       match=".*unsupported-protocol-version.*"):
        client_connect(v2_only_server, 3)


@pytest.mark.fast
def test_server_force_v3(v3_only_server):
    client_connect(v3_only_server, 3)

    with pytest.raises(client.ProtocolError,
                       match=".*unsupported-protocol-version.*"):
        client_connect(v3_only_server, 2)


def test_server_tracking_client(impatient_server):
    conn = client.connect(impatient_server.random_domain().random_addr())

    track_recorder = MultiResponseRecorder()
    ta_id = conn.track(track_recorder)

    track_recorder.ta_id = ta_id

    wait(conn, criteria=track_recorder.accepted)

    # Verify a track query is being sent
    start = time.time()
    timeout = IMPATIENT_IDLE_MAX
    query_time = timeout * 0.5

    wait(conn, criteria=lambda: track_recorder.count_notifications() > 0)

    latency = time.time() - start
    assert latency > (query_time - 0.5)
    assert latency < (query_time + 0.5)

    notifications = track_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.TRACK_TYPE_QUERY)
    ]

    conn.track_reply(ta_id)
    wait(conn, timeout=0.1)

    # Fail to respond to query, expect connection to be closed

    try:
        wait(conn, timeout=(timeout + 0.5))
        assert 0
    except paf.proto.ProtocolError:
        pass

    notifications = track_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.TRACK_TYPE_QUERY),
        (client.EventType.NOTIFY, client.TRACK_TYPE_QUERY)
    ]

    conn.close()


def test_client_activity_avoids_track_queries(impatient_server):
    conn = client.connect(impatient_server.random_domain().random_addr())

    track_recorder = MultiResponseRecorder()
    ta_id = conn.track(track_recorder)

    track_recorder.ta_id = ta_id

    wait(conn, criteria=track_recorder.accepted)

    iter = 10
    timeout = (IMPATIENT_IDLE_MAX + 1) / iter
    for _ in range(iter):
        wait(conn, timeout=timeout)
        conn.ping()

    assert track_recorder.count_notifications() == 0

    conn.close()


def test_ttl_induced_tracking(v3_server):
    conn = client.connect(v3_server.random_domain().random_addr())

    track_recorder = MultiResponseRecorder()
    ta_id = conn.track(track_recorder)

    track_recorder.ta_id = ta_id

    wait(conn, criteria=track_recorder.accepted)

    service_id = conn.service_id()
    generation = 1
    service_props = {
        "name": {"service-x"},
        "value": {0}
    }
    service_ttl = 4
    conn.publish(service_id, generation, service_props, service_ttl)

    start = time.time()
    wait(conn, criteria=lambda: track_recorder.count_notifications() == 1)
    latency = time.time() - start

    assert latency < service_ttl

    conn.track_reply(ta_id)

    conn.unpublish(service_id)

    wait(conn, timeout=service_ttl)

    # No new notifications since rate should have been decreased
    assert track_recorder.count_notifications() == 1

    conn.close()


def test_v2_client_not_timed_out(server):
    conf = client.ServerConf(server.random_domain().random_addr(),
                             proto_version_max=2)
    conn = client.connect(conf)

    assert conn.proto_version == 2

    service_id = conn.service_id()
    generation = 1
    service_props = {
        "name": {"service-x"},
        "value": {0}
    }
    service_ttl = 0
    conn.publish(service_id, generation, service_props, service_ttl)

    wait(conn, timeout=5)

    conn.ping()

    conn.close()


@pytest.mark.fast
def test_invalid_client_id_reuse(server):
    client_id = client.allocate_client_id()
    conn0 = client.connect(server.default_domain().random_addr(),
                           client_id=client_id)
    conn1 = None
    with pytest.raises(client.ProtocolError, match=".*client-id-exists.*"):
        conn1 = client.connect(server.default_domain().random_addr(),
                               client_id=client_id)
    if conn1 is not None:
        conn1.close()
    conn0.ping()
    conn0.close()


@pytest.mark.fast
def test_client_id_reuse_trigger_liveness_check(v3_server):
    client_id = client.allocate_client_id()
    conn0 = client.connect(v3_server.default_domain().random_addr(),
                           client_id=client_id)

    track_recorder = MultiResponseRecorder()
    ta_id = conn0.track(track_recorder)
    track_recorder.ta_id = ta_id

    wait(conn0, criteria=track_recorder.accepted)

    conn1 = None
    with pytest.raises(client.ProtocolError, match=".*client-id-exists.*"):
        conn1 = client.connect(v3_server.default_domain().random_addr(),
                               client_id=client_id)
    if conn1 is not None:
        conn1.close()

    def criteria():
        return track_recorder.count_notifications() == 1

    assert not criteria()

    wait(conn0, criteria=criteria, timeout=1)

    assert criteria()

    conn0.close()


NUM_SERVICES = 1000


@pytest.mark.fast
def test_batch_publish(server):
    conn = client.connect(server.random_domain().random_addr())

    service_ids = set()
    ta_ids = set()
    publish_recorders = []
    for i in range(NUM_SERVICES):
        publish_recorder = SingleResponseRecorder()
        service_id = conn.service_id()
        service_ids.add(service_id)
        ta_id = conn.publish(service_id, 0, {"name": {"service-a"}}, 42,
                             publish_recorder)
        publish_recorder.ta_id = ta_id
        assert ta_id not in ta_ids
        ta_ids.add(ta_id)
        publish_recorders.append(publish_recorder)

    for recorder in publish_recorders:
        wait(conn, criteria=recorder.completed)

    delayed_close(conn)


def run_republish_orphan(domain_addr, new_generation=True,
                         reused_client_id=None):
    conn_pub0 = client.connect(domain_addr, client_id=reused_client_id)

    service_id = conn_pub0.service_id()
    first_generation = 1
    first_service_props = {
        "name": {"service-x"},
        "value": {0}
    }
    service_ttl = 42

    conn_pub0.publish(service_id, first_generation,
                      first_service_props, service_ttl)

    conn_sub = client.connect(domain_addr)
    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(conn_sub.subscription_id(),
                               subscription_recorder)
    subscription_recorder.ta_id = ta_id
    wait([conn_sub, conn_pub0], criteria=subscription_recorder.accepted)

    wait([conn_sub, conn_pub0], criteria=lambda:
         subscription_recorder.count_notifications() >= 1)
    assert subscription_recorder.count_notifications() == 1

    if new_generation:
        second_generation = first_generation + 17
        second_service_props = {
            "name": {"service-x"},
            "value": {1}
        }
    else:
        second_generation = first_generation
        second_service_props = first_service_props

    conn_pub0.close()

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() >= 2)

    conn_pub1 = client.connect(domain_addr, client_id=reused_client_id)
    conn_pub1.publish(service_id, second_generation,
                      second_service_props, service_ttl)

    wait([conn_sub, conn_pub1], criteria=lambda:
         subscription_recorder.count_notifications() >= 3)

    notifications = subscription_recorder.get_notifications()
    orphan_since = notifications[1][3]['orphan_since']

    appeared = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': first_generation,
          'service_props': first_service_props, 'ttl': service_ttl,
          'client_id': conn_pub0.client_id})
    orphanized = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': first_generation,
          'service_props': first_service_props, 'ttl': service_ttl,
          'client_id': conn_pub0.client_id,
          'orphan_since': orphan_since})
    parented = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': second_generation,
          'service_props': second_service_props, 'ttl': service_ttl,
          'client_id': conn_pub1.client_id})

    assert subscription_recorder.get_notifications() == \
        [appeared, orphanized, parented]

    wait([conn_sub, conn_pub1], timeout=0.1)

    assert subscription_recorder.count_notifications() == 3

    conn_pub1.close()
    conn_sub.close()


@pytest.mark.fast
def test_republish_new_generation_orphan_from_same_client_id(server):
    domain_addr = server.random_domain().random_addr()
    client_id = client.allocate_client_id()
    run_republish_orphan(domain_addr, new_generation=True,
                         reused_client_id=client_id)


@pytest.mark.fast
def test_republish_same_generation_orphan_from_same_client_id(server):
    domain_addr = server.random_domain().random_addr()
    client_id = client.allocate_client_id()
    run_republish_orphan(domain_addr, new_generation=False,
                         reused_client_id=client_id)


@pytest.mark.fast
def test_republish_new_generation_orphan_from_different_client_id(server):
    domain_addr = server.random_domain().random_addr()
    run_republish_orphan(domain_addr, new_generation=True)


@pytest.mark.fast
def test_republish_same_generation_orphan_from_different_client_id(server):
    domain_addr = server.random_domain().random_addr()
    run_republish_orphan(domain_addr, new_generation=False)


def run_unpublish_orphan(domain_addr, reused_client_id=None):
    conn_pub0 = client.connect(domain_addr, client_id=reused_client_id)

    service_id = conn_pub0.service_id()
    generation = 1
    service_props = {
        "name": {"service-x"},
        "value": {0}
    }
    service_ttl = 42

    conn_pub0.publish(service_id, generation, service_props, service_ttl)

    conn_sub = client.connect(domain_addr)
    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(conn_sub.subscription_id(),
                               subscription_recorder)
    subscription_recorder.ta_id = ta_id
    wait([conn_sub, conn_pub0], criteria=subscription_recorder.accepted)

    wait([conn_sub, conn_pub0], criteria=lambda:
         subscription_recorder.count_notifications() >= 1)
    assert subscription_recorder.count_notifications() == 1

    conn_pub0.close()

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() >= 2)

    conn_pub1 = client.connect(domain_addr, client_id=reused_client_id)
    conn_pub1.unpublish(service_id)

    wait([conn_sub, conn_pub1], criteria=lambda:
         subscription_recorder.count_notifications() >= 4)

    notifications = subscription_recorder.get_notifications()
    orphan_since = notifications[1][3]['orphan_since']

    appeared = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub0.client_id})
    orphanized = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub0.client_id,
          'orphan_since': orphan_since})
    parented = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub1.client_id})
    disappeared = \
        (client.EventType.NOTIFY, client.MATCH_TYPE_DISAPPEARED, service_id)

    assert subscription_recorder.get_notifications() == \
        [appeared, orphanized, parented, disappeared]

    wait([conn_sub, conn_pub1], timeout=0.1)

    assert subscription_recorder.count_notifications() == 4

    conn_pub1.close()
    conn_sub.close()


@pytest.mark.fast
def test_unpublish_orphan_from_same_client_id(server):
    domain_addr = server.random_domain().random_addr()
    client_id = client.allocate_client_id()
    run_unpublish_orphan(domain_addr, reused_client_id=client_id)


@pytest.mark.fast
def test_unpublish_orphan_from_different_client_id(server):
    domain_addr = server.random_domain().random_addr()
    run_unpublish_orphan(domain_addr)


@pytest.mark.fast
@pytest.mark.require_access_control
def test_orphan_causes_rejection_of_client_with_new_user_id(tls_server):
    domain_addr = tls_server.default_domain().default_addr()
    client_id = client.allocate_client_id()

    os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[0]
    conn_owner = client.connect(domain_addr, client_id=client_id)

    service_id = conn_owner.service_id()
    generation = 1
    service_props = {
        "name": {"service-x"},
    }
    service_ttl = 2

    conn_owner.publish(service_id, generation, service_props, service_ttl)

    conn_owner.close()

    time.sleep(service_ttl / 2)

    os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[1]

    with pytest.raises(client.ProtocolError):
        client.connect(domain_addr, client_id=client_id)

    time.sleep(service_ttl / 2 + 0.25)

    conn = client.connect(domain_addr)

    # the non-owning client id should not affect the orphan status, and
    # thus the service should be removed by now
    assert len(conn.services()) == 0

    conn.close()

    # after the orphaned service has timed out, the client should be
    # able to connect
    non_owner = client.connect(domain_addr, client_id=client_id)
    non_owner.ping()
    non_owner.close()


@pytest.mark.fast
def test_reconnect_immediate_disconnect(server):
    domain_addr = server.random_domain().random_addr()

    client_id = client.allocate_client_id()
    service_id = 4711
    service_generation = 0
    service_props = {"name": {"foo"}}
    service_ttl = 42

    conn = client.connect(domain_addr, client_id=client_id)
    conn.publish(service_id, service_generation, service_props,
                 service_ttl)

    conn.close()

    conn = client.connect(domain_addr, client_id=client_id)
    conn.close()

    conn = client.connect(domain_addr, client_id=client_id)
    conn.publish(service_id, service_generation, service_props,
                 service_ttl)
    conn.close()

    conn = client.connect(domain_addr)
    conn.ping()


@pytest.mark.fast
def test_unpublish_nonexisting_service(server):
    conn = client.connect(server.random_domain().random_addr())

    nonexisting_service_id = 4711
    reason = proto.FAIL_REASON_NON_EXISTENT_SERVICE_ID
    with pytest.raises(client.TransactionError, match=".*%s.*" % reason):
        conn.unpublish(nonexisting_service_id)

    delayed_close(conn)


@pytest.mark.fast
def test_republish_same_generation_non_orphan_same_connection(server):
    conn = client.connect(server.random_domain().random_addr())

    service_id = conn.service_id()
    service_generation = 99
    service_props = {"name": {"foo"}}
    service_ttl = 42

    conn.publish(service_id, service_generation, service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)
    wait(conn, timeout=0.1)

    assert subscription_recorder.count_notifications() == 1

    for i in range(10):
        conn.publish(service_id, service_generation, service_props,
                     service_ttl)

    wait(conn, timeout=0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.close()


def run_failed_republish(server, first_generation, first_props, first_ttl,
                         second_generation, second_props, second_ttl,
                         expected_reason):
    conn = client.connect(server.random_domain().random_addr())

    service_id = conn.service_id()

    conn.publish(service_id, first_generation, first_props,
                 first_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)
    wait(conn, timeout=0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.publish(service_id, first_generation, first_props,
                 first_ttl)

    assert subscription_recorder.count_notifications() == 1

    with pytest.raises(client.TransactionError,
                       match=".*%s.*" % expected_reason):
        conn.publish(service_id, second_generation,
                     second_props, second_ttl)

    assert subscription_recorder.count_notifications() == 1

    conn.close()


@pytest.mark.fast
def test_republish_same_and_older_generation(server):
    first_generation = 11
    second_generation = 10
    service_props = {"name": {"foo"}}
    service_ttl = 42

    run_failed_republish(server, first_generation, service_props,
                         service_ttl, second_generation,
                         service_props, service_ttl,
                         proto.FAIL_REASON_OLD_GENERATION)


@pytest.mark.fast
def test_republish_same_generation_with_different_props(server):
    generation = 11111111111
    first_props = {"name": {"foo"}}
    second_props = {"name": {"bar"}}
    service_ttl = 42

    run_failed_republish(server, generation, first_props,
                         service_ttl, generation,
                         second_props, service_ttl,
                         proto.FAIL_REASON_SAME_GENERATION_BUT_DIFFERENT)


@pytest.mark.fast
def test_republish_same_generation_with_different_ttl(server):
    generation = 11111111111
    props = {"name": {"foo"}}
    first_ttl = 42
    second_ttl = 41

    run_failed_republish(server, generation, props,
                         first_ttl, generation,
                         props, second_ttl,
                         proto.FAIL_REASON_SAME_GENERATION_BUT_DIFFERENT)


def run_unpublish_republished_non_orphan(domain_addr, new_generation=True):
    conn0 = client.connect(domain_addr)

    generation = 0
    service_id = conn0.service_id()
    service_props = {"name": {"service-x"}}
    service_ttl = 42

    conn0.publish(service_id, generation, service_props, service_ttl)

    conn1 = client.connect(domain_addr)

    if new_generation:
        generation += 1
    conn1.publish(service_id, generation, service_props, service_ttl)

    conn1.unpublish(service_id)

    delayed_close(conn0)
    delayed_close(conn1)


@pytest.mark.fast
def test_unpublish_republished_non_orphan_same_generation(server):
    domain_addr = server.random_domain().random_addr()
    run_unpublish_republished_non_orphan(domain_addr, new_generation=False)


@pytest.mark.fast
def test_unpublish_republished_non_orphan_different_generation(server):
    domain_addr = server.random_domain().random_addr()
    run_unpublish_republished_non_orphan(domain_addr, new_generation=True)


@pytest.mark.fast
def test_unpublish_from_different_client_same_user(server):
    domain_addr = server.random_domain().random_addr()
    conn0 = client.connect(domain_addr)

    service_id = conn0.service_id()
    conn0.publish(service_id, 0, {"name": {"service-x"}}, 42)

    conn1 = client.connect(domain_addr)

    conn1.unpublish(service_id)

    delayed_close(conn0)
    delayed_close(conn1)


@pytest.mark.fast
@pytest.mark.require_access_control
def test_unpublish_from_different_user(tls_server):
    domain_addr = tls_server.default_domain().default_addr()

    os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[0]
    conn = client.connect(domain_addr)
    service_id = conn.service_id()
    ta_id = conn.publish(service_id, 0, {"name": {"service-x"}}, 42)
    conn.close()

    os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[1]
    conn = client.connect(domain_addr)
    unpublish_recorder = SingleResponseRecorder()
    ta_id = conn.unpublish(service_id, unpublish_recorder)
    unpublish_recorder.ta_id = ta_id
    wait(conn, criteria=unpublish_recorder.failed)
    conn.close()

    os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[0]
    conn = client.connect(domain_addr)
    unpublish_recorder = SingleResponseRecorder()
    ta_id = conn.unpublish(service_id, unpublish_recorder)
    unpublish_recorder.ta_id = ta_id
    wait(conn, criteria=unpublish_recorder.completed)
    conn.close()


@pytest.mark.fast
def test_publish_and_unpublish_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder,
                           filter='(&(name=service-a)(area=51))')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)

    m_service_props = {
        "name": {"service-a"},
        "address": {"tls:10.10.10.10:1010"},
        "area": {51}
    }
    m_service_generation = 99
    m_service_ttl = 4711

    m_service_id = conn.service_id()
    conn.publish(m_service_id, m_service_generation, m_service_props,
                 m_service_ttl)

    notifications = subscription_recorder.get_notifications()
    assert len(notifications) == 1
    assert notifications[0] == (client.EventType.NOTIFY,
                                client.MATCH_TYPE_APPEARED,
                                m_service_id,
                                {
                                    'generation': m_service_generation,
                                    'service_props': m_service_props,
                                    'ttl': m_service_ttl,
                                    'client_id': conn.client_id
                                })

    # Unpublish trigger subscription
    conn.unpublish(m_service_id)

    wait(conn, criteria=lambda:
         subscription_recorder.count_notifications() >= 2)

    notifications = subscription_recorder.get_notifications()
    assert len(notifications) == 2
    assert notifications[1] == (client.EventType.NOTIFY,
                                client.MATCH_TYPE_DISAPPEARED,
                                m_service_id)

    conn.publish(conn.service_id(), 0, {
        "name": {"non-matching-name"},
        "area": {51}
    }, 99, lambda *args: None)
    conn.publish(conn.service_id(), 0, {
        "name": {"service-a"},
        "area": {42}
    }, 99, lambda *args: None)

    wait(conn, timeout=0.5)

    assert subscription_recorder.count_notifications() == 2

    conn.close()


@pytest.mark.fast
def test_ttl_change_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(conn.subscription_id(), subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)

    service_id = conn.service_id()
    service_props = {'name': {'a b c', 'd e f'}}
    service_first_ttl = 4711
    first_generation = 1

    conn.publish(service_id, first_generation,
                 service_props, service_first_ttl)

    second_generation = first_generation + 1
    service_second_ttl = service_first_ttl * 2
    conn.publish(service_id, second_generation,
                 service_props, service_second_ttl)

    wait(conn, criteria=lambda:
         subscription_recorder.count_notifications() >= 2)

    assert subscription_recorder.get_notifications() == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': first_generation, 'service_props': service_props,
          'ttl': service_first_ttl, 'client_id': conn.client_id}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': second_generation, 'service_props': service_props,
          'ttl': service_second_ttl, 'client_id': conn.client_id})
    ]

    conn.close()


@pytest.mark.fast
def test_subscribe_to_existing_service(server):
    conn = client.connect(server.random_domain().random_addr())

    service_generation = 10
    service_props = {
        "name": {"service-x"},
        "key": {"value"},
        "another_key": {"the_same_value"}
    }
    service_ttl = 99

    service_id = conn.service_id()
    conn.publish(service_id, service_generation, service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(99, subscription_recorder,
                           filter='(name=service-x)')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=lambda:
         subscription_recorder.count_notifications() > 0)
    wait(conn, timeout=0.1)

    notifications = subscription_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn.client_id})
    ]

    assert len(conn.subscriptions()) == 1
    conn.unsubscribe(99)
    assert len(conn.subscriptions()) == 0

    conn.close()


@pytest.mark.fast
def test_subscription_id_errornous_reuse(server):
    conn = client.connect(server.random_domain().random_addr())

    sub_id = 99

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.failed)
    assert subscription_recorder.get_fail_reason() == \
        proto.FAIL_REASON_SUBSCRIPTION_ID_EXISTS

    conn.close()


@pytest.mark.fast
def test_subscription_id_valid_reuse(server):
    conn = client.connect(server.random_domain().random_addr())

    sub_id = 99

    subscribe_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscribe_recorder)
    subscribe_recorder.ta_id = ta_id

    wait(conn, criteria=lambda: subscribe_recorder.accepted)

    unsubscribe_recorder = SingleResponseRecorder()
    ta_id = conn.unsubscribe(sub_id, unsubscribe_recorder)
    unsubscribe_recorder.ta_id = ta_id

    wait(conn, criteria=lambda: unsubscribe_recorder.completed and
         subscribe_recorder.completed)

    resubscribe_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, resubscribe_recorder)
    resubscribe_recorder.ta_id = ta_id

    wait(conn, criteria=resubscribe_recorder.accepted)

    conn.close()


@pytest.mark.fast
def test_subscribe_invalid_syntax_filter(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(99, subscription_recorder,
                           filter='(name=service-x')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.failed)

    assert subscription_recorder.get_fail_reason() == \
        proto.FAIL_REASON_INVALID_FILTER_SYNTAX

    conn.close()


@pytest.mark.fast
def test_modify_existing_trigger_now_matching_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    service_generations = [2, 5, 1000]

    service_id = conn.service_id()
    service_ttl = 34123122
    conn.publish(service_id, service_generations[0],
                 {"name": {"foo"}}, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder,
                           filter='(&(name=foo)(area=51))')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)
    wait(conn, timeout=0.1)

    notifications = subscription_recorder.get_notifications()
    assert len(notifications) == 0

    conn.publish(service_id, service_generations[1],
                 {"name": {"foo"}, "area": {51}}, service_ttl)

    conn.publish(service_id, service_generations[2],
                 {"name": {"bar"}, "area": {51}}, service_ttl)

    wait(conn, criteria=lambda:
         subscription_recorder.count_notifications() >= 2)

    notifications = subscription_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': service_generations[1],
          'service_props': {'name': {'foo'}, 'area': {51}},
          'ttl': service_ttl, 'client_id': conn.client_id}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_DISAPPEARED, service_id)
    ]
    conn.close()


@pytest.mark.fast
def test_unsubscribe(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()

    sub_id = 17
    subscription_ta_id = conn.subscribe(sub_id, subscription_recorder,
                                        filter='(name=service-x)')
    subscription_recorder.ta_id = subscription_ta_id
    wait(conn, criteria=subscription_recorder.accepted)

    conn.unsubscribe(sub_id)

    wait(conn, criteria=subscription_recorder.completed)

    conn.publish(conn.service_id(), 17,
                 {"name": {"service-x", "service-y"}}, 42)

    delayed_close(conn)


@pytest.mark.fast
def test_unsubscribe_nonexisting(server):
    conn = client.connect(server.random_domain().random_addr())

    nonexisting_sub_id = 4711

    unsubscribe_recorder = SingleResponseRecorder()
    unsubscribe_ta_id = conn.unsubscribe(nonexisting_sub_id,
                                         unsubscribe_recorder)
    unsubscribe_recorder.ta_id = unsubscribe_ta_id

    wait(conn, criteria=unsubscribe_recorder.failed)
    assert unsubscribe_recorder.get_fail_reason() == \
        proto.FAIL_REASON_NON_EXISTENT_SUBSCRIPTION_ID

    delayed_close(conn)


@pytest.mark.fast
def test_unsubscribe_from_non_owner(server):
    domain = server.random_domain()
    conn0 = client.connect(domain.default_addr())

    subscription_recorder = MultiResponseRecorder()

    sub_id = 99
    subscription_ta_id = conn0.subscribe(sub_id, subscription_recorder,
                                         filter='(name=service-x)')
    subscription_recorder.ta_id = subscription_ta_id
    wait(conn0, criteria=subscription_recorder.accepted)

    conn1 = client.connect(domain.default_addr())

    unsubscribe_recorder = SingleResponseRecorder()
    unsubscribe_ta_id = conn1.unsubscribe(sub_id, unsubscribe_recorder)
    unsubscribe_recorder.ta_id = unsubscribe_ta_id

    wait(conn1, criteria=unsubscribe_recorder.failed)
    assert unsubscribe_recorder.get_fail_reason() == \
        proto.FAIL_REASON_PERMISSION_DENIED

    delayed_close(conn0)
    delayed_close(conn1)


def by_id(keys):
    return keys[0]


NUM_CLIENTS = 10


@pytest.mark.fast
def test_list_subscriptions(server):
    conns = []
    subscriptions = []
    domain = server.random_domain()
    for i in range(NUM_CLIENTS):
        conn = client.connect(domain.default_addr())

        filter = "(&(name=service-%d)(prop=%d))" % (i, i)

        sub_id = conn.subscription_id()
        subscriptions.append([sub_id, conn.client_id, {'filter': filter}])
        subscription_recorder = MultiResponseRecorder()
        ta_id = conn.subscribe(sub_id, subscription_recorder,
                               filter=filter)
        subscription_recorder.ta_id = ta_id

        wait(conn, criteria=subscription_recorder.accepted)

        conns.append(conn)

    list_conn = random.choice(conns)

    assert sorted(list_conn.subscriptions(), key=by_id) == \
        sorted(subscriptions, key=by_id)

    for conn in conns:
        conn.close()


@pytest.mark.fast
def test_list_services(server):
    conn = client.connect(server.random_domain().random_addr())

    services = []

    for num in range(NUM_SERVICES):
        service_id = conn.service_id()
        service_generation = random.randint(0, 100)
        service_props = {
            "name": {"service-%d" % num},
            "key_str": {"value%d" % num},
            "key_int": {num},
            "key_mv": {"strval%d" % num, num}
        }
        service_ttl = 99

        service = [service_id, service_generation,
                   service_props, service_ttl, conn.client_id]

        services.append(service)
        conn.publish(service_id, service_generation, service_props,
                     service_ttl)

    assert sorted(conn.services(), key=by_id) == \
        sorted(services, key=by_id)

    assert len(conn.services(filter="(key_int>0)")) == (NUM_SERVICES - 1)


@pytest.mark.fast
def test_list_orphan(server):
    domain = server.random_domain()
    pub_conn = client.connect(domain.default_addr())

    service_id = pub_conn.service_id()
    service_generation = 123
    service_props = {"name": "foo"}
    service_ttl = 99

    pub_conn.publish(service_id, service_generation, service_props,
                     service_ttl)

    pub_conn.close()

    list_conn = client.connect(domain.default_addr())
    assert len(list_conn.clients()) == 1

    services = list_conn.services()
    assert len(services) == 1
    orphan_since = services[0][5]['orphan_since']
    assert orphan_since <= time.time()


@pytest.mark.fast
def test_list_services_with_invalid_filter(server):
    conn = client.connect(server.random_domain().random_addr())

    recorder = MultiResponseRecorder()
    ta_id = conn.services(recorder, filter="(&foo)")
    recorder.ta_id = ta_id

    wait(conn, criteria=recorder.failed)

    assert recorder.get_fail_reason() == \
        proto.FAIL_REASON_INVALID_FILTER_SYNTAX

    conn.close()


@pytest.mark.fast
def test_disconnected_client_orphans_service(server):
    domain = server.random_domain()
    conn_sub = client.connect(domain.default_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(42, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn_sub, criteria=subscription_recorder.accepted)

    conn_pub = client.connect(domain.default_addr())

    service_id = conn_pub.service_id()
    service_generation = 10
    service_props = {
        "name": {"service-x"},
        "value": {0}
    }
    service_ttl = 1
    conn_pub.publish(service_id, service_generation, service_props,
                     service_ttl)

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() == 1)

    disconnect_time = time.time()

    conn_pub.close()

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() == 2)
    orphan_latency = time.time() - disconnect_time

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() >= 3)
    timeout_latency = time.time() - disconnect_time

    assert orphan_latency < 0.25
    assert timeout_latency > service_ttl
    assert timeout_latency < service_ttl + 0.25

    notifications = subscription_recorder.get_notifications()

    orphan_since = notifications[1][3]['orphan_since']
    assert orphan_since >= disconnect_time
    assert orphan_since <= disconnect_time + 0.25

    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub.client_id}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub.client_id,
          'orphan_since': orphan_since}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_DISAPPEARED, service_id)
    ]

    assert len(conn_sub.services()) == 0

    conn_sub.close()


def crashing_client(domain_addr, service_ttl):
    conn = client.connect(domain_addr)

    conn.publish(conn.service_id(), 0, {}, service_ttl)
    conn.publish(conn.service_id(), 0, {}, service_ttl)


@pytest.mark.fast
def test_survives_connection_reset(server):
    domain_addr = server.random_domain().random_addr()
    service_ttl = 1

    t = threading.Thread(target=crashing_client,
                         args=(domain_addr, service_ttl))
    t.start()
    t.join()

    time.sleep(service_ttl + 0.25)

    conn = client.connect(domain_addr)
    conn.close()


CLIENT_PROCESS_TTL = 1


class ClientProcess(spawn_mp.Process):
    def __init__(self, domain_addr, ready_queue, unpublish=True,
                 unsubscribe=True):
        spawn_mp.Process.__init__(self)
        self.domain_addr = domain_addr
        self.ready_queue = ready_queue
        self.unpublish = unpublish
        self.unsubscribe = unsubscribe
        self.stop = False

    def handle_term(self, signo, stack):
        self.stop = True

    def run(self):
        # to avoid sharing seed among all the clients
        random.seed(time.time() + os.getpid())
        signal.signal(signal.SIGTERM, self.handle_term)
        conn = None
        while conn is None:
            try:
                conn = client.connect(self.domain_addr, track=True)
            except proto.Error:
                time.sleep(3*random.random())

        service_id = conn.service_id()
        generation = 0
        service_props = {"name": {"service-%d" % service_id}}
        service_ttl = CLIENT_PROCESS_TTL
        conn.publish(service_id, generation, service_props, service_ttl)

        sub_id = conn.subscription_id()
        conn.subscribe(sub_id, lambda *args, **optargs: None)

        self.ready_queue.put(True)

        wait(conn, criteria=lambda: self.stop)

        if self.unpublish:
            conn.unpublish(service_id)
        if self.unsubscribe:
            conn.unsubscribe(sub_id)

        sys.exit(0)


@pytest.mark.fast
def test_survives_killed_clients(server):
    domain_addr = server.random_domain().random_addr()

    num_clients = server.max_or_many_clients()

    num_clients = cpu_speed.adjust_cardinality_down(num_clients)

    ready_queue = spawn_mp.Queue()
    processes = []
    for i in range(num_clients):
        p = ClientProcess(domain_addr, ready_queue)
        p.start()
        processes.append(p)
        ready_queue.get()

    for p in processes:
        if random.random() < 0.75:
            p.terminate()
        else:
            os.kill(p.pid, signal.SIGKILL)

    time.sleep(1)

    conn = client.connect(domain_addr)

    conn.ping()

    for p in processes:
        p.join()

    conn.ping()

    conn.close()


def run_client_reclaims_service(domain, reused_client_id=None):
    conn_pub0 = client.connect(domain.default_addr(),
                               client_id=reused_client_id)

    service_id = conn_pub0.service_id()
    service_generation = 10
    service_props = collections.defaultdict(set)
    service_ttl = 2

    conn_pub0.publish(service_id, service_generation,
                      service_props, service_ttl)

    conn_sub = client.connect(domain.default_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(42, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn_sub, criteria=subscription_recorder.accepted)

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() == 1)

    conn_pub0.close()

    wait(conn_sub, timeout=service_ttl-1)

    assert subscription_recorder.count_notifications() == 2

    conn_pub1 = client.connect(domain.default_addr(),
                               client_id=reused_client_id, track=True)

    conn_pub1.publish(service_id, service_generation,
                      service_props, service_ttl)

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() >= 3)

    # wait for any (errornous!) timeout to happen
    wait([conn_sub, conn_pub1], timeout=2)

    notifications = subscription_recorder.get_notifications()
    orphan_since = notifications[1][3]['orphan_since']

    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub0.client_id}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub0.client_id,
          'orphan_since': orphan_since}),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         {'generation': service_generation, 'service_props': service_props,
          'ttl': service_ttl, 'client_id': conn_pub1.client_id})
    ]

    conn_sub.close()


@pytest.mark.fast
def test_same_client_reclaims_service(server):
    domain = server.random_domain()
    client_id = client.allocate_client_id()
    run_client_reclaims_service(domain, reused_client_id=client_id)


@pytest.mark.fast
def test_different_client_reclaims_service(server):
    domain = server.random_domain()
    run_client_reclaims_service(domain)


MANY_ORPHANS = 100


@pytest.mark.fast
def test_many_orphans(server):
    domain = server.random_domain()

    service_generation = 0
    service_props = {}
    min_service_ttl = 1
    max_service_ttl = 2
    for service_id in range(MANY_ORPHANS):
        conn = client.connect(domain.default_addr())
        service_ttl = random.randint(min_service_ttl, max_service_ttl)
        conn.publish(service_id, service_generation,
                     service_props, service_ttl)
        conn.close()
    time.sleep(max_service_ttl + 0.25)
    conn = client.connect(domain.default_addr())
    assert len(conn.services()) == 0
    assert len(conn.clients()) == 1
    conn.close()


@pytest.mark.fast
def test_orphan_race(server):
    domain_addr = server.random_domain().default_addr()

    # Connection must come through the same address (otherwise,
    # different XCM transport may be used, and they might be
    # considered different users).
    conn0 = client.connect(domain_addr)
    conn1 = client.connect(domain_addr)

    service_id = 123123
    service_generation = 100
    service_props = {}
    service_ttl = 1

    conn0.publish(service_id, service_generation, service_props, service_ttl)
    # To simulate that the server hasn't made conn0's services orphans
    # yet, don't close the connection. This might happen with TCP/TLS,
    # since TCP Keepalive made cause the client considered the
    # connection lost, but the server does not yet agree.

    conn1.publish(service_id, service_generation, service_props, service_ttl)

    conn0.close()

    wait(conn1, timeout=service_ttl + 0.25)

    assert len(conn1.services()) == 1

    conn1.close()


ORDERING_MAX_TTL = 3


def test_orphan_ordering(server):
    domain_addr = server.random_domain().random_addr()

    pub_conn = client.connect(domain_addr)
    sub_conn = client.connect(domain_addr)

    ttls = {}
    for i in range(MANY_ORPHANS):
        service_id = i
        service_ttl = random.randint(0, ORDERING_MAX_TTL)
        ttls[service_id] = service_ttl

        pub_conn.publish(service_id, 0, {}, service_ttl)

    sub_recorder = MultiResponseRecorder()
    ta_id = sub_conn.subscribe(sub_conn.subscription_id(),
                               sub_recorder)
    sub_recorder.ta_id = ta_id

    def many_appeared():
        return len([True for n in sub_recorder.get_notifications()
                    if n[1] == client.MATCH_TYPE_APPEARED]) == MANY_ORPHANS

    wait(sub_conn, criteria=many_appeared)

    pub_conn.close()

    wait(sub_conn, timeout=(ORDERING_MAX_TTL + 1))

    notifications = sub_recorder.get_notifications()

    removed = 0
    prev_ttl = None
    for n in notifications:
        event_type, match_type, service_id, *rest = n
        if match_type == client.MATCH_TYPE_DISAPPEARED:
            if prev_ttl is not None:
                ttl = ttls[service_id]
                assert ttl >= prev_ttl
                prev_ttl = ttl
            else:
                prev_ttl = ttls[service_id]
            removed += 1

    assert removed == MANY_ORPHANS

    sub_conn.close()


def run_misbehaving_client(addr, junk_msg, skip_hello=True):
    valid_hello = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 20,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: client.allocate_client_id(),
        proto.FIELD_PROTO_MIN_VERSION.name: 0,
        proto.FIELD_PROTO_MAX_VERSION.name: 99999
    }
    msgs = [
        json.dumps(valid_hello).encode('utf-8'),
        junk_msg
    ]
    conn = xcm.connect(addr, 0)

    for msg in msgs:
        conn.send(msg)

    while True:
        msg = conn.receive()
        if len(msg) == 0:
            conn.close()
            break


@pytest.mark.fast
def test_misbehaving_clients(server):
    domain_addr = server.random_domain().random_addr()
    conn = client.connect(domain_addr)

    unknown_cmd = {
        proto.FIELD_TA_CMD.name: "non-existing-command",
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(unknown_cmd).encode('utf-8'))

    wrong_type = {
        proto.FIELD_TA_CMD.name: proto.CMD_PING,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_FAIL
    }
    run_misbehaving_client(domain_addr, json.dumps(wrong_type).encode('utf-8'))

    negative_uint = {
        proto.FIELD_TA_CMD.name: proto.CMD_PING,
        proto.FIELD_TA_ID.name: -42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(negative_uint).encode('utf-8'))

    too_large_ta_id = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 1 << 63,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 4711,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.MAX_VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.MAX_VERSION+2
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(too_large_ta_id).encode('utf-8'),
                           skip_hello=True)

    too_large_client_id = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 99,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 1 << 99,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.MAX_VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.MAX_VERSION+2
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(too_large_client_id).encode('utf-8'),
                           skip_hello=True)

    extra_fields = {
        proto.FIELD_TA_CMD.name: proto.CMD_SERVICES,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 99
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(extra_fields).encode('utf-8'))

    missing_ta_id = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 4711,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.MAX_VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.MAX_VERSION+2
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(missing_ta_id).encode('utf-8'))
    run_misbehaving_client(domain_addr,
                           json.dumps(missing_ta_id).encode('utf-8'),
                           skip_hello=True)

    missing_fields = {
        proto.FIELD_TA_CMD.name: proto.CMD_SUBSCRIBE,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(missing_fields).encode('utf-8'))

    prop_value_not_list = {
        proto.FIELD_TA_CMD.name: proto.CMD_PUBLISH,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_SERVICE_ID.name: 123,
        proto.FIELD_GENERATION.name: 0,
        proto.FIELD_SERVICE_PROPS.name: {"name": "not-a-list"},
        proto.FIELD_TTL.name: 5
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(prop_value_not_list).encode('utf-8'))

    run_misbehaving_client(domain_addr, "not valid JSON at all")

    conn.ping()

    delayed_close(conn)


@pytest.mark.fast
def test_many_clients(server):
    domain = server.random_domain()
    conns = []

    num_clients = server.max_or_many_clients()

    while len(conns) < num_clients:
        try:
            conn = client.connect(domain.default_addr())
            conns.append(conn)
        except client.TransportError:
            pass
    replies = []

    def cb(ta_id, *args):
        replies.append(None)
    for i, conn in enumerate(conns):
        conn.ping(response_cb=cb)
    for i, conn in enumerate(conns):
        wait(conn, criteria=lambda: len(replies) == i+1)

    if server.supports(ServerFeature.RESOURCE_LIMITS):
        last_conn = None
        try:
            # the server shouldn't be accepting any more connections
            def fail():
                assert False
            last_conn = client.connect(domain.default_addr(), ready_cb=fail)
            wait(last_conn, timeout=0.5)
            last_conn.close()
        except client.Error:
            pass

    for conn in conns:
        conn.close()


FEW_CLIENTS = 4


def run_list_clients(server, proto_version):
    domain = server.random_domain()
    conf = client.ServerConf(domain.default_addr(),
                             proto_version_min=proto_version,
                             proto_version_max=proto_version)
    conn = client.connect(conf)

    other_conns = []
    for i in range(FEW_CLIENTS):
        other_conn = client.connect(domain.default_addr())
        other_conns.append(other_conn)

    recorder = MultiResponseRecorder()
    ta_id = conn.clients(recorder)
    recorder.ta_id = ta_id
    wait(conn, criteria=recorder.completed)

    notifications = recorder.get_notifications()
    assert len(notifications) == FEW_CLIENTS + 1

    for other_conn in other_conns:
        other_conn.close()
    conn.close()


@pytest.mark.fast
def test_list_clients_v2(server):
    run_list_clients(server, 2)


@pytest.mark.fast
def test_list_clients_v3(v3_server):
    run_list_clients(v3_server, 3)


@pytest.mark.fast
def test_multiple_domains(md_server):
    conns = []
    for domain in md_server.domains:
        conn = client.connect(domain.random_addr())
        conn.publish(4711, 42, {},  17)
        conns.append(conn)

    for conn in conns:
        assert len(conn.clients()) == 1
        assert len(conn.services()) == 1

    for conn in conns:
        conn.close()


@pytest.mark.fast
def test_multiple_sockets_per_domain(ms_server):
    domain = ms_server.random_domain()
    assert len(domain.addrs) > 0

    conns = []
    for num, addr in enumerate(domain.addrs):
        conn = client.connect(addr)
        conn.publish(num, 99, {},  17)
        conns.append(conn)

    for conn in conns:
        assert len(conn.clients()) == len(domain.addrs)
        assert len(conn.services()) == len(domain.addrs)

    for conn in conns:
        conn.close()


@pytest.mark.fast
def test_connect_by_domain_name(server):
    for domain in server.domains:
        conn_by_name = client.connect(domain.name)
        conn_by_addr = client.connect(domain.random_addr())

        assert len(conn_by_name.clients()) == 2

        conn_by_name.close()
        conn_by_addr.close()


MANY_REQUESTS = 10000


@pytest.mark.fast
def test_many_requests(server):
    conn = client.connect(server.random_domain().random_addr())

    ping_recorders = []
    for i in range(MANY_REQUESTS):
        ping_recorder = SingleResponseRecorder()
        ta_id = conn.ping(ping_recorder)
        ping_recorder.ta_id = ta_id
        ping_recorders.append(ping_recorder)

    ping_recorders.reverse()

    for ping_recorder in ping_recorders:
        wait(conn, criteria=ping_recorder.completed)

    delayed_close(conn)


def assure_ping(conn, max_latency):
    start = time.time()
    conn.ping()
    latency = time.time() - start
    assert latency < max_latency


NUM_SLOW_CONN_REQS = 50
ACCEPTABLE_LATENCY = 0.5


@pytest.mark.fast
def test_slow_client(server):
    domain_addr = server.random_domain().random_addr()
    slow_conn = client.connect(domain_addr)

    fast_conn = client.connect(domain_addr)

    for i in range(NUM_SERVICES):
        fast_conn.publish(i, 0, {}, 42)

    replies = []

    def cb(ta_id, *args):
        replies.append(None)

    # try to hog the server with a slow client only issuing new requests,
    # never consuming any responses
    for i in range(NUM_SLOW_CONN_REQS):
        slow_conn.services(response_cb=cb)

    acceptable_latency = cpu_speed.adjust_latency(ACCEPTABLE_LATENCY)
    assure_ping(fast_conn, acceptable_latency)

    # make sure to fill up the server's socket buffer facing the slow client
    deadline = time.time() + 0.25
    while time.time() < deadline:
        slow_conn.try_send()

    assure_ping(fast_conn, acceptable_latency)

    assert len(replies) == 0

    expected_responses = (NUM_SERVICES + 2) * NUM_SLOW_CONN_REQS

    wait(slow_conn, criteria=lambda: len(replies) == expected_responses)

    slow_conn.close()
    fast_conn.close()


class PingProcess(spawn_mp.Process):
    def __init__(self, domain_addr, queue, duration):
        spawn_mp.Process.__init__(self)
        self.domain_addr = domain_addr
        self.queue = queue
        self.duration = duration

    def run(self):
        try:
            deadline = time.time() + self.duration

            start = time.time()
            conn = client.connect(self.domain_addr)
            highest_latency = time.time() - start

            while time.time() < deadline:
                latency = conn.ping()
                if latency > highest_latency:
                    highest_latency = latency
                time.sleep(0.1)

            conn.close()

            self.queue.put(highest_latency)

            sys.exit(0)
        except paf.client.Error:
            sys.exit(1)


MANY_SERVICES = 5000
PING_CLIENT_DURATION = 4


@pytest.mark.skip_in_valgrind
def test_large_client_disconnect(server):

    domain_addr = server.random_domain().random_addr()

    pub_conn = client.connect(domain_addr)
    sub_conn = client.connect(domain_addr)

    num_services = cpu_speed.adjust_cardinality_down(MANY_SERVICES)

    for i in range(num_services):
        service_props = {
            "name": {"service-%d" % i},
            "value": {0}
        }
        service_ttl = int(PING_CLIENT_DURATION/2) + random.randint(-1, 1)
        pub_conn.publish(pub_conn.service_id(), 0, service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = sub_conn.subscribe(sub_conn.subscription_id(),
                               subscription_recorder)
    subscription_recorder.ta_id = ta_id

    q = spawn_mp.Queue()
    p = PingProcess(domain_addr, q, PING_CLIENT_DURATION)
    p.start()

    time.sleep(0.25)

    pub_conn.close()

    highest_latency = None
    while highest_latency is None:
        wait(sub_conn, timeout=0.25)
        try:
            highest_latency = q.get_nowait()
        except queue.Empty:
            pass

    p.join()

    sub_conn.close()

    assert highest_latency < cpu_speed.adjust_latency(ACCEPTABLE_LATENCY)


class ConsumerResult(Enum):
    CONNECT_FAILED = 0
    RESOURCE_ALLOCATION_FAILED = 1
    SUCCESS = 3


class ResourceType(Enum):
    CLIENT = 0
    SERVICE = 1
    SUBSCRIPTION = 2


CONNECT_TIMEOUT = 0.25


class ConsumerProcess(spawn_mp.Process):
    def __init__(self, domain_addr, tls_cert, resource_type, resource_count,
                 result_queue):
        spawn_mp.Process.__init__(self)
        self.domain_addr = domain_addr
        self.tls_cert = tls_cert
        self.resource_type = resource_type
        self.resource_count = resource_count
        self.result_queue = result_queue
        self.stop = False
        self.service_ids = []
        self.conn = None
        self.ready = False

    def handle_term(self, signo, stack):
        self.stop = True

    def make_ready(self):
        self.ready = True

    def connect(self):
        try:
            self.conn = \
                client.connect(self.domain_addr, ready_cb=self.make_ready,
                               track=True)
            wait(self.conn, lambda: self.ready, CONNECT_TIMEOUT)
        except proto.Error:
            pass
        if not self.ready:
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            self.result_queue.put(ConsumerResult.CONNECT_FAILED)
        else:
            self.conn.ping()

    def allocate_resource(self):
        try:
            if self.resource_type == ResourceType.SERVICE:
                for i in range(self.resource_count):
                    service_id = self.conn.service_id()
                    generation = 0
                    service_props = {"name": {"service-%d" % service_id}}
                    service_ttl = 1
                    self.conn.publish(service_id, generation, service_props,
                                      service_ttl)
                    self.service_ids.append(service_id)
                self.result_queue.put(ConsumerResult.SUCCESS)
            elif self.resource_type == ResourceType.SUBSCRIPTION:
                result = ConsumerResult.SUCCESS
                for i in range(self.resource_count):
                    sub_id = self.conn.subscription_id()
                    recorder = MultiResponseRecorder()
                    ta_id = self.conn.subscribe(sub_id, recorder)
                    recorder.ta_id = ta_id
                    wait(self.conn, criteria=lambda:
                         recorder.state != TransactionState.REQUESTING)
                    if recorder.failed():
                        result = ConsumerResult.RESOURCE_ALLOCATION_FAILED
                self.result_queue.put(result)
            elif self.resource_type == ResourceType.CLIENT:
                assert self.resource_count == 1
                self.result_queue.put(ConsumerResult.SUCCESS)
        except client.Error:
            self.result_queue.put(ConsumerResult.RESOURCE_ALLOCATION_FAILED)

    def deallocate_resource(self):
        for service_id in self.service_ids:
            self.conn.unpublish(service_id)

    def run(self):
        if self.tls_cert is not None:
            os.environ['XCM_TLS_CERT'] = self.tls_cert
        random.seed(time.time() + os.getpid())
        signal.signal(signal.SIGTERM, self.handle_term)

        self.connect()
        if self.conn is not None:
            self.allocate_resource()
            wait(self.conn, criteria=lambda: self.stop)
            self.deallocate_resource()
            self.conn.close()
        sys.exit(0)


def spawn_consumer(domain_addr, tls_cert, resource_type, max_attempts,
                   result, result_queue):
    if resource_type == ResourceType.CLIENT:
        num = 1
    else:
        num = random.randint(1, max_attempts)

    consumer = ConsumerProcess(domain_addr, tls_cert, resource_type, num,
                               result_queue)
    consumer.start()

    assert result == result_queue.get()

    return (consumer, num)


def spawn_consumers(domain_addr, tls_cert, resource_type, limit,
                    failure_result):
    result_queue = spawn_mp.Queue()
    consumers = []
    attempts = 0
    while attempts < limit:
        left = limit - attempts
        consumer, num = \
            spawn_consumer(domain_addr, tls_cert, resource_type, left,
                           ConsumerResult.SUCCESS, result_queue)
        consumers.append(consumer)
        attempts += num

    consumer, num = \
        spawn_consumer(domain_addr, tls_cert, resource_type, 1, failure_result,
                       result_queue)
    consumers.append(consumer)

    return consumers


def run_resource_limit(domain_addr, resource_type, user_limit, total_limit,
                       failure_result):

    consumers = []

    # verify per-user limit
    user0 = spawn_consumers(domain_addr, CLIENT_CERTS[1], resource_type,
                            user_limit, failure_result)
    consumers.extend(user0)

    conn = client.connect(domain_addr)
    if resource_type == ResourceType.CLIENT:
        assert len(conn.clients()) == (user_limit + 1)
    elif resource_type == ResourceType.SERVICE:
        assert len(conn.services()) == user_limit
    elif resource_type == ResourceType.SUBSCRIPTION:
        assert len(conn.subscriptions()) == user_limit
    conn.close()

    total_left = total_limit - user_limit
    # For simplicity, the resource limits for the pafd-under-test is
    # configured so that two users may together exceed the total
    # limit.
    assert total_left < user_limit

    # verify total limits
    user1 = spawn_consumers(domain_addr, CLIENT_CERTS[2], resource_type,
                            total_left, failure_result)
    consumers.extend(user1)

    for consumer in consumers:
        consumer.terminate()
        consumer.join()

    conn = client.connect(domain_addr)
    assert len(conn.clients()) == 1
    conn.close()

    # XXX: make sure user0's resources are free'd


@pytest.mark.fast
@pytest.mark.require_resource_limits
def test_max_clients(limited_clients_server):
    domain_addr = limited_clients_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.CLIENT, MAX_USER_CLIENTS,
                       MAX_TOTAL_CLIENTS, ConsumerResult.CONNECT_FAILED)


@pytest.mark.fast
@pytest.mark.require_resource_limits
def test_max_services(limited_services_server):
    domain_addr = limited_services_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.SERVICE, MAX_USER_SERVICES,
                       MAX_TOTAL_SERVICES,
                       ConsumerResult.RESOURCE_ALLOCATION_FAILED)


@pytest.mark.fast
@pytest.mark.require_resource_limits
def test_max_subscriptions(limited_subscriptions_server):
    domain_addr = limited_subscriptions_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.SUBSCRIPTION,
                       MAX_USER_SUBSCRIPTIONS, MAX_TOTAL_SUBSCRIPTIONS,
                       ConsumerResult.RESOURCE_ALLOCATION_FAILED)


@pytest.mark.fast
@pytest.mark.require_resource_limits
def test_default_user_max_services(limited_services_server):
    # use UX to get classified as default user
    domain_ux_addr = limited_services_server.domains[1].default_addr()
    consumers = \
        spawn_consumers(domain_ux_addr, None, ResourceType.SERVICE,
                        MAX_USER_SERVICES,
                        ConsumerResult.RESOURCE_ALLOCATION_FAILED)

    # should also be default user -> no resources available
    conn = client.connect(domain_ux_addr)
    with pytest.raises(client.Error, match=".*resources.*"):
        conn.publish(conn.service_id(), 0, {}, 1)

    for consumer in consumers:
        consumer.terminate()
        consumer.join()

    conn.publish(conn.service_id(), 0, {}, 1)
    conn.close()


@pytest.mark.fast
def test_tcp_dos(tls_server):
    domain_addr = tls_server.default_domain().default_addr()

    conns = []
    while len(conns) < tls_server.max_or_many_clients():
        try:
            conn = xcm.connect(domain_addr, 0)
            conns.append(conn)
        except xcm.error:
            time.sleep(0.1)

    time.sleep(3)

    conn = client.connect(domain_addr)
    conn.close()

    for conn in conns:
        conn.close()


def run_handshake_test(tls_addr, finish_tls):
    if finish_tls:
        addr = tls_addr
    else:
        addr = tls_addr.replace("tls", "tcp", 1)

    conn = xcm.connect(addr, 0)

    start = time.time()
    msg = conn.receive()
    latency = time.time() - start

    assert len(msg) == 0  # connection closed
    assert latency > 2 and latency < 4


def test_drop_client_failing_tls_handshake(tls_server):
    domain_addr = tls_server.default_domain().default_addr()

    run_handshake_test(domain_addr, False)


def test_drop_client_failing_pathfinder_handshake(tls_server):
    domain_addr = tls_server.default_domain().default_addr()

    run_handshake_test(domain_addr, True)


@pytest.mark.fast
def test_unsupported_protocol_version(server):
    conn = xcm.connect(server.random_domain().random_addr(), 0)
    hello = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 4711,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.MAX_VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.MAX_VERSION+2
    }
    conn.send(json.dumps(hello).encode('utf-8'))
    expected_response = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_FAIL,
        proto.FIELD_FAIL_REASON.name:
        proto.FAIL_REASON_UNSUPPORTED_PROTOCOL_VERSION
    }
    in_msg = conn.receive()
    assert len(in_msg) > 0
    actual_response = json.loads(in_msg.decode('utf-8'))
    assert actual_response == expected_response
    conn.close()


def run_leak_clients(domain_addr, num):
    ready_queue = spawn_mp.Queue()
    processes = []
    for i in range(num):
        unpublish = bool(random.getrandbits(1))
        unsubscribe = bool(random.getrandbits(1))
        p = ClientProcess(domain_addr, ready_queue, unpublish=unpublish,
                          unsubscribe=unsubscribe)
        p.start()
        processes.append(p)
    for p in processes:
        ready_queue.get()
    for p in processes:
        p.terminate()
    for p in processes:
        p.join()


def get_rss(pid):
    with open("/proc/%d/status" % pid) as f:
        for line in f:
            e = line.split(":")
            if len(e) == 2 and e[0] == 'VmRSS':
                return int(e[1].replace(" kB", ""))


def exercise_server(domain_addr):
    # Connect on XCM-level only
    for i in range(250):
        while True:
            try:
                conn = xcm.connect(domain_addr, 0)
                conn.send("foo")
                conn.close()
                break
            except xcm.error:
                pass

    # Connect on Pathfinder protocol level
    for i in range(250):
        while True:
            try:
                conn = client.connect(domain_addr)
                conn.close()
                break
            except proto.TransportError:
                pass

    # Spawn off many clients concurrently subscribing and publishing
    for i in range(50):
        run_leak_clients(domain_addr, 5)

    conn = client.connect(domain_addr)
    time.sleep(CLIENT_PROCESS_TTL)
    while len(conn.services()) > 0:
        time.sleep(0.1)
    conn.close()


ALLOWED_RETRIES = 4


@pytest.mark.skip_in_valgrind
def test_server_leak(tls_server):
    domain_addr = tls_server.default_domain().default_addr()

    # warm up
    exercise_server(domain_addr)
    rss = get_rss(tls_server.process.pid)

    # This test case attempts to detect memory leaks by looking for
    # continuously growing server process resident set size
    # (RSS). This method must include a bit of heuristics, since
    # there's nondeterminism in terms of things like Python GC and
    # heap fragmentation. Thus, we allow RSS to "sometimes" grow.

    for i in range(ALLOWED_RETRIES + 1):
        initial_rss = rss
        exercise_server(domain_addr)
        rss = get_rss(tls_server.process.pid)
        if rss <= initial_rss:
            break

    assert rss <= initial_rss


def xcm_has_uxf():
    try:
        s = xcm.server("uxf:%s" % random_name())
        s.close()
        return True
    except xcm.error:
        return False


@pytest.mark.fast
@pytest.mark.require_hook
def test_daemon_hook(request):
    server = server_by_request(request)

    with open("hook.py", "w+") as f:
        f.write("""
def run(servers):
    open("hook.tmp", "w+").write("%d" % len(servers))
""")

    server.configure_random_domain(1)
    server.hook = "hook.run"
    server.start(python_path=os.getcwd())
    time.sleep(1)
    server.stop()

    assert int(open("hook.tmp").read()) == 1

    os.remove("hook.py")
    os.remove("hook.tmp")


@pytest.mark.fast
def test_handle_signals(request):
    if not xcm_has_uxf():
        return
    try:
        domain_name = random_name()
        domain_uxf_file = random_name()
        domain_addr = "uxf:%s" % domain_uxf_file

        assert not os.path.exists(domain_uxf_file)

        server = server_by_request(request)

        server.configure_domain(domain_name, domain_addr)

        server.start()
        server.stop(signal.SIGHUP)
        assert not os.path.exists(domain_uxf_file)

        server.start()
        server.stop(signal.SIGTERM)
        assert not os.path.exists(domain_uxf_file)

        server.start()
        server.stop(signal.SIGKILL)
        assert os.path.exists(domain_uxf_file)
    finally:
        try:
            os.remove(domain_uxf_file)
        except OSError:
            pass


def test_version_consistency():
    setup = subprocess.Popen(["/usr/bin/python3", "./setup.py", "--version"],
                             cwd="../", stdout=subprocess.PIPE)
    setup.wait()

    setup_version = setup.stdout.read().decode('utf-8').rstrip()

    assert setup_version == paf.server.VERSION
