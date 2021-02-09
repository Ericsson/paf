# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

#
# Integration test suite for Pathfinder Server
#

import pytest
import os
import fcntl
import sys
import time
import subprocess
import select
from enum import Enum
import json
import random
import string
import signal
import threading
import collections
import multiprocessing
import yaml

import paf.client as client
import paf.proto as proto
import paf.xcm as xcm
import paf.server

MAX_CLIENTS = 250

SERVER_DEBUG = False

SERVER_CERT = 'cert/cert-server'

NUM_CLIENT_CERTS = 3
CLIENT_CERTS = ["cert/cert-client%d" % n for n in range(NUM_CLIENT_CERTS)]

os.environ['XCM_TLS_CERT'] = CLIENT_CERTS[0]

random.seed()


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
    def __init__(self, name, addrs):
        self.name = name
        self.addrs = addrs
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


class Server:
    def __init__(self):
        self.domains = []
        self.process = None
        self.use_config_file = False
        self.resources = None
        self.hook = None

        os.environ['PAF_DOMAINS'] = DOMAINS_DIR
        os.system("mkdir -p %s" % DOMAINS_DIR)

    def random_domain(self):
        return random.choice(self.domains)

    def default_domain(self):
        return self.domains[0]

    def configure_domain(self, name, addrs):
        if isinstance(addrs, str):
            addrs = [addrs]
        domain = Domain(name, addrs)
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

    def configure_random_domain(self, num_addrs, addr_fun=random_addr):
        name = self.get_random_name()
        addrs = []
        while (len(addrs) < num_addrs):
            addr = addr_fun()
            if not self.is_addr_used(addr):
                addrs.append(addr)
        return self.configure_domain(name, addrs)

    def set_resources(self, resources):
        self.resources = resources
        self.use_config_file = True

    def _write_config_file(self):
        conf = {}
        if SERVER_DEBUG:
            log_conf = {}
            log_conf["filter"] = "debug"
            conf["log"] = log_conf
        domains_conf = []
        for domain in self.domains:
            domains_conf.append({"addrs": domain.addrs})
        conf["domains"] = domains_conf
        if self.resources is not None:
            conf["resources"] = self.resources
        with open(CONFIG_FILE, 'w') as file:
            yaml.dump(conf, file)

    def _cmd(self):
        cmd = ["pafd"]
        if self.hook is not None:
            cmd.extend(["-r", self.hook])
        if self.use_config_file:
            self._write_config_file()
            cmd.extend(["-f", CONFIG_FILE])
        else:
            cmd.extend(["-c", str(MAX_CLIENTS)])
            if SERVER_DEBUG:
                cmd.extend(["-l", "debug"])
            for domain in self.domains:
                cmd.extend(["-m", "%s" % "+".join(domain.addrs)])
        return cmd

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
        cmd = self._cmd()
        pafd_env = os.environ.copy()
        pafd_env['XCM_TLS_CERT'] = SERVER_CERT
        if python_path is not None:
            pafd_env['PYTHONPATH'] = "%s:%s" % \
                (python_path, pafd_env['PYTHONPATH'])
        self.process = subprocess.Popen(cmd, env=pafd_env)
        self._assure_up()

    def stop(self, signo=signal.SIGTERM):
        if self.process is None:
            return
        self.process.send_signal(signo)
        self.process.wait()
        if self.use_config_file:
            os.remove(CONFIG_FILE)
        self.process = None


def random_server(min_domains, max_domains, min_addrs_per_domain,
                  max_addrs_per_domain):
    server = Server()
    num_domains = random.randint(min_domains, max_domains)
    for i in range(num_domains):
        num_addrs = random.randint(min_addrs_per_domain, max_addrs_per_domain)
        server.configure_random_domain(num_addrs)
    server.start()
    return server


@pytest.yield_fixture(scope='function')
def server():
    server = random_server(1, 4, 1, 4)
    yield server
    server.stop()


@pytest.yield_fixture(scope='function')
def md_server():
    server = random_server(8, 16, 1, 4)
    yield server
    server.stop()


@pytest.yield_fixture(scope='function')
def ms_server():
    server = random_server(1, 4, 16, 32)
    yield server
    server.stop()


@pytest.yield_fixture(scope='function')
def tls_server():
    server = Server()
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


def limited_server(resources):
    server = Server()
    server.configure_random_domain(1, addr_fun=random_tls_addr)
    server.configure_random_domain(1, addr_fun=random_ux_addr)
    server.set_resources(resources)
    server.start()
    return server


@pytest.yield_fixture(scope='function')
def limited_clients_server():
    server = limited_server({
        "user": {"clients": MAX_USER_CLIENTS},
        "total": {"clients": MAX_TOTAL_CLIENTS}
    })
    yield server
    server.stop()


@pytest.yield_fixture(scope='function')
def limited_services_server():
    server = limited_server({
        "user": {"services": MAX_USER_SERVICES},
        "total": {"services": MAX_TOTAL_SERVICES}
    })
    yield server
    server.stop()


@pytest.yield_fixture(scope='function')
def limited_subscriptions_server():
    server = limited_server({
        "user": {"subscriptions": MAX_USER_SUBSCRIPTIONS},
        "total": {"subscriptions": MAX_TOTAL_SUBSCRIPTIONS}
    })
    yield server
    server.stop()


def set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def wait(conn, criteria=lambda: False, timeout=None):
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
        poll.register(conn.fileno(), select.EPOLLIN)
        while not criteria():
            if timeout is not None:
                time_left = deadline - time.time()
                if time_left <= 0:
                    break
            else:
                time_left = None
            poll.poll(time_left)
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
    conn = client.connect(server.random_domain().random_addr())
    assert conn.proto_version == proto.VERSION


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
    wait(conn_sub, criteria=subscription_recorder.accepted)

    wait(conn_sub, criteria=lambda:
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

    wait(conn_sub, criteria=lambda:
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

    wait(conn_sub, timeout=0.1)

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
def test_republish_same_generation_orphan_from_same_client(server):
    domain_addr = server.random_domain().random_addr()
    client_id = client.allocate_client_id()
    run_republish_orphan(domain_addr, new_generation=False,
                         reused_client_id=client_id)


@pytest.mark.fast
def test_republish_new_generation_orphan_from_different_client(server):
    domain_addr = server.random_domain().random_addr()
    run_republish_orphan(domain_addr, new_generation=True)


@pytest.mark.fast
def test_republish_same_generation_orphan_from_different_client(server):
    domain_addr = server.random_domain().random_addr()
    run_republish_orphan(domain_addr, new_generation=False)


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

    conn.publish(service_id, service_generation, service_props, service_ttl)
    conn.publish(service_id, service_generation, {}, service_ttl)

    wait(conn, timeout=0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.close()


@pytest.mark.fast
def test_republish_same_and_older_generation(server):
    conn = client.connect(server.random_domain().random_addr())

    service_id = conn.service_id()
    service_generations = [10, 11]
    service_props = {"name": {"foo"}}
    service_ttl = 42

    conn.publish(service_id, service_generations[1], service_props,
                 service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria=subscription_recorder.accepted)
    wait(conn, timeout=0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.publish(service_id, service_generations[1], service_props,
                 service_ttl)

    assert subscription_recorder.count_notifications() == 1

    reason = proto.FAIL_REASON_OLD_GENERATION
    with pytest.raises(client.TransactionError, match=".*%s.*" % reason):
        service_props = {}
        conn.publish(service_id, service_generations[0],
                     service_props, service_ttl)

    assert subscription_recorder.count_notifications() == 1

    conn.close()


@pytest.mark.fast
def test_unpublish_from_different_client_same_user(server):
    domain_addr = server.random_domain().random_addr()
    conn0 = client.connect(domain_addr)

    publish_recorder = SingleResponseRecorder()
    service_id = conn0.service_id()
    ta_id = conn0.publish(service_id, 0, {"name": {"service-x"}},
                          42, publish_recorder)
    publish_recorder.ta_id = ta_id
    wait(conn0, criteria=publish_recorder.completed)

    conn1 = client.connect(domain_addr)

    conn1.unpublish(service_id)

    delayed_close(conn0)
    delayed_close(conn1)


@pytest.mark.fast
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


def by_id(l):
    return l[0]


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


class ClientProcess(multiprocessing.Process):
    def __init__(self, domain_addr, ready_queue, unpublish=True,
                 unsubscribe=True):
        multiprocessing.Process.__init__(self)
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
                conn = client.connect(self.domain_addr)
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

    num_clients = MAX_CLIENTS-1
    ready_queue = multiprocessing.Queue()
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
                               client_id=reused_client_id)

    conn_pub1.publish(service_id, service_generation,
                      service_props, service_ttl)

    wait(conn_sub, criteria=lambda:
         subscription_recorder.count_notifications() >= 3)

    # wait for any (errornous!) timeout to happen
    wait(conn_pub1, timeout=2)

    wait(conn_sub, timeout=0.1)

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
        proto.FIELD_PROTO_MIN_VERSION.name: proto.VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.VERSION+2
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(too_large_ta_id).encode('utf-8'),
                           skip_hello=True)

    too_large_client_id = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 99,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 1 << 99,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.VERSION+2
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
        proto.FIELD_PROTO_MIN_VERSION.name: proto.VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.VERSION+2
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
    while len(conns) < MAX_CLIENTS:
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


@pytest.mark.fast
def test_list_clients(server):
    domain = server.random_domain()
    conn = client.connect(domain.default_addr())

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

    assure_ping(fast_conn, ACCEPTABLE_LATENCY)

    # make sure to fill up the server's socket buffer facing the slow client
    deadline = time.time() + 0.25
    while time.time() < deadline:
        slow_conn.try_send()

    assure_ping(fast_conn, ACCEPTABLE_LATENCY)

    assert len(replies) == 0

    expected_responses = (NUM_SERVICES + 2) * NUM_SLOW_CONN_REQS

    wait(slow_conn, criteria=lambda: len(replies) == expected_responses)

    slow_conn.close()
    fast_conn.close()


class ConsumerResult(Enum):
    CONNECT_FAILED = 0
    RESOURCE_ALLOCATION_FAILED = 1
    SUCCESS = 3


class ResourceType(Enum):
    CLIENT = 0
    SERVICE = 1
    SUBSCRIPTION = 2


CONNECT_TIMEOUT = 0.25


class ConsumerProcess(multiprocessing.Process):
    def __init__(self, domain_addr, tls_cert, resource_type, resource_count,
                 result_queue):
        multiprocessing.Process.__init__(self)
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
                client.connect(self.domain_addr, ready_cb=self.make_ready)
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
    result_queue = multiprocessing.Queue()
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
def test_max_clients(limited_clients_server):
    domain_addr = limited_clients_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.CLIENT, MAX_USER_CLIENTS,
                       MAX_TOTAL_CLIENTS, ConsumerResult.CONNECT_FAILED)


@pytest.mark.fast
def test_max_services(limited_services_server):
    domain_addr = limited_services_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.SERVICE, MAX_USER_SERVICES,
                       MAX_TOTAL_SERVICES,
                       ConsumerResult.RESOURCE_ALLOCATION_FAILED)


@pytest.mark.fast
def test_max_subscriptions(limited_subscriptions_server):
    domain_addr = limited_subscriptions_server.default_domain().default_addr()
    run_resource_limit(domain_addr, ResourceType.SUBSCRIPTION,
                       MAX_USER_SUBSCRIPTIONS, MAX_TOTAL_SUBSCRIPTIONS,
                       ConsumerResult.RESOURCE_ALLOCATION_FAILED)


@pytest.mark.fast
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
    while len(conns) < MAX_CLIENTS:
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


@pytest.mark.fast
def test_unsupported_protocol_version(server):
    conn = xcm.connect(server.random_domain().random_addr(), 0)
    hello = {
        proto.FIELD_TA_CMD.name: proto.CMD_HELLO,
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST,
        proto.FIELD_CLIENT_ID.name: 4711,
        proto.FIELD_PROTO_MIN_VERSION.name: proto.VERSION+1,
        proto.FIELD_PROTO_MAX_VERSION.name: proto.VERSION+2
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
    ready_queue = multiprocessing.Queue()
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
def test_daemon_hook():
    open("hook.py", "w+").write("""
def run(servers):
    open("hook.tmp", "w+").write("%d" % len(servers))
""")
    server = Server()
    server.configure_random_domain(1)
    server.hook = "hook.run"
    server.start(python_path=os.getcwd())
    time.sleep(1)
    server.stop()

    assert int(open("hook.tmp").read()) == 1

    os.remove("hook.py")
    os.remove("hook.tmp")


@pytest.mark.fast
def test_handle_signals():
    if not xcm_has_uxf():
        return
    try:
        domain_name = random_name()
        domain_uxf_file = random_name()
        domain_addr = "uxf:%s" % domain_uxf_file

        assert not os.path.exists(domain_uxf_file)

        server = Server()

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
