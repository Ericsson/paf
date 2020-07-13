#
# Integration test suite for Pathfinder Server
#

import pytest
import os
import fcntl
import errno
import logging
import sys
import time
import subprocess
import select
from enum import Enum
import json
import random
import tempfile
import string
import signal
import threading
import collections
import multiprocessing

import paf.client as client
import paf.proto as proto
import paf.xcm as xcm

from logging.handlers import MemoryHandler

MAX_CLIENTS = 250

SERVER_DEBUG = False

SERVER_CERT = 'cert/cert-server'

CLIENT_CERT = 'cert/cert-client'

os.environ['XCM_TLS_CERT'] = CLIENT_CERT

random.seed()

def random_name():
    len = random.randint(1, 32)
    name = ""
    while len > 0:
        name += random.choice(string.ascii_lowercase)
        len -= 1
    return name

def random_ux_addr():
    return "ux:%s" % random_name()

def random_port():
    return random.randint(2000, 32000)

def random_tcp_addr():
    return "tcp:127.0.0.1:%d" % random_port()

def random_tls_addr():
    return "tls:127.0.0.1:%d" % random_port()

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

class Domain:
    def __init__(self, name, addrs):
        self.name = name
        self.addrs = addrs
        self.server_process = None
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
    def configure_random_domain(self, num_addrs):
        name = random_name()
        addrs = []
        while (len(addrs) < num_addrs):
            addr = random_addr()
            if not self.is_addr_used(addr):
                addrs.append(addr)
        return self.configure_domain(name, addrs)
    def start(self):
        if self.process != None:
            return
        cmd = [ "pafd", "-c", str(MAX_CLIENTS) ]
        if SERVER_DEBUG:
            cmd.extend(["-l", "debug", "-s"])

        for domain in self.domains:
            cmd.extend(["-m", "%s" % "+".join(domain.addrs)])

        pafd_env = os.environ.copy()
        pafd_env['XCM_TLS_CERT'] = SERVER_CERT
        self.process = subprocess.Popen(cmd, env = pafd_env)

        time.sleep(0.25)
    def stop(self, signo = signal.SIGTERM):
        if self.process == None:
            return
        self.process.send_signal(signo)
        self.process = None
        time.sleep(0.1)

def random_server(min_domains, max_domains, min_addrs_per_domain,
                  max_addrs_per_domain):
    server = Server()
    num_domains = random.randint(min_domains, max_domains)
    for i in range(0, num_domains):
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
    server.configure_domain(random_name(), random_tls_addr())
    server.start()
    return server

def set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

def wait(conn, criteria = lambda: False, timeout = None):
    rfd = None
    wfd = None
    old_wakeup_fd = None
    try:
        rfd, wfd = os.pipe()
        set_nonblocking(rfd)
        set_nonblocking(wfd)
        old_wakeup_fd = signal.set_wakeup_fd(wfd)
        if timeout != None:
            deadline = time.time() + timeout
        while not criteria():
            if timeout != None:
                time_left = deadline - time.time()
                if time_left <= 0:
                    break
            else:
                time_left = None

            client_fds, client_events = conn.want()
            if len(client_fds) > 0:
                poll = select.poll()
                client.populate(poll, client_fds, client_events)
                poll.register(rfd, select.EPOLLIN)
                try: # only needed in Python 2
                    poll.poll(time_left)
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise e

            conn.process()
    finally:
        if rfd != None:
            os.close(rfd)
        if wfd != None:
            os.close(wfd)
        if old_wakeup_fd != None:
            signal.set_wakeup_fd(-1)

def delayed_close(conn):
    # always wait a little, to allow trailing messages to arrive, which
    # might sent in error, and thus should be detected
    wait(conn, timeout = 0.01)
    conn.close()

class Recorder:
    def __init__(self):
        self.replies = []
        self.ta_id = None
    def __call__(self, ta_id, event, *args, **optargs):
        assert self.ta_id != None
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

def test_hello(server):
    conn = client.connect(server.random_domain().random_addr())
    proto_version = conn.hello()[0]
    assert proto_version == proto.VERSION

NUM_SERVICES = 1000
def test_batch_publish(server):
    conn = client.connect(server.random_domain().random_addr())

    service_ids = set()
    ta_ids = set()
    publish_recorders = []
    for i in range(0, NUM_SERVICES):
        publish_recorder = SingleResponseRecorder()
        service_id = conn.service_id()
        service_ids.add(service_id)
        ta_id = conn.publish(service_id, 0, {"name": { "service-a" } }, 42,
                             publish_recorder)
        publish_recorder.ta_id = ta_id
        assert not ta_id in ta_ids
        ta_ids.add(ta_id)
        publish_recorders.append(publish_recorder)

    for recorder in publish_recorders:
        wait(conn, criteria = recorder.completed)

    delayed_close(conn)

def test_republish_from_new_client(server):
    domain_addr = server.random_domain().random_addr()
    conn_pub0 = client.connect(domain_addr)

    service_id = conn_pub0.service_id()
    first_generation = 1
    service_props = {
        "name": { "service-x" },
        "value": { 0 }
    }
    service_ttl = 42

    conn_pub0.publish(service_id, first_generation, service_props, service_ttl)

    conn_sub = client.connect(domain_addr)
    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(conn_sub.subscription_id(),
                               subscription_recorder)
    subscription_recorder.ta_id = ta_id
    wait(conn_sub, criteria = subscription_recorder.accepted)

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() >= 1)
    assert subscription_recorder.count_notifications() == 1

    second_generation = first_generation + 17
    conn_pub1 = client.connect(domain_addr)
    conn_pub1.publish(service_id, second_generation, service_props, service_ttl)

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() >= 2)

    assert subscription_recorder.get_notifications() == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'service_props': service_props, 'generation': first_generation,
           'ttl': service_ttl, 'client_id': conn_pub0.client_id }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         { 'generation': second_generation,
           'service_props': service_props, 'ttl': service_ttl,
           'client_id': conn_pub1.client_id })
    ]

    conn_pub0.close()

    wait(conn_sub, timeout = 0.1)

    assert subscription_recorder.count_notifications() == 2

    conn_pub1.close()
    conn_sub.close()

def test_unpublish_nonexisting_service(server):
    conn = client.connect(server.random_domain().random_addr())

    nonexisting_service_id = 4711
    reason = proto.FAIL_REASON_NON_EXISTENT_SERVICE_ID
    with pytest.raises(client.TransactionError, match=".*%s.*" % reason):
        conn.unpublish(nonexisting_service_id)

    delayed_close(conn)

def test_unpublish_from_non_owner(server):
    domain_addr = server.random_domain().random_addr()
    conn0 = client.connect(domain_addr)

    publish_recorder = SingleResponseRecorder()
    service_id = conn0.service_id()
    ta_id = conn0.publish(service_id, 0, { "name": { "service-x" }},
                          42, publish_recorder)
    publish_recorder.ta_id = ta_id
    wait(conn0, criteria = publish_recorder.completed)

    conn1 = client.connect(domain_addr)

    conn1.unpublish(service_id)

    delayed_close(conn0)
    delayed_close(conn1)

def test_publish_and_unpublish_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder,
                           filter='(&(name=service-a)(area=51))')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)

    m_service_props = {
        "name" : { "service-a" },
        "address": { "tls:10.10.10.10:1010" },
        "area" : { 51 }
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

    wait(conn, criteria = \
         lambda: subscription_recorder.count_notifications() >= 2)

    notifications = subscription_recorder.get_notifications()
    assert len(notifications) == 2
    assert notifications[1] == (client.EventType.NOTIFY,
                                client.MATCH_TYPE_DISAPPEARED,
                                m_service_id)

    conn.publish(conn.service_id(), 0, {
        "name" : { "non-matching-name" },
        "area": { 51 }
    }, 99, lambda *args: None)
    conn.publish(conn.service_id(), 0, {
        "name": { "service-a" },
        "area" : { 42 }
    }, 99, lambda *args: None)

    wait(conn, timeout = 0.5)

    assert subscription_recorder.count_notifications() == 2

    conn.close()

def test_ttl_change_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(conn.subscription_id(), subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)

    service_id = conn.service_id()
    service_props = { 'name': { 'a b c', 'd e f' } }
    service_first_ttl = 4711
    first_generation = 1

    conn.publish(service_id, first_generation,
                 service_props, service_first_ttl)

    second_generation = first_generation + 1
    service_second_ttl = service_first_ttl * 2
    conn.publish(service_id, second_generation,
                 service_props, service_second_ttl)

    wait(conn, criteria = \
         lambda: subscription_recorder.count_notifications() >= 2)

    assert subscription_recorder.get_notifications() == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'generation': first_generation, 'service_props': service_props,
           'ttl': service_first_ttl, 'client_id': conn.client_id }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         { 'generation': second_generation, 'service_props': service_props,
           'ttl': service_second_ttl, 'client_id': conn.client_id })
    ]

    conn.close()

def test_subscribe_to_existing_service(server):
    conn = client.connect(server.random_domain().random_addr())

    service_generation = 10
    service_props = {
        "name": { "service-x" },
        "key": { "value" },
        "another_key" : { "the_same_value" }
    }
    service_ttl = 99

    service_id = conn.service_id()
    conn.publish(service_id, service_generation, service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(99, subscription_recorder,
                           filter='(name=service-x)')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = \
         lambda: subscription_recorder.count_notifications() > 0)
    wait(conn, timeout = 0.1)

    notifications = subscription_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn.client_id })
    ]

    assert len(conn.subscriptions()) == 1
    conn.unsubscribe(99)
    assert len(conn.subscriptions()) == 0

    conn.close()

def test_subscription_id_errornous_reuse(server):
    conn = client.connect(server.random_domain().random_addr())

    sub_id = 99

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.failed)
    assert subscription_recorder.get_fail_reason() == \
        proto.FAIL_REASON_SUBSCRIPTION_ID_EXISTS

    conn.close()

def test_subscription_id_valid_reuse(server):
    conn = client.connect(server.random_domain().random_addr())

    sub_id = 99

    subscribe_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, subscribe_recorder)
    subscribe_recorder.ta_id = ta_id

    wait(conn, criteria = lambda: subscribe_recorder.accepted)

    unsubscribe_recorder = SingleResponseRecorder()
    ta_id = conn.unsubscribe(sub_id, unsubscribe_recorder)
    unsubscribe_recorder.ta_id = ta_id

    wait(conn, criteria = lambda: unsubscribe_recorder.completed and \
         subscribe_recorder.completed)

    resubscribe_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(sub_id, resubscribe_recorder)
    resubscribe_recorder.ta_id = ta_id

    wait(conn, criteria = resubscribe_recorder.accepted)

    conn.close()

def test_subscribe_invalid_syntax_filter(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(99, subscription_recorder,
                           filter='(name=service-x')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.failed)

    assert subscription_recorder.get_fail_reason() == \
        proto.FAIL_REASON_INVALID_FILTER_SYNTAX

    conn.close()

def test_modify_existing_trigger_now_matching_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    service_generations = [2, 5, 1000]

    service_id = conn.service_id()
    service_ttl = 34123122
    conn.publish(service_id, service_generations[0],
                 { "name": { "foo" } }, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder,
                           filter='(&(name=foo)(area=51))')
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)
    wait(conn, timeout = 0.1)

    notifications = subscription_recorder.get_notifications()
    assert len(notifications) == 0

    conn.publish(service_id, service_generations[1],
                 { "name": { "foo" }, "area": { 51 } }, service_ttl)

    conn.publish(service_id, service_generations[2],
                 { "name": { "bar" }, "area": { 51 } }, service_ttl)

    wait(conn, criteria = \
         lambda: subscription_recorder.count_notifications() >= 2)

    notifications = subscription_recorder.get_notifications()
    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'generation': service_generations[1],
           'service_props': { 'name': { 'foo' }, 'area': { 51 } },
           'ttl': service_ttl, 'client_id': conn.client_id }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_DISAPPEARED, service_id)
    ]
    conn.close()

def test_republish_same_generation_doesnt_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    service_id = conn.service_id()
    service_generation = 99
    service_props = { "name": { "foo" } }
    service_ttl = 42

    conn.publish(service_id, service_generation, service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)
    wait(conn, timeout = 0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.publish(service_id, service_generation, service_props, service_ttl)
    conn.publish(service_id, service_generation, {}, service_ttl)

    wait(conn, timeout = 0.1)

    assert subscription_recorder.count_notifications() == 1

    conn.close()

def test_republish_older_generation_doesnt_trigger_subscription(server):
    conn = client.connect(server.random_domain().random_addr())

    service_id = conn.service_id()
    service_generations = [10, 11]
    service_props = { "name": { "foo" } }
    service_ttl = 42

    conn.publish(service_id, service_generations[1], service_props, service_ttl)

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn.subscribe(17, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn, criteria = subscription_recorder.accepted)
    wait(conn, timeout = 0.1)

    assert subscription_recorder.count_notifications() == 1

    service_props = {}
    conn.publish(service_id, service_generations[0],
                 service_props, service_ttl)

    assert subscription_recorder.count_notifications() == 1

    conn.close()

def test_unsubscribe(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()

    sub_id = 17
    subscription_ta_id = conn.subscribe(sub_id, subscription_recorder,
                                        filter='(name=service-x)')
    subscription_recorder.ta_id = subscription_ta_id
    wait(conn, criteria = subscription_recorder.accepted)

    conn.unsubscribe(sub_id)

    wait(conn, criteria = subscription_recorder.completed)

    conn.publish(conn.service_id(), 17,
                 { "name" : { "service-x", "service-y" } }, 42)

    delayed_close(conn)

def test_unsubscribe_nonexisting(server):
    conn = client.connect(server.random_domain().random_addr())

    subscription_recorder = MultiResponseRecorder()

    nonexisting_sub_id = 4711

    unsubscribe_recorder = SingleResponseRecorder()
    unsubscribe_ta_id = conn.unsubscribe(nonexisting_sub_id,
                                         unsubscribe_recorder)
    unsubscribe_recorder.ta_id = unsubscribe_ta_id

    wait(conn, criteria = unsubscribe_recorder.failed)
    assert unsubscribe_recorder.get_fail_reason() == \
        proto.FAIL_REASON_NON_EXISTENT_SUBSCRIPTION_ID

    delayed_close(conn)

def test_unsubscribe_from_non_owner(server):
    domain = server.random_domain()
    conn0 = client.connect(domain.default_addr())

    subscription_recorder = MultiResponseRecorder()

    sub_id = 99
    service_name = "service-x"
    subscription_ta_id = conn0.subscribe(sub_id, subscription_recorder,
                                         filter='(name=service-x)')
    subscription_recorder.ta_id = subscription_ta_id
    wait(conn0, criteria = subscription_recorder.accepted)

    conn1 = client.connect(domain.default_addr())

    unsubscribe_recorder = SingleResponseRecorder()
    unsubscribe_ta_id = conn1.unsubscribe(sub_id, unsubscribe_recorder)
    unsubscribe_recorder.ta_id = unsubscribe_ta_id

    wait(conn1, criteria = unsubscribe_recorder.failed)
    assert unsubscribe_recorder.get_fail_reason() == \
        proto.FAIL_REASON_NOT_SUBSCRIPTION_OWNER

    delayed_close(conn0)
    delayed_close(conn1)

def by_id(l):
    return l[0]

NUM_CLIENTS = 10
def test_list_subscriptions(server):
    conns = []
    subscriptions = []
    domain = server.random_domain()
    for i in range(0, NUM_CLIENTS):
        conn = client.connect(domain.default_addr())

        filter = "(&(name=service-%d)(prop=%d))" % (i, i)

        sub_id = conn.subscription_id()
        subscriptions.append([sub_id, conn.client_id, { 'filter': filter }])
        subscription_recorder = MultiResponseRecorder()
        ta_id = conn.subscribe(sub_id, subscription_recorder,
                               filter=filter)
        subscription_recorder.ta_id = ta_id

        wait(conn, criteria = subscription_recorder.accepted)

        conns.append(conn)

    list_conn = random.choice(conns)

    assert sorted(list_conn.subscriptions(), key=by_id) == \
        sorted(subscriptions, key=by_id)

    for conn in conns:
        conn.close()

def test_list_services(server):
    conn = client.connect(server.random_domain().random_addr())

    services = []

    for num in range(0, NUM_SERVICES):
        service_id = conn.service_id()
        service_generation = random.randint(0, 100)
        service_props = {
            "name": { "service-%d" % num },
            "key_str": { "value%d" % num },
            "key_int": { num },
            "key_mv": { "strval%d" % num, num }
        }
        service_ttl = 99

        service = [service_id, service_generation,
                   service_props, service_ttl, conn.client_id]

        services.append(service)
        conn.publish(service_id, service_generation, service_props, service_ttl)

    assert sorted(conn.services(), key=by_id) == \
        sorted(services, key=by_id)

    assert len(conn.services(filter="(key_int>0)")) == (NUM_SERVICES - 1)

def test_list_orphan(server):
    domain = server.random_domain()
    pub_conn = client.connect(domain.default_addr())

    service_id = pub_conn.service_id()
    service_generation = 123
    service_props = { "name": "foo" }
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

def test_list_services_with_invalid_filter(server):
    conn = client.connect(server.random_domain().random_addr())

    recorder = MultiResponseRecorder()
    ta_id = conn.services(recorder, filter="(&foo)")
    recorder.ta_id = ta_id

    wait(conn, criteria = recorder.failed)

    assert recorder.get_fail_reason() == \
        proto.FAIL_REASON_INVALID_FILTER_SYNTAX

    conn.close()

def test_disconnected_client_orphans_service(server):
    domain = server.random_domain()
    conn_sub = client.connect(domain.default_addr())

    subscription_recorder = MultiResponseRecorder()
    ta_id = conn_sub.subscribe(42, subscription_recorder)
    subscription_recorder.ta_id = ta_id

    wait(conn_sub, criteria = subscription_recorder.accepted)

    conn_pub = client.connect(domain.default_addr())

    service_id = conn_pub.service_id()
    service_generation = 10
    service_props = {
        "name": { "service-x" },
        "value": { 0 }
    }
    service_ttl = 1
    conn_pub.publish(service_id, service_generation, service_props,
                     service_ttl)

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() == 1)

    disconnect_time = time.time()

    conn_pub.close()

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() == 2)
    orphan_latency = time.time() - disconnect_time

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() >= 3)
    timeout_latency = time.time() - disconnect_time

    assert orphan_latency < 0.25
    assert timeout_latency > service_ttl
    assert timeout_latency < service_ttl + 0.25

    notifications = subscription_recorder.get_notifications()

    orphan_since = notifications[1][3]['orphan_since']
    assert orphan_since >= int(disconnect_time)
    assert orphan_since <= int(disconnect_time)+1

    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn_pub.client_id }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn_pub.client_id,
           'orphan_since': orphan_since }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_DISAPPEARED, service_id)
    ]

    assert len(conn_sub.services()) == 0

    conn_sub.close()

def crashing_client(domain_addr, service_ttl):
    conn = client.connect(domain_addr)

    conn.publish(conn.service_id(), 0, {}, service_ttl)
    conn.publish(conn.service_id(), 0, {}, service_ttl)

def test_survives_connection_reset(server):
    domain_addr = server.random_domain().random_addr()
    service_ttl = 1

    t = threading.Thread(target=crashing_client,
                         args=(domain_addr, service_ttl))
    t.start()
    t.join()

    time.sleep(service_ttl + 0.25)

    conn = client.connect(domain_addr)
    conn.ping()
    conn.close()

def test_survives_connection_reset(server):
    domain_addr = server.random_domain().random_addr()
    service_ttl = 1

    t = threading.Thread(target=crashing_client,
                         args=(domain_addr, service_ttl))
    t.start()
    t.join()

    time.sleep(service_ttl + 0.25)

    conn = client.connect(domain_addr)
    conn.ping()
    conn.close()

CLIENT_PROCESS_TTL = 1
class ClientProcess(multiprocessing.Process):
    def __init__(self, domain_addr, ready_queue, unpublish = True,
                 unsubscribe = True):
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
        while conn == None:
            try:
                conn = client.connect(self.domain_addr)
            except proto.Error:
                time.sleep(3*random.random())

        service_id = conn.service_id()
        generation = 0
        service_props = { "name": { "service-%d" % service_id } }
        service_ttl = CLIENT_PROCESS_TTL
        conn.publish(service_id, generation, service_props, service_ttl)

        sub_id = conn.subscription_id()
        conn.subscribe(sub_id, lambda *args, **optargs: None)

        self.ready_queue.put(True)
        wait(conn, criteria = lambda: self.stop)

        if self.unpublish:
            conn.unpublish(service_id)
        if self.unsubscribe:
            conn.unsubscribe(sub_id)

        sys.exit(0)

def test_survives_killed_clients(server):
    domain_addr = server.random_domain().random_addr()

    num_clients = MAX_CLIENTS-1
    ready_queue = multiprocessing.Queue()
    processes = []
    for i in range(0, num_clients):
        p = ClientProcess(domain_addr, ready_queue)
        p.start()
        processes.append(p)
        ready_queue.get()

    for p in processes:
        if random.random() < 0.75:
            p.terminate()
        else:
            # Python 2 is missing the Process.kill() method
            os.kill(p.pid, signal.SIGKILL)

    time.sleep(1)

    conn = client.connect(domain_addr)

    conn.ping()

    for p in processes:
        p.join()

    conn.ping()

    conn.close()

def test_reconnecting_client_keeps_service_alive(server):
    domain = server.random_domain()
    conn_pub0 = client.connect(domain.default_addr())

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

    wait(conn_sub, criteria = subscription_recorder.accepted)

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() == 1)

    conn_pub0.close()

    wait(conn_sub, timeout = service_ttl-1)

    assert subscription_recorder.count_notifications() == 2

    conn_pub1 = client.connect(domain.default_addr())

    conn_pub1.publish(service_id, service_generation,
                      service_props, service_ttl)

    wait(conn_sub, criteria = \
         lambda: subscription_recorder.count_notifications() >= 3)

    # wait for any (errornous!) timeout to happen
    wait(conn_pub1, timeout = 2)

    wait(conn_sub, timeout = 0.1)

    notifications = subscription_recorder.get_notifications()
    orphan_since = notifications[1][3]['orphan_since']

    assert notifications == [
        (client.EventType.NOTIFY, client.MATCH_TYPE_APPEARED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn_pub0.client_id }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn_pub0.client_id,
           'orphan_since': orphan_since }),
        (client.EventType.NOTIFY, client.MATCH_TYPE_MODIFIED, service_id,
         { 'generation': service_generation, 'service_props': service_props,
           'ttl': service_ttl, 'client_id': conn_pub1.client_id })
    ]

    conn_sub.close()

MANY_ORPHANS = 100

def test_many_orphans(server):
    domain = server.random_domain()

    service_generation = 0
    service_props = {}
    min_service_ttl = 1
    max_service_ttl = 2
    for service_id in range(0, MANY_ORPHANS):
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

def run_misbehaving_client(addr, junk_msg, skip_hello = True):
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

def test_misbehaving_clients(server):
    domain_addr = server.random_domain().random_addr()
    conn = client.connect(domain_addr)

    unknown_cmd = {
        proto.FIELD_TA_CMD.name: "non-existing-command",
        proto.FIELD_TA_ID.name: 42,
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST
    }
    run_misbehaving_client(domain_addr, json.dumps(unknown_cmd).encode('utf-8'))

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
        proto.FIELD_MSG_TYPE.name: proto.MSG_TYPE_REQUEST
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(missing_ta_id).encode('utf-8'))

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
        proto.FIELD_SERVICE_PROPS.name: { "name": "not-a-list" },
        proto.FIELD_TTL.name: 5
    }
    run_misbehaving_client(domain_addr,
                           json.dumps(prop_value_not_list).encode('utf-8'))

    run_misbehaving_client(domain_addr, "not valid JSON at all")

    conn.ping()

    delayed_close(conn)

def test_many_clients(server):
    domain = server.random_domain()
    conns = []
    while len(conns) < MAX_CLIENTS:
        try:
            conn = None
            conn = client.connect(domain.default_addr())
            wait(conn, criteria = conn.ready)
            conns.append(conn)
        except client.TransportError:
            if conn:
                conn.close()

    replies = []
    for i, conn in enumerate(conns):
        cb = lambda ta_id, *args: replies.append(None)
        conn.ping(cb)
    for i, conn in enumerate(conns):
        wait(conn, criteria = lambda: len(replies) == i+1)

    last_conn = None
    try:
        # the server shouldn't be accepting any more connections
        last_conn = client.connect(domain.default_addr())
        ping_recorder = SingleResponseRecorder()
        last_conn.ping(ping_recorder)
        wait(last_conn, timeout = 0.5)
        assert not ping_recorder.completed()
        last_conn.close()
    except client.Error:
        pass

    for conn in conns:
        conn.close()

FEW_CLIENTS = 4
def test_list_clients(server):
    domain = server.random_domain()
    conn = client.connect(domain.default_addr())

    other_conns = []
    for i in range(0, FEW_CLIENTS):
        other_conn = client.connect(domain.default_addr())
        wait(other_conn, criteria = other_conn.ready)
        other_conns.append(other_conn)

    recorder = MultiResponseRecorder()
    ta_id = conn.clients(recorder)
    recorder.ta_id = ta_id
    wait(conn, criteria = recorder.completed)

    notifications = recorder.get_notifications()
    assert len(notifications) == FEW_CLIENTS + 1

    for other_conn in other_conns:
        other_conn.close()
    conn.close()

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

def test_connect_by_domain_name(server):
    for domain in server.domains:
        conn_by_name = client.connect(domain.name)
        conn_by_name.ping()

        conn_by_addr = client.connect(domain.random_addr())
        conn_by_addr.ping()

        assert len(conn_by_name.clients()) == 2

        conn_by_name.close()
        conn_by_addr.close()

MANY_REQUESTS=10000
def test_many_requests(server):
    conn = client.connect(server.random_domain().random_addr())

    ping_recorders = []
    for i in range(0, MANY_REQUESTS):
        ping_recorder = SingleResponseRecorder()
        ta_id = conn.ping(ping_recorder)
        ping_recorder.ta_id = ta_id
        ping_recorders.append(ping_recorder)

    ping_recorders.reverse()

    for ping_recorder in ping_recorders:
        wait(conn, criteria = ping_recorder.completed)

    delayed_close(conn)

def assure_ping(conn, max_latency):
    start = time.time()
    conn.ping()
    latency = time.time() - start
    assert latency < max_latency

NUM_SLOW_CONN_REQS = 50
ACCEPTABLE_LATENCY = 0.5
def test_slow_client(server):
    domain_addr = server.random_domain().random_addr()
    slow_conn = client.connect(domain_addr)

    fast_conn = client.connect(domain_addr)

    for i in range(0, NUM_SERVICES):
        fast_conn.publish(i, 0, {}, 42)

    replies = []
    cb = lambda ta_id, *args: replies.append(None)

    # try to hog the server with a slow client only issuing new requests,
    # never consuming any responses
    for i in range(0, NUM_SLOW_CONN_REQS):
        slow_conn.services(response_cb = cb)

    assure_ping(fast_conn, ACCEPTABLE_LATENCY)

    # make sure to fill up the server's socket buffer facing the slow client
    deadline = time.time() + 0.25
    while time.time() < deadline:
        slow_conn.try_send()

    assure_ping(fast_conn, ACCEPTABLE_LATENCY)

    assert len(replies) == 0

    expected_responses = (NUM_SERVICES + 2) * NUM_SLOW_CONN_REQS

    wait(slow_conn, criteria = lambda: len(replies) == expected_responses)

    slow_conn.close()
    fast_conn.close()

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
        proto.FIELD_FAIL_REASON.name: proto.FAIL_REASON_UNSUPPORTED_PROTOCOL_VERSION
    }
    in_msg = conn.receive()
    assert len(in_msg) > 0
    actual_response = json.loads(in_msg.decode('utf-8'))
    assert actual_response == expected_response
    conn.close()

def run_leak_clients(domain_addr, num):
    ready_queue = multiprocessing.Queue()
    processes = []
    for i in range(0, num):
        unpublish = bool(random.getrandbits(1))
        unsubscribe = bool(random.getrandbits(1))
        p = ClientProcess(domain_addr, ready_queue, unpublish = unpublish,
                          unsubscribe = unsubscribe)
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
    for i in range(0, 250):
        while True:
            try:
                conn = xcm.connect(domain_addr, 0)
                conn.send("foo")
                conn.close()
                break;
            except xcm.error:
                pass

    # Connect on Pathfinder protocol level
    for i in range(0, 250):
        while True:
            try:
                conn = client.connect(domain_addr)
                conn.ping()
                conn.close()
                break;
            except proto.TransportError:
                pass

    # Spawn off many clients concurrently subscribing and publishing
    for i in range(0, 100):
        run_leak_clients(domain_addr, 5)

    conn = client.connect(domain_addr)
    time.sleep(CLIENT_PROCESS_TTL)
    while len(conn.services()) > 0:
        time.sleep(0.1)
    conn.close()

ALLOWED_RETRIES = 3
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

    for i in range(0, ALLOWED_RETRIES + 1):
        initial_rss = rss
        exercise_server(domain_addr)
        rss = get_rss(tls_server.process.pid)
        if rss == initial_rss:
            break

    assert rss == initial_rss

def xcm_has_uxf():
    try:
        s = xcm.server("uxf:%s" % random_name())
        s.close()
        return True
    except xcm.error:
        return False

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
