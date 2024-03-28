# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

from collections import deque
from enum import Enum
import errno
import json
import random
import select
import time
import os

import paf.xcm as xcm
import paf.proto as proto

MATCH_TYPE_APPEARED = proto.MATCH_TYPE_APPEARED
MATCH_TYPE_MODIFIED = proto.MATCH_TYPE_MODIFIED
MATCH_TYPE_DISAPPEARED = proto.MATCH_TYPE_DISAPPEARED

TRACK_TYPE_QUERY = proto.TRACK_TYPE_QUERY
TRACK_TYPE_REPLY = proto.TRACK_TYPE_REPLY

MAX_MSGS_PER_ROUND = 128

ProtocolError = proto.ProtocolError
TransportError = proto.TransportError
Error = proto.Error

PROTO_VERSION_RANGE = proto.VERSION_RANGE
PROTO_VERSIONS = proto.VERSIONS


class TransactionError(Error):
    def __init__(self, reason=None):
        if reason is not None:
            message = "Protocol transaction failed: '%s'." % reason
        else:
            message = "Protocol transaction failed for unknown reason." % \
                reason
        Error.__init__(self, message)


class EventType(Enum):
    ACCEPT = 0
    NOTIFY = 1
    INFORM = 2
    COMPLETE = 3
    FAIL = 4


class TransactionState(Enum):
    IDLE = 0
    REQUESTING = 1
    ACCEPTED = 2
    TERMINATED = 3


class Transaction:
    def __init__(self, ta_type, ta_id):
        self.ta_id = ta_id
        self.ta_type = ta_type
        self.state = TransactionState.IDLE

    def produce_request(self, request_args, request_optargs,
                        response_cb):
        request_msg = {}

        proto.FIELD_TA_CMD.put(self.ta_type.cmd, request_msg)
        proto.FIELD_TA_ID.put(self.ta_id, request_msg)
        proto.FIELD_MSG_TYPE.put(proto.MSG_TYPE_REQUEST, request_msg)

        assert len(request_args) == len(self.ta_type.request_fields)

        for i, field in enumerate(self.ta_type.request_fields):
            field_value = request_args[i]
            field.put(field_value, request_msg)

        for opt_field in self.ta_type.opt_request_fields:
            if opt_field.name in request_optargs:
                field_value = request_optargs.get(opt_field.python_name())
                if field_value is not None:
                    opt_field.put(field_value, request_msg)
                del request_optargs[opt_field.name]

        assert len(request_optargs) == 0

        self.cb = response_cb
        self.state = TransactionState.REQUESTING
        return request_msg

    def produce_inform(self, inform_args, inform_optargs):
        assert self.state == TransactionState.ACCEPTED

        inform_msg = {}

        proto.FIELD_TA_CMD.put(self.ta_type.cmd, inform_msg)
        proto.FIELD_TA_ID.put(self.ta_id, inform_msg)
        proto.FIELD_MSG_TYPE.put(proto.MSG_TYPE_INFORM, inform_msg)

        assert len(inform_args) == len(self.ta_type.inform_fields)

        for i, field in enumerate(self.ta_type.inform_fields):
            field_value = inform_args[i]
            field.put(field_value, inform_msg)

        for opt_field in self.ta_type.opt_inform_fields:
            if opt_field.name in inform_optargs:
                field_value = inform_optargs.get(opt_field.python_name())
                if field_value is not None:
                    opt_field.put(field_value, inform_msg)
                del inform_optargs[opt_field.name]

        assert len(inform_optargs) == 0

        return inform_msg

    def consume_message(self, in_msg):
        ta_cmd = proto.FIELD_TA_CMD.pull(in_msg)
        if ta_cmd != self.ta_type.cmd:
            raise ProtocolError("Received message in transaction %d; expected "
                                "\"%s\" command, but got \"%s\"." %
                                (self.ta_id, self.ta_type.cmd, ta_cmd))

        msg_type = proto.FIELD_MSG_TYPE.pull(in_msg)

        if msg_type == proto.MSG_TYPE_ACCEPT and \
           self.state == TransactionState.REQUESTING and \
           self.ta_type.ia_type in (proto.InteractionType.MULTI_RESPONSE,
                                    proto.InteractionType.TWO_WAY):
            event = EventType.ACCEPT
            fields = self.ta_type.accept_fields
            opt_fields = self.ta_type.opt_accept_fields
            self.state = TransactionState.ACCEPTED
        elif (msg_type == proto.MSG_TYPE_NOTIFY and
              self.state == TransactionState.ACCEPTED):
            event = EventType.NOTIFY
            fields = self.ta_type.notify_fields
            opt_fields = self.ta_type.opt_notify_fields
        elif (msg_type == proto.MSG_TYPE_COMPLETE and
              ((self.state == TransactionState.REQUESTING and
                self.ta_type.ia_type == proto.InteractionType.SINGLE_RESPONSE)
               or
               (self.state == TransactionState.ACCEPTED and
                self.ta_type.ia_type ==
                proto.InteractionType.MULTI_RESPONSE))):
            fields = self.ta_type.complete_fields
            opt_fields = self.ta_type.opt_complete_fields
            event = EventType.COMPLETE
            self.state = TransactionState.TERMINATED
        elif msg_type == proto.MSG_TYPE_FAIL:
            fields = self.ta_type.fail_fields
            opt_fields = self.ta_type.opt_fail_fields
            event = EventType.FAIL
            self.state = TransactionState.TERMINATED
        else:
            raise ProtocolError("Received invalid message type %s "
                                "for %s transaction %d in state %s" %
                                (msg_type, self.ta_type.cmd,
                                 self.ta_id, self.state.name))
        args = [field.pull(in_msg) for field in fields]

        optargs = {}
        for opt_field in opt_fields:
            opt_value = opt_field.pull(in_msg, opt=True)
            if opt_value is not None:
                optargs[opt_field.python_name()] = opt_value

        if len(in_msg) > 0:
            raise ProtocolError("Server sent message with unknown fields: "
                                "%s" % list(in_msg.keys()))
        self.cb(self.ta_id, event, *args, **optargs)


def wait(conn, criteria):
    poll = select.poll()
    poll.register(conn.fileno(), select.POLLIN)
    while not criteria():
        poll.poll()
        conn.process()


class Call:
    def __init__(self, conn):
        self.conn = conn
        self.ta_id = None
        self.result = None

    def __call__(self, ta_id, event, *args, **optargs):
        assert self.ta_id is not None
        assert self.ta_id == ta_id
        self.result = event
        if self.result == EventType.FAIL:
            self.reason = optargs.get('fail_reason')

    def get(self):
        wait(self.conn, lambda: self.result == EventType.COMPLETE or
             self.result == EventType.FAIL)
        if self.result == EventType.FAIL:
            raise TransactionError(reason=self.reason)


class LatencyCall(Call):
    def __init__(self, conn):
        self.start = time.time()
        Call.__init__(self, conn)

    def __call__(self, ta_id, event, *args):
        if event == EventType.COMPLETE:
            self.latency = time.time() - self.start
        Call.__call__(self, ta_id, event, *args)

    def get(self):
        Call.get(self)
        return self.latency


class NotifyCall(Call):
    def __init__(self, conn):
        Call.__init__(self, conn)
        self.notifications = []

    def __call__(self, ta_id, event, *args, **optargs):
        if event == EventType.NOTIFY:
            notification = list(args)
            if len(optargs) > 0:
                notification.append(optargs)
            self.notifications.append(notification)
        Call.__call__(self, ta_id, event, *args, **optargs)

    def get(self):
        Call.get(self)
        return self.notifications


class CompleteCall(Call):
    def __init__(self, conn):
        Call.__init__(self, conn)

    def __call__(self, ta_id, event, *args):
        if event == EventType.COMPLETE:
            self.complete = args
        Call.__call__(self, ta_id, event, *args)

    def get(self):
        Call.get(self)
        return self.complete


class Tracker:
    def __init__(self, conn):
        self.conn = conn
        self.accepted = False
        self.failed = False
        self.notification_queries = 0
        self.inform_queries = 0
        self.notification_replies = 0

    def __call__(self, ta_id, event, *args, **optargs):
        if event == EventType.ACCEPT:
            self.accepted = True
        elif event == EventType.FAIL:
            self.fail_reason = optargs.get('fail_reason')
            self.failed = True
        elif event == EventType.NOTIFY:
            track_type = args[0]
            if track_type == TRACK_TYPE_QUERY:
                self.conn.track_reply(ta_id)
                self.notification_queries += 1
            else:
                self.notification_replies += 1
                if self.notification_replies > self.inform_queries:
                    raise ProtocolError("Received unsolicited track replies")

    def query(self):
        assert self.accepted

        self.conn.track_query(self.ta_id)
        self.inform_queries += 1


class ServerConf:
    def __init__(self, addr, proto_version_min=proto.MIN_VERSION,
                 proto_version_max=proto.MAX_VERSION):
        self.addr = addr
        self.proto_version_min = proto_version_min
        self.proto_version_max = proto_version_max
        self.attrs = {}

    def check_proto_version_range(self, supported_range):
        if self.proto_version_min > proto.MAX_VERSION:
            raise ProtocolError("Minimum configured protocol version "
                                "is higher than highest supported by "
                                "client")

        if self.proto_version_max < proto.MIN_VERSION:
            raise ProtocolError("Maximum configured protocol version "
                                "is lower than lowest supported by "
                                "client")

    def proto_version_range(self):
        return (self.proto_version_min, self.proto_version_max)

    def set_proto_version(self, proto_version):
        self.proto_version_min = proto_version
        self.proto_version_max = proto_version


class Client:
    def __init__(self, client_id, server_conf, track, ready_cb):
        self.client_id = client_id
        try:
            attrs = {'xcm.blocking': False}
            attrs.update(server_conf.attrs)
            self.conn_sock = xcm.connect(server_conf.addr, attrs=attrs)
        except xcm.error as e:
            raise TransportError(str(e))

        server_conf.check_proto_version_range(proto.VERSION_RANGE)

        self.proto_version_range = server_conf.proto_version_range()

        if track:
            self.tracker = Tracker(self)
        else:
            self.tracker = None
        self.ready_cb = ready_cb
        self.ta_id = 0
        self.out_wire_msgs = deque()
        self.transactions = {}
        self.proto_version = None
        self.update()
        try:
            self.initial_hello()
        except Error:
            self.close()
            raise

    def initial_hello(self):
        self.hello(response_cb=self.initial_hello_cb)
        if self.ready_cb is None:
            wait(self, criteria=lambda: self.proto_version is not None)

    def initial_hello_cb(self, ta_id, event, *args, **optargs):
        if event == EventType.FAIL:
            reason = optargs.get('fail_reason')
            if reason is None:
                reason = "reason unknown"
            raise ProtocolError("Protocol establishment failed: %s" %
                                reason)
        elif event == EventType.COMPLETE:
            selected_version = args[0]

            if selected_version not in proto.VERSIONS:
                raise ProtocolError("Server selected unsupported "
                                    "protocol version %d (required %d-%d)"
                                    % (selected_version, proto.MIN_VERSION,
                                       proto.MAX_VERSION))
            self.proto_version = selected_version

            if self.tracker is not None:
                self.tracker.ta_id = self.track(self.tracker)

            if self.ready_cb is not None:
                self.ready_cb()

    def close(self):
        self.conn_sock.close()

    def hello(self, proto_version_range=None, response_cb=None):
        if proto_version_range is None:
            proto_version_range = self.proto_version_range

        return self.issue_request(proto.TA_HELLO,
                                  (self.client_id, proto_version_range[0],
                                   proto_version_range[1]), {},
                                  CompleteCall, response_cb)

    def publish(self, service_id, generation, service_props, ttl,
                response_cb=None):
        return self.issue_request(proto.TA_PUBLISH, (service_id, generation,
                                                     service_props, ttl),
                                  {}, Call, response_cb)

    def unpublish(self, service_id, response_cb=None):
        return self.issue_request(proto.TA_UNPUBLISH, (service_id,),
                                  {}, Call, response_cb)

    def subscribe(self, sub_id, response_cb, filter=None):
        return self.async_request(proto.TA_SUBSCRIBE, (sub_id,),
                                  {'filter': filter}, response_cb)

    def track(self, response_cb):
        ta_type = proto.lookup_type(self.proto_version, proto.CMD_TRACK)
        return self.async_request(ta_type, (), (), response_cb)

    def track_query(self, track_ta_id):
        self.issue_inform(track_ta_id, (proto.TRACK_TYPE_QUERY,), {})

    def track_reply(self, track_ta_id):
        self.issue_inform(track_ta_id, (proto.TRACK_TYPE_REPLY,), {})

    def unsubscribe(self, sub_id, response_cb=None):
        return self.issue_request(proto.TA_UNSUBSCRIBE, (sub_id,), {},
                                  Call, response_cb)

    def subscriptions(self, response_cb=None):
        return self.issue_request(proto.TA_SUBSCRIPTIONS, (), {},
                                  NotifyCall, response_cb)

    def services(self, response_cb=None, filter=None):
        return self.issue_request(proto.TA_SERVICES, (), {'filter': filter},
                                  NotifyCall, response_cb)

    def ping(self, response_cb=None):
        return self.issue_request(proto.TA_PING, (), {}, LatencyCall,
                                  response_cb)

    def clients(self, response_cb=None):
        ta_type = proto.lookup_type(self.proto_version, proto.CMD_CLIENTS)
        return self.issue_request(ta_type, (), {}, NotifyCall,
                                  response_cb)

    def service_id(self):
        return self.gen_id()

    def subscription_id(self):
        return self.gen_id()

    def gen_id(self):
        INT64_MAX = 0x7fffffffffffffff
        return random.randint(0, INT64_MAX)

    def next_ta_id(self):
        ta_id = self.ta_id
        self.ta_id += 1
        return ta_id

    def issue_request(self, ta_type, request_args, request_optargs,
                      call_cls, response_cb):
        if response_cb is not None:
            return self.async_request(ta_type, request_args, request_optargs,
                                      response_cb)
        else:
            assert call_cls is not None
            return self.sync_request(ta_type, request_args,
                                     request_optargs, call_cls)

    def sync_request(self, ta_type, request_args, request_optargs,
                     call_cls):
        call = call_cls(self)
        ta_id = self.async_request(ta_type, request_args, request_optargs,
                                   call)
        assert ta_id is not None
        call.ta_id = ta_id
        return call.get()

    def async_request(self, ta_type, request_args, request_optargs,
                      response_cb):
        ta_id = self.next_ta_id()
        transaction = Transaction(ta_type, ta_id)
        request_msg = transaction.produce_request(request_args,
                                                  request_optargs,
                                                  response_cb)
        out_wire_msg = json.dumps(request_msg).encode('utf-8')
        self.out_wire_msgs.append(out_wire_msg)
        self.transactions[ta_id] = transaction
        self.try_send()
        return ta_id

    def issue_inform(self, ta_id, inform_args, inform_optargs):
        transaction = self.transactions[ta_id]
        inform_msg = transaction.produce_inform(inform_args, inform_optargs)
        out_wire_msg = json.dumps(inform_msg).encode('utf-8')
        self.out_wire_msgs.append(out_wire_msg)
        self.try_send()

    def fileno(self):
        return self.conn_sock.fileno()

    def update(self):
        condition = xcm.SO_RECEIVABLE
        if len(self.out_wire_msgs) > 0:
            condition |= xcm.SO_SENDABLE
        self.conn_sock.set_target(condition)

    def process(self):
        for i in range(0, MAX_MSGS_PER_ROUND):
            if not self.try_send():
                break
        for i in range(0, MAX_MSGS_PER_ROUND):
            if not self.try_receive():
                break

    def try_send(self):
        try:
            if len(self.out_wire_msgs) > 0:
                out_wire_msg = self.out_wire_msgs.popleft()
                self.conn_sock.send(out_wire_msg)
                return True
            else:
                return False
        except xcm.error as e:
            if e.errno == errno.EAGAIN:
                self.out_wire_msgs.appendleft(out_wire_msg)
                return False
            else:
                raise TransportError(str(e))
        finally:
            self.update()

    def try_receive(self):
        try:
            in_wire_msg = self.conn_sock.receive()
            if len(in_wire_msg) == 0:
                raise ProtocolError("Server closed connection")
            in_msg = json.loads(in_wire_msg.decode('utf-8'))
            ta_id = proto.FIELD_TA_ID.pull(in_msg)
            if ta_id not in self.transactions:
                raise ProtocolError("Received message related to unknown "
                                    "transaction %d" % ta_id)
            transaction = self.transactions[ta_id]
            transaction.consume_message(in_msg)
            if transaction.state == TransactionState.TERMINATED:
                del self.transactions[ta_id]
            return True
        except xcm.error as e:
            if e.errno == errno.EAGAIN:
                return False
            else:
                raise TransportError(str(e))
        except ValueError:
            raise ProtocolError("Error decoding response message JSON")
        finally:
            self.update()


DOMAINS_ENV = 'PAF_DOMAINS'
DEFAULT_DOMAINS_DIR = '/run/paf/domains.d'
DOMAIN_FILE_TO_XCM_ATTR = {
    'tlsCertificateFile': 'tls.cert_file',
    'tlsKeyFile': 'tls.key_file',
    'tlsTrustedCaFile': 'tls.tc_file'
}


def domains_dir():
    if DOMAINS_ENV in os.environ:
        return os.environ[DOMAINS_ENV]
    else:
        return DEFAULT_DOMAINS_DIR


def domain_filename(domain):
    return os.path.join(domains_dir(), domain)


def list_domains():
    d = domains_dir()
    for f in os.listdir(d):
        if os.path.isfile(os.path.join(d, f)):
            yield f


def looks_like_json_object(s):
    # see RFC 7159, section 2 for grammar
    for c in s:
        if c in ('\n', '\r', ' ', '\t'):
            continue
        if c == '{':
            return True
        return False
    return False


def parse_domain_json(data):
    root = json.loads(data)
    servers = []
    for server_obj in root['servers']:
        server = ServerConf(server_obj['address'])
        for json_name, xcm_name in DOMAIN_FILE_TO_XCM_ATTR.items():
            if json_name in server_obj:
                server.attrs[xcm_name] = server_obj[json_name]

        if "minProtocolVersion" in server_obj:
            server.proto_version_min = server_obj["minProtocolVersion"]
        if "maxProtocolVersion" in server_obj:
            server.proto_version_max = server_obj["maxProtocolVersion"]

        servers.append(server)
    return servers


def parse_domain_custom(data):
    servers = []
    for line in data.split('\n'):
        addr = line.strip()
        if len(addr) == 0 or addr[0] == '#':
            continue
        servers.append(ServerConf(addr))
    return servers


def read_domain(domain):
    try:
        domains_data = open(domain_filename(domain)).read()
    except IOError:
        return []

    if looks_like_json_object(domains_data):
        return parse_domain_json(domains_data)
    else:
        return parse_domain_custom(domains_data)


def domain_server(domain):
    servers = read_domain(domain)
    if len(servers) > 0:
        return servers[0]
    else:
        return ServerConf(domain)


def allocate_client_id():
    return random.randint(0, ((1 << 63) - 1))


def connect(domain_or_addr_or_conf, client_id=None, ready_cb=None,
            track=False):
    if isinstance(domain_or_addr_or_conf, ServerConf):
        server = domain_or_addr_or_conf
    else:
        server = domain_server(domain_or_addr_or_conf)
    if client_id is None:
        client_id = allocate_client_id()
    return Client(client_id, server, track, ready_cb)
