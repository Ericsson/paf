# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

from collections import deque
from enum import Enum, auto
from itertools import chain
import errno
import time

import paf.xcm as xcm
import paf.proto as proto
from paf.proto import ProtocolError, Message
import paf.sd as sd
import paf.filter
import paf.props as props
import paf.eventloop as eventloop
import paf.timer
from paf.logging import LogCategory, debug, info, warning

MAJOR_VERSION = 1
MINOR_VERSION = 1
PATCH_VERSION = 1

VERSION = "%d.%d.%d" % (MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION)


class TransactionState(Enum):
    IDLE = auto()
    REQUESTED = auto()
    ACCEPTED = auto()
    FAILED = auto()
    COMPLETED = auto()


class Transaction:
    def __init__(self, ta_id, ta_type, proto_version, debug, term_cb):
        self.ta_id = ta_id
        self.ta_type = ta_type
        self.proto_version = proto_version
        self.debug = debug
        self.term_cb = term_cb
        self.state = TransactionState.IDLE

    def message(self, in_msg):
        if not in_msg.is_client_generated():
            raise ProtocolError("Message received in transaction %d is not "
                                "of a client-generated type" % self.ta_id)

        if in_msg.is_request():
            self.request(in_msg)
        else:
            assert in_msg.is_inform()
            self.inform(in_msg)

    def request(self, in_msg):
        if self.state == TransactionState.ACCEPTED:
            raise ProtocolError("Received duplicate request message in "
                                "transaction %d" % self.ta_id)

        assert self.state == TransactionState.IDLE

        self.state = TransactionState.REQUESTED

    def inform(self, in_msg):
        if self.state == TransactionState.IDLE:
            raise ProtocolError("Inform message precedes request in "
                                "transaction %d" % self.ta_id)

        assert self.state == TransactionState.ACCEPTED

    def response(self, msg_type, fields, opt_fields, *args, **optargs):
        self.debug("Responding with message type \"%s\" in transaction %d." %
                   (msg_type, self.ta_id), LogCategory.PROTOCOL)

        out_msg = Message(self.ta_type, self.ta_id, msg_type, args, optargs)

        return out_msg.to_wire()

    def complete(self, *args, **optargs):
        if self.is_single_response():
            assert self.state == TransactionState.REQUESTED
        else:
            assert self.state == TransactionState.ACCEPTED

        self.state = TransactionState.COMPLETED

        self.terminate()

        return self.response(proto.MSG_TYPE_COMPLETE,
                             self.ta_type.complete_fields,
                             self.ta_type.opt_complete_fields,
                             *args, **optargs)

    def fail(self, *args, **optargs):
        assert self.state != TransactionState.IDLE

        self.state = TransactionState.FAILED

        self.terminate()

        return self.response(proto.MSG_TYPE_FAIL,
                             self.ta_type.fail_fields,
                             self.ta_type.opt_fail_fields,
                             *args, **optargs)

        self.terminate()

    def accept(self, *args, **optargs):
        assert self.is_multi_response() or self.is_two_way()

        self.state = TransactionState.ACCEPTED

        return self.response(proto.MSG_TYPE_ACCEPT,
                             self.ta_type.accept_fields,
                             self.ta_type.opt_accept_fields,
                             *args, **optargs)

    def notify(self, *args, **optargs):
        assert self.is_multi_response() or self.is_two_way()

        return self.response(proto.MSG_TYPE_NOTIFY,
                             self.ta_type.notify_fields,
                             self.ta_type.opt_notify_fields,
                             *args, **optargs)

    def terminate(self):
        if self.term_cb is not None:
            self.term_cb(self)

    def is_single_response(self):
        return self.ta_type.ia_type == proto.InteractionType.SINGLE_RESPONSE

    def is_multi_response(self):
        return self.ta_type.ia_type == proto.InteractionType.MULTI_RESPONSE

    def is_two_way(self):
        return self.ta_type.ia_type == proto.InteractionType.TWO_WAY


MAX_SEND_BATCH = 64
MAX_ACCEPT_BATCH = 16
SOFT_OUT_WIRE_LIMIT = 128


class Connection:
    def __init__(self, sd, conn_sock, event_loop, server, handshake_cb,
                 proto_version_limit, idle_limit, idle_cb, term_cb):
        self.client_id = None
        self.proto_version = None
        self.conn_addr = conn_sock.get_attr("xcm.remote_addr")
        self.sd = sd
        self.conn_sock = conn_sock
        self.conn_source = eventloop.XcmSource(conn_sock)
        self.out_wire_msgs = deque()
        self.event_loop = event_loop
        self.server = server
        self.handshake_cb = handshake_cb
        self.proto_version_limit = proto_version_limit
        self.idle_limit = idle_limit
        self.idle_cb = idle_cb
        self.term_cb = term_cb
        self.update_source()
        self.event_loop.add(self.conn_source, self.activate)
        self.tas = {}
        self.sub_tas = {}
        self.connect_time = time.time()
        self.handshaked = False
        self.track_ta_id = None
        self.track_query_ts = None
        self.track_latency = None
        self.info("Accepted new client connection from \"%s\"." %
                  self.conn_addr, LogCategory.PROTOCOL)

    def log(self, log_fun, msg, category):
        if self.sd.name is not None:
            prefix = "%s: " % self.sd.name
        else:
            prefix = ""

        if self.client_id is not None:
            client = "0x%x" % self.client_id
        else:
            client = "unknown"

        log_fun("%s<%s> %s" % (prefix, client, msg), category)

    def debug(self, msg, category):
        self.log(debug, msg, category)

    def info(self, msg, category):
        self.log(info, msg, category)

    def warning(self, msg, category):
        self.log(warning, msg, category)

    def sendable(self):
        return len(self.out_wire_msgs) > 0

    def receivable(self):
        # Don't accept more work (requests or informs) in case many
        # messages are enroute to the client
        return len(self.out_wire_msgs) < SOFT_OUT_WIRE_LIMIT

    def update_source(self):
        condition = 0
        if self.sendable():
            condition |= xcm.SO_SENDABLE
        if self.receivable():
            condition |= xcm.SO_RECEIVABLE
        self.conn_source.update(condition)

    def activate(self):
        try:
            if self.receivable():
                self.try_receive()
            if self.sendable():
                self.try_send()
            self.update_source()
        except xcm.error as e:
            if e.errno == 0:
                self.debug("Connection is closed.", LogCategory.PROTOCOL)
            else:
                self.debug("Error on socket send or receive: %s." % e,
                           LogCategory.PROTOCOL)
            self.terminate()
        except proto.Error as e:
            self.warning("%s." % str(e), LogCategory.PROTOCOL)
            self.terminate()

    def try_send(self):
        for i in range(min(MAX_SEND_BATCH, len(self.out_wire_msgs))):
            try:
                out_wire_msg = self.out_wire_msgs.popleft()
                self.conn_sock.send(out_wire_msg)
                self.debug("Sent message: %s." % out_wire_msg,
                           LogCategory.PROTOCOL)
            except xcm.error as e:
                if e.errno != errno.EAGAIN:
                    raise
                self.out_wire_msgs.appendleft(out_wire_msg)

    def try_receive(self):
        try:
            in_wire_msg = self.conn_sock.receive()
            if len(in_wire_msg) == 0:
                raise xcm.error(0, "Connection closed")
            self.debug("Received message: %s" % in_wire_msg,
                       LogCategory.PROTOCOL)
            self.process(in_wire_msg)
        except xcm.error as e:
            if e.errno != errno.EAGAIN:
                raise

    def process(self, in_wire_msg):
        in_msg = Message.parse(self.proto_version, in_wire_msg)

        ta = self.tas.get(in_msg.ta_id)

        if ta is None:
            ta = Transaction(in_msg.ta_id, in_msg.ta_type, self.proto_version,
                             self.debug, self.ta_terminated)
            self.tas[ta.ta_id] = ta

        ta.message(in_msg)

        if in_msg.cmd() == proto.CMD_HELLO or self.handshaked:
            self.debug("Processing \"%s\" command %s with transaction "
                       "id %d." % (in_msg.ta_type.cmd, in_msg.msg_type,
                                   in_msg.ta_id), LogCategory.PROTOCOL)

            if self.handshaked:
                self.sd.client_active(self.client_id)

            for response in self.invoke_handler(ta, in_msg):
                self.respond(response)
        else:
            self.warning("Attempt to issue \"%s\" before issuing \"%s\"." %
                         (ta.ta_type.cmd, proto.CMD_HELLO),
                         LogCategory.SECURITY)
            self.respond(ta.fail(fail_reason=proto.FAIL_REASON_NO_HELLO))

    def ta_terminated(self, ta):
        del self.tas[ta.ta_id]

    def invoke_handler(self, ta, in_msg):
        if in_msg.is_request():
            handler_type = "request"
        else:
            handler_type = "inform"

        fun_name = "%s_%s" % (in_msg.cmd().replace("-", "_"), handler_type)
        fun = getattr(self, fun_name)
        return fun(ta, *in_msg.args, **in_msg.optargs)

    def determine_user_id(self):
        user_id = None
        if self.conn_addr.startswith("tls"):
            try:
                subject_key_id = \
                    self.conn_sock.get_attr("tls.peer_subject_key_id")
                subject_key_id_s = \
                    ":".join(["%02x" % b for b in subject_key_id])
                user_id = "ski:%s" % subject_key_id_s
            except xcm.error:
                self.warning("Unable to retrieve X509v3 Subject Key "
                             "Identifier. This attribute only exists in "
                             "XCM version 12 or later.", LogCategory.SECURITY)
        if self.conn_addr.startswith("tcp") or \
           (user_id is None and self.conn_addr.startswith("tls")):
            ip_port = self.conn_addr.split(":", 1)[1]
            ip = ip_port.rsplit(":", 1)[0]
            user_id = "ip:%s" % ip
        if user_id is None:
            user_id = sd.DEFAULT_USER_ID
        return user_id

    def hello_request(self, ta, client_id, min_version, max_version):
        if self.client_id is None:
            self.client_id = client_id
        elif self.client_id != client_id:
            self.warning("Attempt to change client id denied.",
                         LogCategory.SECURITY)
            yield ta.fail(fail_reason=proto.FAIL_PERMISSION_DENIED)
            return
        elif self.handshaked:
            self.debug("Received hello from client with handshake "
                       "procedure already successfully completed.",
                       LogCategory.PROTOCOL)
            yield ta.complete(self.proto_version)
            return
        if min_version == max_version:
            self.debug("Client supports protocol version %d (only)." %
                       min_version, LogCategory.PROTOCOL)
        else:
            self.debug("Client supports protocol versions between "
                       "%d and %d." % (min_version, max_version),
                       LogCategory.PROTOCOL)

        user_id = self.determine_user_id()
        self.info("User id is \"%s\"." % user_id, LogCategory.SECURITY)

        self.proto_version = \
            self.proto_version_limit.get_highest_allowed(min_version,
                                                         max_version)

        if self.proto_version is not None:
            try:
                if self.proto_version >= 3:
                    idle_limit = self.idle_limit
                else:
                    idle_limit = None

                self.sd.client_connect(self.client_id, user_id,
                                       self.idle_limit, self.idle_cb)

                self.debug("Handshake producedure finished for client from "
                           "\"%s\"." % self.conn_addr, LogCategory.PROTOCOL)
                self.debug("Protocol version %d is selected." %
                           self.proto_version, LogCategory.PROTOCOL)

                if idle_limit is not None:
                    self.debug("Initial max idle time is %d s." %
                               idle_limit.idle_default(), LogCategory.PROTOCOL)
                    self.idle_limit = idle_limit

                self.handshaked = True
                self.handshake_cb(self)

                yield ta.complete(self.proto_version)

                self.configure_tcp_keepalive()
            except sd.AlreadyExistsError:
                # There's a race between which of client and server
                # sees that the connection is down. If the client
                # wins, he might reconnect before the server has yet
                # realized the "old" client connection is down, and
                # this will cause the new hello request to
                # fail. However, the client will retry, so it's not an
                # issue.
                self.warning("Client %x is already connected." % client_id,
                             LogCategory.PROTOCOL)
                yield ta.fail(fail_reason=proto.FAIL_REASON_CLIENT_ID_EXISTS)
            except sd.PermissionError as e:
                self.warning("Unable to connect: %s." % e,
                             LogCategory.PROTOCOL)
                yield ta.fail(fail_reason=proto.FAIL_REASON_PERMISSION_DENIED)
            except sd.ResourceError as e:
                self.warning("Unable to connect: %s." % e,
                             LogCategory.SECURITY)
                reason = proto.FAIL_REASON_INSUFFICIENT_RESOURCES
                yield ta.fail(fail_reason=reason)
        else:
            self.warning("Client doesn't support a protocol version in "
                         "the range %d - %d." % (proto.MIN_VERSION,
                                                 proto.MAX_VERSION),
                         LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_UNSUPPORTED_PROTOCOL_VERSION
            yield ta.fail(fail_reason=reason)

    def is_tracked(self):
        return self.track_ta_id is not None and self.track_ta_id in self.tas

    def has_outstanding_track_query(self):
        return self.is_tracked() and self.track_query_ts is not None

    def track_request(self, ta):
        if self.is_tracked():
            self.warning("Track transaction already exists.",
                         LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_TRACK_EXISTS
            yield ta.fail(fail_reason=reason)
        else:
            self.track_ta_id = ta.ta_id
            self.debug("Installed tracker.", LogCategory.CORE)
            yield ta.accept()

    def track_inform(self, ta, track_type):
        if track_type == proto.TRACK_TYPE_QUERY:
            yield ta.notify(proto.TRACK_TYPE_REPLY)
            self.debug("Replied to track query.", LogCategory.CORE)
        elif track_type == proto.TRACK_TYPE_REPLY:
            if self.track_query_ts is None:
                raise ProtocolError("Received unsolicited track reply in "
                                    "track transaction %d", track_type,
                                    ta.ta_id)
            self.track_latency = time.time() - self.track_query_ts
            self.track_query_ts = None
            self.debug("Received to track query reply (after %.1f ms)." %
                       (1e3 * self.track_latency), LogCategory.CORE)
        else:
            raise ProtocolError("Received unknown track type \"%s\" in "
                                "track transaction %d", track_type,
                                ta.ta_id)

    def subscribe_request(self, ta, sub_id, filter=None):
        try:
            if filter is not None:
                filter = paf.filter.parse(filter)
            self.sd.create_subscription(self.client_id, sub_id, filter,
                                        self.subscription_triggered)
            self.sub_tas[sub_id] = ta
            log_msg = "Assigned subscription id %d to new subscription" % \
                      sub_id
            if filter is not None:
                log_msg += " with filter \"%s\"" % filter
            log_msg += "."
            self.debug(log_msg, LogCategory.CORE)
            yield ta.accept()
            # Subscription creation and activation must be separate,
            # to avoid having the match callback called before the
            # server has gotten the subscription id.
            self.sd.activate_subscription(self.client_id, sub_id)
        except paf.filter.ParseError as e:
            self.warning("Received subscription request with malformed "
                         "filter: %s." % str(e), LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_INVALID_FILTER_SYNTAX
            yield ta.fail(fail_reason=reason)
        except sd.AlreadyExistsError as e:
            self.warning("Received invalid subscription request: %s." % e,
                         LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_SUBSCRIPTION_ID_EXISTS
            yield ta.fail(fail_reason=reason)
        except sd.ResourceError as e:
            self.warning("Resource error processing subscription request %x: "
                         "%s." % (sub_id, e), LogCategory.SECURITY)
            reason = proto.FAIL_REASON_INSUFFICIENT_RESOURCES
            yield ta.fail(fail_reason=reason)

    def unsubscribe_request(self, ta, sub_id):
        try:
            self.sd.unsubscribe(self.client_id, sub_id)
            sub_ta = self.sub_tas[sub_id]
            del self.sub_tas[sub_id]
            yield sub_ta.complete()
            yield ta.complete()
            self.debug("Canceled subscription %d in transaction %d." %
                       (sub_id, sub_ta.ta_id), LogCategory.CORE)
        except sd.PermissionError as e:
            self.warning("Permission error while unsubscribing %x: "
                         "%s." % (sub_id, e), LogCategory.SECURITY)
            reason = proto.FAIL_REASON_PERMISSION_DENIED
            yield ta.fail(fail_reason=reason)
        except sd.NotFoundError:
            self.warning("Attempted to unsubscribe to non-existent "
                         "subscription %d." % sub_id, LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_NON_EXISTENT_SUBSCRIPTION_ID
            yield ta.fail(fail_reason=reason)

    def subscriptions_request(self, ta):
        yield ta.accept()
        for subscription in self.sd.get_subscriptions():
            if subscription.filter is not None:
                filter = str(subscription.filter)
            else:
                filter = None
            yield ta.notify(subscription.sub_id, subscription.client_id,
                            filter=filter)
        yield ta.complete()

    def services_request(self, ta, filter=None):
        try:
            if filter is not None:
                filter = paf.filter.parse(filter)
                self.debug("Accepted list request for services "
                           "matching %s." % str(filter), LogCategory.CORE)
            else:
                self.debug("Accepted list request for all services.",
                           LogCategory.CORE)
            yield ta.accept()
            for service in self.sd.get_services():
                if filter is None or filter.match(service.props()):
                    yield ta.notify(service.service_id, service.generation(),
                                    service.props(), service.ttl(),
                                    service.client_id(),
                                    orphan_since=service.orphan_since())
            yield ta.complete()
        except paf.filter.ParseError as e:
            self.info("Received list services request with malformed "
                      "filter: %s." % str(e), LogCategory.CORE)
            yield ta.fail(fail_reason=proto.FAIL_REASON_INVALID_FILTER_SYNTAX)

    def publish_request(self, ta, service_id, generation, service_props, ttl):
        try:
            service = self.sd.publish(self.client_id, service_id, generation,
                                      service_props, ttl)
            if not service.has_prev_generation():
                self.debug("Published new service with id %x, generation %d, "
                           "props %s and TTL %d s." %
                           (service_id, generation,
                            props.to_str(service_props), ttl),
                           LogCategory.CORE)
            else:
                log_msg = "Re-published service with id %x. " \
                    "Generation %d -> %d." \
                    % (service_id, service.prev_generation(),
                       service.generation())
                if service.was_orphan():
                    log_msg += " Replacing orphan."
                if service.props() != service.prev_props():
                    log_msg += " Properties changed from %s to %s." \
                               % (props.to_str(service.prev_props()),
                                  props.to_str(service.props()))
                if service.ttl() != service.prev_ttl():
                    log_msg += " TTL changed from %d to %d s." \
                               % (service.prev_ttl(), service.ttl())
                if service.client_id() != service.prev_client_id():
                    log_msg += " Owner is changed from %x to %x." \
                               % (service.prev_client_id(),
                                  service.client_id())
                self.debug(log_msg, LogCategory.CORE)
            yield ta.complete()
        except sd.PermissionError as e:
            self.warning("Permission error while publishing service %x: "
                         "%s." % (service_id, e), LogCategory.SECURITY)
            yield ta.fail(fail_reason=proto.FAIL_REASON_PERMISSION_DENIED)
        except sd.ResourceError as e:
            self.warning("Resource error while publishing service %x: "
                         "%s." % (service_id, e), LogCategory.SECURITY)
            yield ta.fail(fail_reason=proto.FAIL_REASON_INSUFFICIENT_RESOURCES)
        except sd.GenerationError as e:
            self.warning("Error while re-publishing service %x: %s." %
                         (service_id, e), LogCategory.CORE)
            yield ta.fail(fail_reason=proto.FAIL_REASON_OLD_GENERATION)
        except sd.SameGenerationButDifferentError as e:
            self.warning("Error while re-publishing service %x: %s." %
                         (service_id, e), LogCategory.CORE)
            yield ta.fail(
                fail_reason=proto.FAIL_REASON_SAME_GENERATION_BUT_DIFFERENT
            )

    def unpublish_request(self, ta, service_id):
        try:
            self.sd.unpublish(self.client_id, service_id)
            self.debug("Unpublished service id %x." % service_id,
                       LogCategory.CORE)
            yield ta.complete()
        except sd.PermissionError as e:
            self.warning("Permission error while trying to unpublish service "
                         "id %x: %s." % (service_id, e), LogCategory.SECURITY)
            reason = proto.FAIL_REASON_PERMISSION_DENIED
            yield ta.fail(fail_reason=reason)
        except sd.NotFoundError:
            self.warning("Attempted to unpublish non-existent service "
                         "id %d." % service_id, LogCategory.PROTOCOL)
            reason = proto.FAIL_REASON_NON_EXISTENT_SERVICE_ID
            yield ta.fail(fail_reason=reason)

    def ping_request(self, ta):
        yield ta.complete()

    def clients_request(self, ta):
        yield ta.accept()

        now = time.time()

        extended = self.proto_version >= 3

        for conn in self.server.client_connections.values():
            idle_time = now - self.sd.client_last_seen(conn.client_id)

            optargs = {}
            if conn.is_tracked():
                optargs["latency"] = conn.track_latency

            if extended:
                yield ta.notify(conn.client_id, conn.conn_addr,
                                int(conn.connect_time), idle_time,
                                conn.proto_version, **optargs)
            else:
                yield ta.notify(conn.client_id, conn.conn_addr,
                                int(conn.connect_time))

        yield ta.complete()

    def track_query(self, ta):
        self.track_query_ts = time.time()
        self.respond(ta.notify(proto.TRACK_TYPE_QUERY))

    def subscription_triggered(self, sub_id, match_type, service):
        subscription = self.server.sd.get_subscription(sub_id)
        if subscription.filter is not None:
            filter_s = "with filter %s" % subscription.filter
        else:
            filter_s = "without filter"
        if match_type == sd.MatchType.DISAPPEARED:
            service_props = service.prev_props()
        else:
            service_props = service.props()
        self.debug("Subscription id %d %s received %s event by "
                   "service id %x with properties %s." %
                   (sub_id, filter_s, match_type.name,
                    service.service_id, props.to_str(service_props)),
                   LogCategory.CORE)
        proto_match_type = getattr(proto, "MATCH_TYPE_%s" %
                                   match_type.name)
        ta = self.sub_tas[sub_id]
        if match_type == sd.MatchType.DISAPPEARED:
            self.respond(ta.notify(proto_match_type, service.service_id))
        else:
            self.respond(ta.notify(proto_match_type, service.service_id,
                                   generation=service.generation(),
                                   service_props=service.props(),
                                   ttl=service.ttl(),
                                   client_id=service.client_id(),
                                   orphan_since=service.orphan_since()))

    def respond(self, out_wire_msg):
        self.out_wire_msgs.append(out_wire_msg)
        self.update_source()

    def configure_tcp_keepalive(self):
        tp = self.conn_sock.get_attr("xcm.transport")

        if self.proto_version >= 3 and tp in ("tls", "tcp"):
            self.conn_sock.set_attr("tcp.keepalive", False)
            self.debug("TCP keepalive disabled.", LogCategory.PROTOCOL)

    def terminate(self):
        self.info("Disconnected.", LogCategory.PROTOCOL)
        if self.handshaked:
            self.sd.client_disconnect(self.client_id)
        self.event_loop.remove(self.conn_source)
        self.conn_sock.close()
        self.conn_sock = None
        self.conn_source = None
        self.term_cb(self)

    def check_idle(self):
        self.debug("Performing idle check.", LogCategory.PROTOCOL)
        # In Pathfinder protocol versions prior to 3, the transport
        # connection is used as an indication of the remote peer being
        # alive. If the connection is alive, the client is alive.
        if self.proto_version < 3:
            self.sd.client_active(self.client_id)
        elif self.is_tracked() and not self.has_outstanding_track_query():
            self.track_query(self.tas[self.track_ta_id])

    def time_out(self):
        self.debug("Client %d timed out." % self.client_id, LogCategory.CORE)
        self.terminate()


CLEAN_INTERVAL = 1
MAX_HANDSHAKE_TIME = 2

PAF_TO_XCM_SOCKET_ATTRS = {
    'cert': 'tls.cert_file',
    'key': 'tls.key_file',
    'tc': 'tls.tc_file',
    'crl': 'tls.crl_file'
}


class Server:
    def __init__(self, name, sockets, max_user_resources, max_total_resources,
                 proto_version_limit, idle_limit, event_loop):
        self.timer_manager = paf.timer.TimerManager(self.timer_changed)
        self.sd = sd.ServiceDiscovery(name, self.timer_manager,
                                      max_user_resources, max_total_resources)
        self.proto_version_limit = proto_version_limit
        self.idle_limit = idle_limit
        self.event_loop = event_loop
        self.server_socks = {}
        for socket in sockets:
            self.add_socket(socket)
        for source in self.server_socks.keys():
            self.event_loop.add(source, self.sock_activate)
        self.timer_source = eventloop.Source()
        self.event_loop.add(self.timer_source, self.timer_activate)
        self.clientless_connections = set()
        self.client_connections = {}
        self.clean_out_timer = None

    def add_socket(self, socket_conf):
        xcm_attrs = {"xcm.blocking": False}

        for attr_name, attr_value in socket_conf.tls_attrs.items():
            xcm_name = PAF_TO_XCM_SOCKET_ATTRS[attr_name]
            xcm_attrs[xcm_name] = attr_value

            if attr_name == "crl":
                xcm_attrs["tls.check_crl"] = True

        sock = xcm.server(socket_conf.addr, attrs=xcm_attrs)
        source = eventloop.XcmSource(sock)
        source.update(xcm.SO_ACCEPTABLE)
        self.server_socks[source] = sock

    def debug(self, msg, category):
        if self.sd.name is not None:
            prefix = "%s: " % self.sd.name
        else:
            prefix = ""
        debug("%s%s" % (prefix, msg), category)

    def num_connections(self):
        return len(self.clientless_connections) + len(self.client_connections)

    def client_capacity_left(self):
        max_clients = self.sd.max_total_clients()
        if max_clients is None:
            return None
        return max_clients - self.num_connections()

    def max_clients_reached(self):
        left = self.client_capacity_left()
        if left is None:
            return False
        return left == 0

    def schedule_clean_out(self):
        if len(self.clientless_connections) > 0 and \
           self.clean_out_timer is None:
            expiration_time = time.time() + CLEAN_INTERVAL
            self.clean_out_timer = \
                self.timer_manager.add(self.clean_out_handler,
                                       expiration_time)

    def clean_out_connection(self, conn, handshake_time):
        self.debug("Dropping connection from %s since it failed to "
                   "finish the protocol handshake within %.1f s." %
                   (conn.conn_addr, MAX_HANDSHAKE_TIME), LogCategory.PROTOCOL)
        conn.terminate()

    def clean_out_connections(self):
        if len(self.clientless_connections) > 0:
            self.debug("Scanning for idle connections. %d connection(s) has "
                       "not completed the protocol hand shake." %
                       len(self.clientless_connections), LogCategory.PROTOCOL)
            now = time.time()
            failed = []
            for conn in self.clientless_connections:
                if conn.client_id is None or \
                   not self.sd.has_client(conn.client_id):
                    handshake_time = now - conn.connect_time
                    if handshake_time > MAX_HANDSHAKE_TIME:
                        failed.append((conn, handshake_time))
            for conn, handshake_time in failed:
                self.clean_out_connection(conn, handshake_time)

    def clean_out_handler(self):
        self.clean_out_timer = None
        self.clean_out_connections()
        self.schedule_clean_out()

    def sock_activate(self):
        for source, sock in self.server_socks.items():
            if self.max_clients_reached():
                sock.finish()
                source.update(0)
            else:
                batch_size = self.client_capacity_left()
                if batch_size is None or batch_size > MAX_ACCEPT_BATCH:
                    batch_size = MAX_ACCEPT_BATCH
                for i in range(batch_size):
                    try:
                        conn_sock = sock.accept()
                        self.update_source(source)
                        conn = Connection(self.sd, conn_sock, self.event_loop,
                                          self, self.conn_handshake_completed,
                                          self.proto_version_limit,
                                          self.idle_limit, self.client_idle,
                                          self.conn_terminated)
                        self.clientless_connections.add(conn)
                        self.schedule_clean_out()
                    except xcm.error as e:
                        self.update_source(source)
                        if e.errno != errno.EAGAIN:
                            self.debug("Error accepting client: %s" % e,
                                       LogCategory.PROTOCOL)
                        break

    def timer_changed(self):
        timeout = self.timer_manager.next_timeout()

        if timeout is not None:
            self.timer_source.set_timeout(timeout)
        else:
            self.timer_source.clear_timeout()

    def timer_activate(self):
        self.timer_manager.process()

    def update_source(self, source):
        if self.max_clients_reached():
            condition = 0
        else:
            condition = xcm.SO_ACCEPTABLE
        source.update(condition)

    def conn_handshake_completed(self, conn):
        self.clientless_connections.remove(conn)
        self.client_connections[conn.client_id] = conn

    def conn_terminated(self, conn):
        if self.max_clients_reached():
            for source in self.server_socks.keys():
                source.update(xcm.SO_ACCEPTABLE)

        if conn.handshaked:
            del self.client_connections[conn.client_id]
        else:
            self.clientless_connections.remove(conn)

    def client_idle(self, client, warning):
        conn = self.client_connections[client.client_id]
        if warning:
            conn.check_idle()
        else:
            self.debug("Connection for client %d timed out" %
                       client.client_id, LogCategory.PROTOCOL)
            conn.terminate()

    def close_server_socks(self):
        for sock in self.server_socks.values():
            sock.close()

    def terminate(self):
        conns = [conn for conn in chain(self.clientless_connections,
                                        self.client_connections.values())]

        for conn in conns:
            conn.terminate()

        assert len(self.clientless_connections) == 0
        assert len(self.client_connections) == 0

        for stream in self.server_socks.keys():
            self.event_loop.remove(stream)
        self.close_server_socks()


def create(name, sockets, max_user_resources, max_total_resources,
           proto_version_limit, idle_limit, event_loop):
    return Server(name, sockets, max_user_resources, max_total_resources,
                  proto_version_limit, idle_limit, event_loop)
