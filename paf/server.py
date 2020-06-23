import logging
import json
from collections import deque
import errno
import time

import paf.xcm as xcm
import paf.proto as proto
from paf.proto import ProtocolError
import paf.sd as sd
import paf.filter
import paf.props as props
import paf.eventloop as eventloop

logger = logging.getLogger()

class Transaction:
    def __init__(self, debug):
        self.debug = debug
    def request(self, in_wire_msg):
        try:
            in_msg = json.loads(in_wire_msg.decode('utf-8'))
            ta_cmd = proto.FIELD_TA_CMD.pull(in_msg)
            self.ta_id = proto.FIELD_TA_ID.pull(in_msg)
            msg_type = proto.FIELD_MSG_TYPE.pull(in_msg)

            if msg_type != proto.MSG_TYPE_REQUEST:
                raise proto.ProtocolError("Incoming request is of invalid "
                                          "type \"%s\"" % msg_type)

            self.ta_type = self.lookup_type(ta_cmd)

            self.debug("Processing \"%s\" command request with transaction "
                       "id %d." % (self.ta_type.cmd, self.ta_id))

            request_args = []
            for field in self.ta_type.request_fields:
                request_args.append(field.pull(in_msg))

            opt_request_args = {}
            for field in self.ta_type.opt_request_fields:
                arg = field.pull(in_msg, opt=True)
                if arg != None:
                    opt_request_args[field.name] = arg

            if len(in_msg) > 0:
                raise ProtocolError("Request contains unknown fields: %s" % \
                                    in_msg)

            return request_args, opt_request_args
        except ValueError:
            raise ProtocolError("Error JSON decoding incoming message")
    def lookup_type(self, ta_cmd):
        t = proto.TA_TYPES.get(ta_cmd)
        if t == None:
            raise proto.ProtocolError("Client issued unknown command "
                                      "\"%s\"" % ta_cmd)
        return t
    def response(self, msg_type, fields, opt_fields, *args, **optargs):
        out_msg = {}

        self.debug("Responding with message type \"%s\" in transaction %d." % \
                   (msg_type, self.ta_id))

        proto.FIELD_TA_CMD.put(self.ta_type.cmd, out_msg)
        proto.FIELD_TA_ID.put(self.ta_id, out_msg)
        proto.FIELD_MSG_TYPE.put(msg_type, out_msg)

        assert len(args) == len(fields)
        for i, field in enumerate(fields):
            field.put(args[i], out_msg)

        for opt_field in opt_fields:
            opt_name = opt_field.python_name()
            if opt_name in optargs:
                opt_value = optargs.get(opt_name)
                if opt_value != None:
                    opt_field.put(opt_value, out_msg)
                del optargs[opt_name]
        assert len(optargs) == 0

        out_wire_msg = json.dumps(out_msg).encode('utf-8')
        return out_wire_msg
    def complete(self, *args, **optargs):
        return self.response(proto.MSG_TYPE_COMPLETE,
                             self.ta_type.complete_fields,
                             self.ta_type.opt_complete_fields,
                             *args, **optargs)
    def fail(self, *args, **optargs):
        return self.response(proto.MSG_TYPE_FAIL,
                             self.ta_type.fail_fields,
                             self.ta_type.opt_fail_fields,
                             *args, **optargs)
    def accept(self, *args, **optargs):
        assert self.ta_type.ia_type == proto.InteractionType.MULTI_RESPONSE
        return self.response(proto.MSG_TYPE_ACCEPT,
                             self.ta_type.accept_fields,
                             self.ta_type.opt_accept_fields,
                             *args, **optargs)
    def notify(self, *args, **optargs):
        assert self.ta_type.ia_type == proto.InteractionType.MULTI_RESPONSE
        return self.response(proto.MSG_TYPE_NOTIFY,
                             self.ta_type.notify_fields,
                             self.ta_type.opt_notify_fields,
                             *args, **optargs)

MAX_SEND_BATCH = 64
MAX_ACCEPT_BATCH = 16
SOFT_OUT_WIRE_LIMIT = 128

class Client:
    def __init__(self, sd, conn_sock, event_loop, server, term_cb):
        self.client_id = None
        self.client_ref = conn_sock.get_attr("xcm.remote_addr")
        self.sd = sd
        self.conn_sock = conn_sock
        self.conn_source = eventloop.XcmSource(conn_sock)
        self.out_wire_msgs = deque()
        self.event_loop = event_loop
        self.server = server
        self.term_cb = term_cb
        self.event_loop.add(self.conn_source, self.activate)
        self.subscriptions = {}
        self.connect_time = time.time()
        self.update_source()
        logger.info("Accepted new client from \"%s\"." %  self.client_ref)
    def format_entry(self, msg):
        if self.client_id != None:
            client = "%d" % self.client_id
        else:
            client = "unknown"
        return "<client %s> %s" % (client, msg)
    def debug(self, msg):
        logger.debug(self.format_entry(msg))
    def info(self, msg):
        logger.info(self.format_entry(msg))
    def warning(self, msg):
        logger.warning(self.format_entry(msg))
    def sendable(self):
        return len(self.out_wire_msgs) > 0
    def receivable(self):
        # avoid accepting more work (requests) if already a lot of
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
            self.debug("Error on socket send or receive: %s." % e)
            self.terminate()
        except proto.Error as e:
            self.warning("%s." % str(e))
            self.terminate()
    def try_send(self):
        for i in range(0, min(MAX_SEND_BATCH, len(self.out_wire_msgs))):
            try:
                out_wire_msg = self.out_wire_msgs.popleft()
                self.conn_sock.send(out_wire_msg)
                self.debug("Sent message: %s." % out_wire_msg)
            except xcm.error as e:
                if e.errno != errno.EAGAIN:
                    raise
                self.out_wire_msgs.appendleft(out_wire_msg)
    def try_receive(self):
        try:
            in_wire_msg = self.conn_sock.receive()
            if len(in_wire_msg) == 0:
                raise xcm.error(0, "Connection closed")
            self.debug("Received message: %s" % in_wire_msg)
            self.request(in_wire_msg)
        except xcm.error as e:
            if e.errno != errno.EAGAIN:
                raise
    def request(self, in_wire_msg):
        ta = Transaction(self.debug)
        args, optargs = ta.request(in_wire_msg)
        if ta.ta_type.cmd == proto.CMD_HELLO or self.client_id != None:
            for response in self.invoke_handler(ta, args, optargs):
                self.respond(response)
        else:
            self.warning("Client attempted to issue \"%s\" before issuing "
                         "\"%s\"." % (ta.ta_type.cmd, proto.CMD_HELLO))
            ta.respond(ta.fail(fail_reason = proto.FAIL_REASON_NO_HELLO))
    def invoke_handler(self, ta, args, optargs):
        fun_name = "%s_request" % ta.ta_type.cmd.replace("-", "_")
        fun = getattr(self, fun_name)
        return fun(ta, *args, **optargs)
    def hello_request(self, ta, client_id, min_version, max_version):
        self.client_id = client_id
        self.debug("Client 0x%x supports protocol versions between "
                   "%d and %d." % (client_id, min_version, max_version))
        if proto.VERSION >= min_version and proto.VERSION <= max_version:
            yield ta.complete(proto.VERSION)
        else:
            self.warning("Client doesn't support protocol version %d." % \
                         proto.VERSION)
            yield ta.fail(fail_reason = \
                          proto.FAIL_REASON_UNSUPPORTED_PROTOCOL_VERSION)
    def subscribe_request(self, ta, sub_id, filter=None):
        try:
            if self.sd.has_subscription(sub_id):
                yield ta.fail(fail_reason = \
                              proto.FAIL_REASON_SUBSCRIPTION_ID_EXISTS)
                return
            if filter != None:
                filter = paf.filter.parse(filter)
            self.sd.create_subscription(sub_id, filter, self.client_id,
                                        self.subscription_triggered)
            self.subscriptions[sub_id] = ta
            log_msg = "Assigned subscription id %d to new subscription" % \
                      sub_id
            if filter != None:
                log_msg += " with filter \"%s\"" % filter
            log_msg += "."
            self.debug(log_msg)
            yield ta.accept()
            # Subscription creation and activation must be separate,
            # to avoid having the match callback called before the
            # server has gotten the subscription id.
            self.sd.activate_subscription(sub_id)
        except paf.filter.ParseError as e:
            self.debug("Received subscription request with malformed "
                       "filter: %s." % str(e))
            yield ta.fail(fail_reason = proto.FAIL_REASON_INVALID_FILTER_SYNTAX)
    def unsubscribe_request(self, ta, sub_id):
        if sub_id in self.subscriptions:
            sub_ta = self.subscriptions[sub_id]
            self.sd.remove_subscription(sub_id)
            del self.subscriptions[sub_id]
            yield sub_ta.complete()
            yield ta.complete()
            self.debug("Canceled subscription %d in transaction %d." % \
                       (sub_id, sub_ta.ta_id))
        else:
            if sub_id in self.sd.subscriptions:
                yield ta.fail(fail_reason = \
                              proto.FAIL_REASON_NOT_SUBSCRIPTION_OWNER)
                self.debug("Attempted to unsubscribe to subscription %d "
                           "not owned by this client." % sub_id)
            else:
                yield ta.fail(fail_reason = \
                              proto.FAIL_REASON_NON_EXISTENT_SUBSCRIPTION_ID)
                self.debug("Attempted to unsubscribe to non-existent "
                           "subscription %d." % sub_id)
    def subscriptions_request(self, ta):
        yield ta.accept()
        for client in self.server.clients:
            for sub_id in client.subscriptions:
                sub = self.sd.subscriptions[sub_id]
                if sub.filter != None:
                    filter = str(sub.filter)
                else:
                    filter = None
                yield ta.notify(sub_id, client.client_id, filter=filter)
        yield ta.complete()
    def services_request(self, ta, filter=None):
        try:
            if filter != None:
                filter = paf.filter.parse(filter)
                self.debug("Accepted list request for services "
                           "matching %s." % str(filter))
            else:
                self.debug("Accepted list request for all services.")
            yield ta.accept()
            for service in self.sd.get_services():
                if filter == None or filter.match(service.props):
                    yield ta.notify(service.service_id, service.generation,
                                    service.props, service.ttl, service.owner,
                                    orphan_since=service.orphan_since)
            yield ta.complete()
        except paf.filter.ParseError as e:
            self.debug("Received list services request with malformed "
                       "filter: %s." % str(e))
            yield ta.fail(fail_reason = \
                          proto.FAIL_REASON_INVALID_FILTER_SYNTAX)
    def publish_request(self, ta, service_id, generation, service_props, ttl):
        service = self.sd.publish(service_id, generation, service_props,
                                  ttl, self.client_id)

        if service == None:
            self.warning("Re-publication of id %x did not cause a change." % \
                         service_id)
        elif service.before == None:
            self.debug("Published new service with id %x, generation %d, "
                       "props %s and TTL %d s." % \
                       (service_id, generation, props.to_str(service_props), \
                        ttl))
        else:
            log_msg = "Re-published service with id %x. Generation %d -> %d." \
                % (service_id, service.before.generation, service.generation)
            if service.before.is_orphan():
                      log_msg += " Replacing orphan."
            if service.props != service.before.props:
                log_msg += " Properties changed from %s to %s." \
                           % (props.to_str(service.before.props),
                              props.to_str(service.props))
            if service.ttl != service.before.ttl:
                log_msg += " TTL changed from %d to %d s." \
                           % (service.before.ttl, service.ttl)
            if service.owner != service.before.owner:
                log_msg += " Owner is changed from %x to %x." \
                           % (service.before.owner, service.owner)
            self.debug(log_msg)
            if generation < service.before.generation:
                self.warning("Re-published service with lower generation "
                             "(%d replacing %d)." % (generation, \
                                                     service.before.generation))
        yield ta.complete()
    def unpublish_request(self, ta, service_id):
        if not self.sd.has_service(service_id):
            self.debug("Attempted to unpublish non-existent service "
                       "id %d" % service_id)
            yield ta.fail(fail_reason = \
                          proto.FAIL_REASON_NON_EXISTENT_SERVICE_ID)
        else:
            service_props = self.sd.get_service(service_id).props
            self.sd.unpublish(service_id)
            self.debug("Unpublished service %s with service id %x." % \
                       (props.to_str(service_props), service_id))
            yield ta.complete()
    def ping_request(self, ta):
        yield ta.complete()
    def clients_request(self, ta):
        yield ta.accept()
        for client in self.server.clients:
            if client.client_id != None:
                yield ta.notify(client.client_id, client.client_ref,
                                int(client.connect_time))
        yield ta.complete()
    def subscription_triggered(self, sub_id, match_type, service):
        subscription = self.server.sd.get_subscription(sub_id)
        if subscription.filter != None:
            filter_s = "with filter %s" % subscription.filter
        else:
            filter_s = "without filter"
        self.debug("Subscription id %d %s received %s event by "
                   "service id %x with properties %s" % \
                   (sub_id, filter_s, match_type.name,
                    service.service_id, props.to_str(service.props)))
        proto_match_type = getattr(proto, "MATCH_TYPE_%s" %
                                    match_type.name)
        ta = self.subscriptions[sub_id]
        if match_type == sd.MatchType.DISAPPEARED:
            self.respond(ta.notify(proto_match_type, service.service_id))
        else:
            self.respond(ta.notify(proto_match_type, service.service_id,
                                   generation=service.generation,
                                   service_props=service.props,
                                   ttl=service.ttl, client_id=service.owner,
                                   orphan_since=service.orphan_since))
        self.update_source()
    def respond(self, out_wire_msg):
        self.out_wire_msgs.append(out_wire_msg)
    def terminate(self):
        self.info("Disconnected.")
        self.sd.client_disconnect(self.client_id)
        self.event_loop.remove(self.conn_source)
        self.conn_sock.close()
        self.conn_sock = None
        self.conn_source = None
        self.term_cb(self)

class Server:
    def __init__(self, server_addrs, max_clients, event_loop):
        self.sd = sd.ServiceDiscovery(self.check_orphans)
        self.max_clients = max_clients
        self.event_loop = event_loop
        self.server_socks = {}
        for server_addr in server_addrs:
            sock = xcm.server(server_addr)
            sock.set_blocking(False)
            source = eventloop.XcmSource(sock)
            self.server_socks[source] = sock
        for source, sock in self.server_socks.items():
            self.event_loop.add(source, self.sock_activate)
        self.orphan_timer = eventloop.Source()
        self.event_loop.add(self.orphan_timer, self.timer_activate)
        self.clients = []
        self.next_id = 0
        self.update_sources()
    def sock_activate(self):
        for sock in self.server_socks.values():
            if len(self.clients) == self.max_clients:
                sock.finish()
            else:
                try:
                    left = self.max_clients - len(self.clients)
                    for i in range(0, min(MAX_ACCEPT_BATCH, left)):
                        conn_sock = sock.accept()
                        client = Client(self.sd, conn_sock, self.event_loop,
                                        self, self.client_terminated)
                        self.clients.append(client)
                except xcm.error as e:
                    if e.errno != errno.EAGAIN:
                        logger.debug("Error accepting client: %s" % e)
        self.update_sources()
    def timer_activate(self):
        timed_out = self.sd.purge_orphans()
        for orphan_id in timed_out:
            logger.debug("Timed out orphan service %x." % orphan_id)
        self.orphan_timer.set_timeout(self.sd.next_orphan_timeout())
    def update_sources(self):
        if len(self.clients) < self.max_clients:
            condition = xcm.SO_ACCEPTABLE
        else:
            condition =  0
        for source in self.server_socks:
            source.update(condition)
    def client_terminated(self, client):
        self.clients.remove(client)
        self.update_sources()
    def terminate(self):
        for client in self.clients:
            client.terminate()
        for stream, sock in self.server_socks.items():
            self.event_loop.remove(stream)
            sock.close()
    def check_orphans(self, change_type, after, before):
        if (after != None and after.is_orphan()) or \
           (before != None and before.is_orphan()):
            self.orphan_timer.set_timeout(self.sd.next_orphan_timeout())

def create(addrs, max_clients, event_loop):
    return Server(addrs, max_clients, event_loop)
