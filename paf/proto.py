from enum import Enum
import collections

VERSION = 2

MSG_TYPE_REQUEST = 'request'
MSG_TYPE_ACCEPT = 'accept'
MSG_TYPE_NOTIFY = 'notify'
MSG_TYPE_COMPLETE = 'complete'
MSG_TYPE_FAIL = 'fail'

CMD_HELLO = 'hello'
CMD_SUBSCRIBE = 'subscribe'
CMD_UNSUBSCRIBE = 'unsubscribe'
CMD_SUBSCRIPTIONS = 'subscriptions'
CMD_SERVICES = 'services'
CMD_PUBLISH = 'publish'
CMD_UNPUBLISH = 'unpublish'
CMD_PING = 'ping'
CMD_CLIENTS = 'clients'

class Field:
    def __init__(self, name):
        self.name = name
    def python_name(self):
        return self.name.replace('-', '_')
    def pull(self, in_msg, opt=False):
        value = in_msg.get(self.name)
        if value == None and opt:
            return None
        if value == None:
            raise ProtocolError("Message is missing required "
                                "field \"%s\"" % self.name)
        del in_msg[self.name]
        return value
    def put(self, value, out_msg):
        out_msg[self.name] = value

class StringField(Field):
    pass

class UIntField(Field):
    def pull(self, value, opt=False):
        value = Field.pull(self, value, opt=opt)
        if value == None:
            return None
        if not isinstance(value, int):
            raise ProtocolError("Message field %s is not an integer" % \
                                self.name)
        if value < 0:
            raise ProtocolError("Message field %s has the negative " \
                                "value %d" % (self.name, value))
        return value

class NumberField(Field):
    def pull(self, value, opt=False):
        value = Field.pull(self, value, opt=opt)
        if value == None:
            return None
        if not isinstance(value, (int, float)):
            raise ProtocolError("Message field %s is not a number" % \
                                self.name)
        return value

class PropsField(Field):
    def pull(self, in_msg, opt=False):
        wire_props = Field.pull(self, in_msg, opt=opt)
        if wire_props == None:
            return None
        if not isinstance(wire_props, dict):
            raise ProtocolError("Value for field %s is not a dictionary" % \
                                self.name)
        return self.from_wire(wire_props)
    def from_wire(self, wire_props):
        props = collections.defaultdict(set)
        for key, values in wire_props.items():
            if not isinstance(key, str):
                raise ProtocolError("Service property key is not a string")
            if not isinstance(values, list):
                raise ProtocolError("Service property value is not a list")
            for value in values:
                if not isinstance(value, (str, int)):
                    raise ProtocolError("Service property value is neither "
                                        "string nor integer")
                props[key].add(value)
        return props
    def put(self, props, out_msg):
        out_msg[self.name] = self.to_wire(props)
    def to_wire(self, props):
        wire_props = collections.defaultdict(list)
        for key, values in props.items():
            for value in values:
                wire_props[key].append(value)
        return wire_props

FIELD_TA_CMD = StringField('ta-cmd')
FIELD_TA_ID = UIntField('ta-id')
FIELD_MSG_TYPE = StringField('msg-type')

FIELD_FAIL_REASON = StringField('fail-reason')

FIELD_PROTO_MIN_VERSION = UIntField('protocol-minimum-version')
FIELD_PROTO_MAX_VERSION = UIntField('protocol-maximum-version')
FIELD_PROTO_VERSION = UIntField('protocol-version')

FIELD_SERVICE_PROPS = PropsField('service-props')
FIELD_SERVICE_ID = UIntField('service-id')
FIELD_GENERATION = UIntField('generation')

FIELD_TTL = UIntField('ttl')
FIELD_ORPHAN_SINCE = NumberField('orphan-since')

FIELD_SUBSCRIPTION_ID = UIntField('subscription-id')

FIELD_FILTER = StringField('filter')

FIELD_CLIENT_ID = UIntField('client-id')
FIELD_CLIENT_ADDR = StringField('client-address')
FIELD_TIME = UIntField('time')

FIELD_MATCH_TYPE = StringField('match-type')

MATCH_TYPE_APPEARED = 'appeared'
MATCH_TYPE_MODIFIED = 'modified'
MATCH_TYPE_DISAPPEARED = 'disappeared'

FAIL_REASON_NO_HELLO = 'no-hello'
FAIL_REASON_CLIENT_ID_EXISTS = 'client-id-exists'
FAIL_REASON_INVALID_FILTER_SYNTAX = 'invalid-filter-syntax'
FAIL_REASON_SUBSCRIPTION_ID_EXISTS = 'subscription-id-exists'
FAIL_REASON_NON_EXISTENT_SUBSCRIPTION_ID = 'non-existent-subscription-id'
FAIL_REASON_NON_EXISTENT_SERVICE_ID = 'non-existent-service-id'
FAIL_REASON_UNSUPPORTED_PROTOCOL_VERSION = 'unsupported-protocol-version'
FAIL_REASON_PERMISSION_DENIED = 'permission-denied'
FAIL_REASON_OLD_GENERATION = 'old-generation'
FAIL_REASON_INSUFFICIENT_RESOURCES = 'insufficient-resources'

class InteractionType(Enum):
    SINGLE_RESPONSE = 0
    MULTI_RESPONSE = 1

TA_TYPES = {}

class TransactionType:
    def __init__(self, cmd, ia_type, request_fields=[],
                 opt_request_fields=[],
                 accept_fields=[],
                 opt_accept_fields=[],
                 notify_fields=[],
                 opt_notify_fields=[],
                 complete_fields=[],
                 opt_complete_fields=[],
                 fail_fields=[],
                 opt_fail_fields=[]):
        self.cmd = cmd
        self.ia_type = ia_type
        self.request_fields = request_fields
        self.opt_request_fields = opt_request_fields
        self.accept_fields = accept_fields
        self.opt_accept_fields = opt_accept_fields
        self.notify_fields = notify_fields
        self.opt_notify_fields = opt_notify_fields
        self.complete_fields = complete_fields
        self.opt_complete_fields = opt_complete_fields
        self.fail_fields = fail_fields
        self.opt_fail_fields = opt_fail_fields
        TA_TYPES[cmd] = self

TA_HELLO = TransactionType(
    CMD_HELLO,
    InteractionType.SINGLE_RESPONSE,
    request_fields = [FIELD_CLIENT_ID, FIELD_PROTO_MIN_VERSION,
                      FIELD_PROTO_MAX_VERSION],
    complete_fields = [FIELD_PROTO_VERSION],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_SUBSCRIBE = TransactionType(
    CMD_SUBSCRIBE,
    InteractionType.MULTI_RESPONSE,
    request_fields = [FIELD_SUBSCRIPTION_ID],
    opt_request_fields = [FIELD_FILTER],
    notify_fields = [FIELD_MATCH_TYPE, FIELD_SERVICE_ID],
    opt_notify_fields = [FIELD_GENERATION, FIELD_SERVICE_PROPS, FIELD_TTL,
                         FIELD_CLIENT_ID, FIELD_ORPHAN_SINCE],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_UNSUBSCRIBE = TransactionType(
    CMD_UNSUBSCRIBE,
    InteractionType.SINGLE_RESPONSE,
    request_fields = [FIELD_SUBSCRIPTION_ID],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_SUBSCRIPTIONS = TransactionType(
    CMD_SUBSCRIPTIONS,
    InteractionType.MULTI_RESPONSE,
    notify_fields = [FIELD_SUBSCRIPTION_ID, FIELD_CLIENT_ID],
    opt_notify_fields = [FIELD_FILTER]
)

TA_SERVICES = TransactionType(
    CMD_SERVICES,
    InteractionType.MULTI_RESPONSE,
    opt_request_fields = [FIELD_FILTER],
    notify_fields = [FIELD_SERVICE_ID, FIELD_GENERATION, FIELD_SERVICE_PROPS,
                     FIELD_TTL, FIELD_CLIENT_ID],
    opt_notify_fields = [FIELD_ORPHAN_SINCE],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_PUBLISH = TransactionType(
    CMD_PUBLISH,
    InteractionType.SINGLE_RESPONSE,
    request_fields = [FIELD_SERVICE_ID, FIELD_GENERATION,
                      FIELD_SERVICE_PROPS, FIELD_TTL],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_UNPUBLISH = TransactionType(
    CMD_UNPUBLISH,
    InteractionType.SINGLE_RESPONSE,
    request_fields = [FIELD_SERVICE_ID],
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_PING = TransactionType(
    CMD_PING,
    InteractionType.SINGLE_RESPONSE,
    opt_fail_fields = [FIELD_FAIL_REASON]
)

TA_CLIENTS = TransactionType(
    CMD_CLIENTS,
    InteractionType.MULTI_RESPONSE,
    notify_fields = [FIELD_CLIENT_ID, FIELD_CLIENT_ADDR, FIELD_TIME]
)

class Error(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class ProtocolError(Error):
    def __init__(self, message):
        Error.__init__(self, message)

class TransportError(Error):
    def __init__(self, message):
        Error.__init__(self, message)
