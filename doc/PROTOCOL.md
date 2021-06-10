# Pathfinder Protocol Specification

## Conventions

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "MAY", and "OPTIONAL" in this document are to
be interpreted as described in [RFC2119].

Literal protocol text is marked like `this`.

## Overview

The Pathfinder protocol provides an interface to a light-weight
directory service, intended for sharing information about services in
a distributed system.

The Pathfinder protocol has primitives for publishing information, and
subscribing to information being added, modified, or removed. The
protocol includes a query language and an asynchronous notification
mechanism. It also makes provisions to allow for "stateless" servers,
in the sense the use of server-side non-volatile storage (or the
equivalent) is optional. The authoritative state is kept with the
originating client, and server state may thus be recreated, if needed.

## Document Version

This is version 2.0.0-draft.1 of the Pathfinder protocol
specification.

This is an early draft and may well include inconsistencies and other
types of errors.

## Protocol Version

This document describes version 2 of the Pathfinder protocol.

## Data Model

### Integers

All integers used in the Pathfinder protocol MUST be within the value
range of a two-complement 64-bit signed integer (i.e. from -2^63 to
2^63 - 1 [inclusive]). Non-negative integers MUST be within the
non-negative part of this range.

### Strings

All strings used in the Pathfinder protocol MUST NOT contain NUL
characters, but are otherwise allowed to contain any characters that
are allowed by JSON.

### Unique Identifiers

Identifiers for clients, services, subscriptions MUST be non-negative
integers, unique within that [domain](#domains) and object
type. Protocol transaction identifiers need only be unique among
concurrent transactions on that connection.

Clients MUST use the same client, service and subscription identifiers
across different server instances, in case [multiple servers are
used](#multiple-server-domains) to server the same domain.

Clients MAY use a properly-seeded high-quality pseudo random number
generator (PRNG) to select identifiers, provided the risk of
collisions are deemed low enough for application in question. This
method requires no coordination between clients.

### Service Records

A Pathfinder service record (or short, service) consist of a globally
unique service id, a set of properties, a time-to-live (TTL) field, a
generation number and a reference to the owner, in the form of a
client identifier.

The service identifier MUST remain constant.

#### Properties

A Pathfinder service record property (or short, property) consists of
a name and a value.

There MAY be multiple properties with the same name in a service
record (i.e., the properties constitute a multimap).

The property name is a UTF-8 string, and the value is either a UTF-8
string or an integer.

Service properties MAY be modified.

#### TTL

The service record TTL specifies how quickly (in seconds) a service
record should be removed, after the owning client has crashed, exited
or lost network connectivity with the server.

The TTL MAY be modified.

#### Generation

At the time of creation, the client MUST assign the service record a
generation number, in the form of a non-negative integer. The initial
generation number SHOULD be 0.

In case the associated properties and/or TTL are changed, the
generation number MUST also be increased. It SHOULD be incremented by
one.

The generation number provides a way for servers and clients to
determine which of two instances of the same service is the most
recent. This become important when multiple servers are used to serve
the same domain. In such a scenario, depending on network
characteristics and server availability and latency, information about
different instances of the same service (matching some subscription)
may arrive out of order to a client, from different servers. The
generation number SHOULD be used to discard notifications containing
outdated service information.

#### Owner

The client publishing a service MUST become its owner. The ownership
MUST be transferred, if the service is being republished by another
client, with a different client identifier. The server MAY prohibit
such operations (e.g., since it would constitute an access control
violation).

An owner is identified by its client identifier.

### Client and Servers

The distribution of responsibility between a Pathfinder client and
server is as customary; the client issues commands, which the server
executes. The Pathfinder protocol-level client SHOULD also the client
on the transport protocol level.

Client and server in this document refers to the Pathfinder protocol
endpoints, not the processes that implements them.

An client application process MAY act as multiple clients.

A server process MAY act as multiple Pathfinder protocol-level
servers, but in that case MUST use distinct transport protocol-level
server endpoints for each Pathfinder protocol-level server.

#### Transient Clients

A client MAY use only a short-lived connection to publish services
and/or issue queries.

Such a client SHOULD reconnect and republish its services no later
than half of the lowest of its published services' TTL has passed,
since it disconnected.

To avoid excessive load on the server, the TTL for services owned by
transient clients SHOULD be sized appropriately.

In case the server's state is lost (e.g. due to a restart of a server
which doesn't store the service directory on non-volatile storage),
the service will seem unavailable until the next time the transient
client reconnects.

### Domains

A server MUST present a single, nonhierarchical service record
namespace. Barring any security-related filtering or partitioning, all
clients SHOULD see all services, subscriptions, and other clients.

A Pathfinder domain is either one such namespace, or, in case of a
multiple-server domain, the union of the several servers' name spaces.
This grouping of server instances MAY be a purely client-side
convention, and thus the servers are unaware of each others existence
and operate independently.

A Pathfinder server MUST serve exactly one domain.

#### Multiple Server Domains

A client connected to multiple servers serving the same domain SHOULD
identify itself with the same client identifier across all servers.

A client publishing the same service on multiple servers serving the
same domain, SHOULD use the same service data and meta data, such as
the service identifier and generation number, for the same service,
for every publication.

A client issuing a subscription on multiple servers serving the same
domain, SHOULD use the same subscription identifier for the same
subscription on each server.

A client MUST ignore subscription matches for a service record of an
lower (i.e. older) generation number than from a previously received
match, for that service record.

A client SHOULD consider a service having appeared when it has
received at least one an appeared type match.

A client SHOULD consider a service modified when it has received at
least one modified type match.

A client SHOULD consider a service removed only after receiving a
removed type match from all subscriptions in which the service has
appeared.

### Service Lifetime

A client SHOULD only keep a service published, for as long as this
service is deemed available to potential users.

If the transport protocol connection between the client and the server
is lost (e.g., because of a server crash or a loss of network
connectivity), a reconnecting client MUST republish all its services.

The server MUST allow a client to publish a service, with identical
service identifier, properties, TTL, and owning client identifier to
an already-existing service. However, it MAY prohibit such a republish
operation for security reasons (e.g., transport protocol level
authorization suggests the two client identifiers should not be
allowed to overwrite each other services), if the new and
already-existing service are not identical.

The lifetime of a service is tied to the owning client being connected
to the server. If the transport connection of a particular client is
closed or lost for any other reason, the server SHOULD consider the
service an orphan.

The state transition from non-orphan to orphan, or vice versa, should
be notified in all subscription transaction which have matched that
service. Such notifications MAY be delayed.

After the service's TTL has expired, orphan services should be removed
and the appropriate notifications sent in all subscription
transactions which have matched that service.

### Subscriptions

The Pathfinder protocol provides an asynchronous notification
mechanism, allowing for clients to monitor the presences of services
matching a certain filter.

Using a subscription, a client may ask to be informed about every
service the server knows about matching a particular
filter.

Initially, the client will receive an `appeared` type match, for every
service record matching the filter. In case such a matching record is
subsequently republished in a modified form, the client will receive a
̀modified` type notification (assuming a still-active subscription). A
service is considered modified in case its properties, TTL, owning
client, and/or orphan status is changed.

A server MAY delay subscription notifications for efficiency reasons
(e.g. to avoid excessive amount of TTL modified notifications in face
of a likely-transient network issue). Such a delay SHOULD only be a
fraction of the TTL of the services in question.

If a matching service is removed, the client will receive a
`disappeared` type notification.

## Transport Protocol

The Pathfinder protocol MUST be run over a reliable, point-to-point,
transport protocol connection. The transport protocol MAY provide byte
stream type service, or one that preserve message boundaries. In the
latter case, a transport protocol message MUST contain exactly one
Pathfinder protocol message.

The transport protocol messages MUST be delivered in the order they
were sent, on a particular connection.

The relationship between a Pathfinder client and a connection to a
Pathfinder server SHOULD be 1:1.

### Message Size Limitations

The transport protocol and/or the Pathfinder client or server MAY
impose hard or administrative limitations on Pathfinder protocol
message size.

### Dead Peer Detection

The transport protocol SHOULD provide some mechanism to detect lost
network connectivity, a crashed remote host or remote process, in a
timely manner.

In case TCP is used as the underlying transport protocol, TCP keep
alive MAY be used.

## Filter Representation

The Pathfinder protocol uses a string representation for the service
record filters used in various protocol operations.

The filters are specified using a prefix notation and has a syntax
much resembling that of Lightweight Directory Access Protocol (LDAP),
as specified in [RFC4515]. The Pathfinder representation differs in
some significant ways, in particular when it comes to escaping special
characters.

A Pathfinder filter is defined by the following grammar, in Augmented
Backus-Naur Form (ABNF) [RFC5234]:

    filter         = LPAREN filtercomp RPAREN
    filtercomp     = and / or / not / item
    and            = AMPERSAND filterlist
    or             = VERTBAR filterlist
    not            = EXCLAMATION filter
    filterlist     = 1*filter
    item           = simple / present / substring
    simple         = key op value
    key            = string
    value          = string
    op             = equal / greater / less
    present        = key EQUALS ASTERISK
    substring      = key EQUALS [initial] any [final]
    initial        = string
    any            = ASTERISK *(string ASTERISK)
    final          = string
    string         = 0*(unescaped / escaped)
    unescaped      = %x01-20 / %x22-25 / %x27 / %x2B-3B / %x3F-5B /
                     %x5D-7B / %x7E-10FFFF ; all except special and NUL
    escaped        = ESC special
    special        = EXCLAMATION / AMPERSAND / ASTERISK / LPAREN /
                     RPAREN / LANGLE / EQUALS / RANGLE / ESC / VERTBAR
    integer        = [ MINUS ] digits
    digits         = ZERO / ( DIGIT1-9 *DIGIT )
    ZERO           = %x30    ; 0
    DIGIT1-9       = %x31-39 ; 1-9
    MINUS          = %x2D    ; -
    EXCLAMATION    = %x21    ; !
    AMPERSAND      = %x26    ; &
    LPAREN         = %x28    ; (
    RPAREN         = %x29    ; )
    ASTERISK       = %x2A    ; *
    LANGLE         = %x3C    ; <
    EQUALS         = %x3D    ; =
    RANGLE         = %x3E    ; >
    ESC            = %x5C    ; \
    VERTBAR        = %x7C    ; |

#### Examples

    (name=service-a)
    (&(name=service-a)(version>11))

    (game= a space adventure )

## Message Format

The Pathfinder protocol uses JSON as specified in [RFC7159] for
client-server information exchange.

## Message Field

A field is coded as a member (i.e. key-value pair) in the message JSON
object. The member key is the field name, and the value the field's
value.

### Field Value Types

The message field value MUST be of one of the following types:

Field Type | Encoding      | Comment
-----------|---------------|-----------------------------------
String     | JSON String   |
Number     | JSON Number   | Integer or floating point.
Props      | JSON Object   |
Int>=0     | JSON Number   | Integer in the range 0 to (1 << 63) - 1 (inclusive).

## Message

The Pathfinder client and server exchange information using protocol
messages.

A message consist of a set of message fields. The same field name MUST
NOT be repeated within the same message.

A protocol message MUST be encoded as a JSON object.

A protocol message MUST NOT contain any fields beyond those specified
as mandatory or optional (i.e not mandatory).

### Mandatory Fields

All messages MUST contain the following fields:

Field Name | Field Type | Description
-----------|------------|-----------------------------------
`ta-cmd`   | String     | Protocol command.
`ta-id`    | Int>=0     | Transaction identifier.
`msg-type` | String     | Message type.

### Message Types

Message Type | Originator | Transaction Type | Description
-------------|------------|-----|------------
`request`    | Client     | All | Request command being executed.
`accept`     | Server     | Multiple response | Command request accepted.
`notify`     | Server     | Multiple response | Notification.
`complete`   | Server     | All | Command successfully completed.
`fail`       | Server     | All | Command failed.

## Transactions

All protocol messages MUST be sent as a part of a Pathfinder protocol
command transaction (short, transaction).

There are two types of transactions; single and multiple response.

All transactions MUST be initiated by the client.

All transactions MUST be considered terminated in case the transport
connection is closed.

The server MUST allow for multiple concurrent transactions originating
from the same client. It MAY impose an upper bound on the number of
such transactions.

A client issuing a large batch of protocol commands (e.g. publishing
all its services), SHOULD impose a reasonable upper bound on the
number of concurrent transactions.

### Transaction Identifiers

Each transaction is identified with a transaction identifier. Unlike
other Pathfinder protocol identifiers, the transaction identifier
needs only be allocated in such a manner, that no non-terminated
transaction exists with that transaction identifier, on that
connection.

It is the client's responsibility to allocate transaction identifiers.

A client MAY use zero as the first transaction identifier on that
connection, and subsequently add one for each new transaction.

### Single Response Transaction

For single response commands, the client MUST send a `request` type
message as the first message of the transaction. The server MUST
respond with either message with of the type `complete` or
`fail`. Either of these two terminates the transaction.

A `complete` message means the requested command was successfully
executed.

A `fail` message means that the server was unable or unwilling to
perform the requested command.

The client and server MUST NOT send any other messages beyond these as
a part of a single response transaction.

Below is an example of a single response transaction:

    .--------.                                                     .--------.
    | Client |                                                     | Server |
    '--------'                                                     '--------'
        |                                                               |
        |   {"ta-cmd": "ping", "ta-id": 42, "msg-type": "request"}      |
        |-------------------------------------------------------------->|
        |                                                               |
        |   {"ta-cmd": "ping", "ta-id": 42, "msg-type": "complete"}     |
        |<--------------------------------------------------------------|
        |                                                               |

### Multiple Response Transaction

For multiple response commands, the client MUST send a `request` type
message as the first message of the transaction. The server MUST
respond with a message of either the `accept` or `fail` type. `fail`
terminates the transaction.

An `accept` message means the requested command was accepted and is
being executed asynchronously by the server. After `accept`, the
server MAY follow up with one or `notification` messages.

A `fail` message means that the server was unable or unwilling to
perform the requested command, terminating the transaction.

A `complete` message, which MUST follow an `accept` or `notification`
message, means that the requested operation has completed.

The client and server MUST NOT send any other messages beyond these as
a part of a single reponse transaction.

Below is an example of a multiple response transaction. Here, the
client issues a `subscribe` command, and receives two `appeared` type
matches, after which it issues a `unsubscribe` command (a separate
transactions, not show in the sequence). The `unsubscribe` terminates
the `subscribe` transaction, causing the server to generate a
`complete` type message.

    .--------.                                                     .--------.
    | Client |                                                     | Server |
    '--------'                                                     '--------'
        |                                                               |
        | {"ta-cmd": "subscribe", "ta-id": 17, "msg-type": "request",   |
        |  "subscription-id": 15965902, "filter": "(name=foo)"}         |
        |-------------------------------------------------------------->|
        |                                                               |
        | {"ta-cmd": "subscribe", "ta-id": 17, "msg-type": "accept"}    |
        |<--------------------------------------------------------------|
        |                                                               |
        |                                                               |
        | {"ta-cmd": "subscribe", "ta-id": 17, "msg-type": "notify",    |
        |  "match-type": "appeared", "service-id": 8809789524339752160, |
        |  "generation": 0, "service-props": {"name": ["foo"],          |
        |  "address": ["http://1.2.3.4:5555"]}, "ttl": 60,              |
        |  "client-id": 3876347552450328157}                            |
        |<--------------------------------------------------------------|
        |                                                               |
        | {"ta-cmd": "subscribe", "ta-id": 17, "msg-type": "notify",    |
        |  "match-type": "appeared", "service-id": 4316281890261261088, |
        |  "generation": 0, "service-props": {"name": ["foo"],          |
        |  "address": ["https://6.7.8.9:1010"]}, "ttl": 120,            |
        |  "client-id": 213592449598267276}                             |
        |<--------------------------------------------------------------|
        |                                                               |
        | {"ta-cmd": "subscribe", "ta-id": 17, "msg-type": "complete"}  |
        |<--------------------------------------------------------------|

## Protocol Commands

The section lists all commands available in the Pathfinder protocol.

A message field listed as mandatory MUST be included in the message. A
message field listed as not mandatory (i.e., optional) MAY be
included.

### Common Reason Codes

All messages of message type fail MAY include a `fail-reason` field.

All messages of the `fail` message type, with the exception of `hello`
command transactions, may have a `fail-reason` field with one of the
following values:

Reason Code | Description
------------|------------
`no-hello`    | Command issued before hello.
`insufficient-resources` | Unable to complete command due to insufficient resources. May be actual or administrative limit.
`permission-denied` | Permission to complete command was denied.

### Hello Command

**Command:** `hello`  
**Transaction type:** Single response

The `hello` command transaction constitute an initial protocol
handshake. The client provides its client identifier and a range of
protocol versions it supports and the server either responds with a
`complete` type message, in which it informs the client which protocol
version to be used on this connection, or a `fail` type message, if
the client was rejected.

The `hello` command MUST be the first command issued on a particular
connection. Failure to issue `hello` in a timely manner MAY give the
server reason to close the transport connection.

After a client has successfully completed a `hello` command
transaction, it may issue other commands, including another `hello`
command, provided it has the same value for the `client-id`,
`protocol-minimum-version` and `protocol-maximum-version` fields.

A rejected client MAY make more attempts to finish the handshake, with
new `hello` command transactions.

#### Hello Request

In addition to the [mandatory message fields](#mandatory-fields), a
`hello` command `request` message MUST contain the following fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`client-id` | Int>=0 | Yes | Unique client-generated identifier.
`protocol-minimum-version` | Int>=0 | Yes | The minimum protocol version the client supports.
`protocol-maximum-version` | Int>=0 | Yes | The maximum protocol version the client supports.

For this document to potentially apply, the protocol version range
MUST include protocol version 2.

The range is inclusive.

#### Hello Complete

In addition to the [mandatory message fields](#mandatory-fields), a
hello command complete message MUST contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`protocol-version` | Int>=0 | Yes | The actual protocol version to be used on this connection.

For this document to apply, the `protocol-version` MUST be `2`.

#### Hello Fail

In addition to the [mandatory message fields](#mandatory-fields), a
hello command `fail` type message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

##### Hello Fail Reason Codes

A `fail` type message in a hello command transaction MAY include a
`fail-reason` field. In that case, it SHOULD have one of the following
values:

Reason Code | Description
------------|------------
`client-id-exists` | Client identifier is already being used on another connection. This may be a transient error condition, in case the client recently was connected to the server, and the server has yet to terminate the old connection. It may also represent a client identifier collision.
`permission-denied` | Client is temporarily or permanently denied access to this server.
`unsupported-protocol-version` | Provided range of acceptable protocol versions did not overlap with the server's.
`insufficient-resources` | Client is temporarily or permanently rejected due to a lack of server resources.

### Subscribe Command

**Command:** `subscribe`  
**Transaction type:** Multiple response

The `subscribe` command allows a client to subscribe to information
about services. The server MUST send the client `appeared` match type
notifications for matching services already-published at the time of
`subscribe`, or such published thereafter, until the subscription is
terminated.

In addition, the server MUST send `modified` match type notifications
for already-matched services being modified. A service is considered
modified if its properties, TTL, orphan status and/or owning client
has changed.

In case a matched service is removed, the server MUST generate a
`disappeared` match type notification. The reason for the removal may
be the service has been explicitly been unpublished, or that it became
an orphan because the server lost contact with the owning client, and
now the TTL has expired.

To limit the search scope, the client MAY supply a filter in the
[filter string format](#filter-representation).

The `unsubscribe` command is be used to terminate a subscription
transaction.

#### Subscribe Request

In addition to the [mandatory message fields](#mandatory-fields), a
`subscribe` command request message MUST/MAY contain the following
fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`subscription-id` | Int>=0 | Yes | The subscription identifier to be used for this subscription.
`filter` | String | No | Filter to specify which services will match this subscription. Subscriptions with no filter will match all services.

#### Subscribe Accept

A `subscribe` `accept` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscribe Notification

In addition to the [mandatory message fields](#mandatory-fields), a
`subscribe` command `notification` message MUST contain the following
fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`match-type` | String | Yes | Will be `appeared` for new matches, `modified` for already-notified matching services being modified, or `removed` for matched services being unpublished or otherwise removed.
`service-id` | Int>=0 | Yes | Service identifier of appeared/modified/disappeared services.

The following fields MUST/MAY be present in `appeared` and `modified`
type matches:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`generation` | Int>=0 | Yes | Generation number.
`service-props` | Props | Yes | The complete set of service properties.
`ttl` | Int>=0 | Yes | Service time-to-live (in seconds).
`client-id` | Int>=0 | Yes | Service's current owner.
`orphan-since` | Number | No | Time when contact to the owning client was lost, expressed in seconds since the UNIX epoch, in UTC. If left out, the owning client is in contact with the server.

#### Subscribe Complete

A `subscribe` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscribe Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`subscribe` command `fail` type message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

##### Subscribe Fail Reason Codes

In addition to the [common reason codes](#common-reason-codes), the
value of the `fail-reason` field MAY be any one of the following:

Reason Code | Description
------------|------------
`subscription-id-exists` | Subscription identifier already exists.
`invalid-filter-syntax` | Provided filter has an invalid syntax.

### Unsubscribe Command

**Command:** `unsubscribe`  
**Transaction type:** Single response

The `unsubscribe` command terminates an active subscription. It MUST be
issued by the same client, on the same transport connection, as the
subscribe command transactions it terminates.

#### Unsubscribe Request

In addition to the [mandatory message fields](#mandatory-fields), a
subscribe command request message MUST contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`subscription-id` | Int>=0 | Yes | The subscription identifier of the subscription to be terminated.

#### Unsubscribe Complete

A `unsubscribe` `complete` message MUST only have the [mandatory
message fields](#mandatory-fields).

#### Unsubscribe Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`unsubscribe` command `fail` type message MAY contain the following
field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason | String` | No | Failure reason code.

##### Unsubscribe Fail Reason Codes

In addition to the [common reason codes](#common-reason-codes), the
value of the `fail-reason` field MAY be any one of the following:

Reason Code | Description
------------|------------
`non-existent-subscription-id` | Unknown subscription identifier.

### Subscriptions Command

**Command:** `subscriptions`  
**Transaction type:** Multiple response

With the `subscriptions` command, a client asks for information
concerning all currently-known subscriptions on the server, for all
connected clients.

In response to a `subscriptions` command, the server SHOULD only
provide a snapshot of the current state of the server, after which it
terminates the transaction with a `complete` type message.

#### Subscriptions Request

A `subscriptions` `request` message MUST only have the [mandatory
message fields](#mandatory-fields).

#### Subscriptions Accept

A `subscriptions` `accept` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscriptions Notification

In addition to the [mandatory message fields](#mandatory-fields), a
subscriptions command request message MUST/MAY contain the following
fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`subscription-id` | Int>=0 | Yes | Subscription identifier.
`client-id` | Int>=0 | Yes | Client identifier of the subscription owner.
`filter` | String | No | Subscription filter.

#### Subscriptions Complete

A `subscriptions` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscriptions Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`subscriptions` command `fail` type message MAY contain the following
field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

The `fail-reason` value SHOULD take any of the values among the
[common reason codes](#common-reason-codes).

### Services Command

**Command:** `services`  
**Transaction type:** Multiple response

With the `services` command, client asks for information concerning
all currently-known services.

To limit the search scope, the client MAY supply a filter in the
[filter string format](#filter-representation).

In response to a `services` command, the server SHOULD only provide a
snapshot of the current state of the server, after which it terminates
the transaction with a `complete` type message.

#### Services Request

In addition to the [mandatory message fields](#mandatory-fields), a
`services` command request message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`filter` | String | No | Filter to specify which services will be included among the notifications. If left out, all services will match.

#### Services Accept

A `services` `accept` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Services Notification

In addition to the [mandatory message fields](#mandatory-fields), a
`services` command `notification`message MUST/MAY contain the
following fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`service-id` | Int>=0 | Yes | Service identifier.
`generation` | Int>=0 | Yes | Generation number.
`service-props` | Props | Yes | Service properties.
`ttl` | Int>=0 | Yes | Service time-to-live (in seconds).
`client-id` | Int>=0 | Yes | Service's current owner.
`orphan-since` | Number | No | Time when contact to the owning client was lost, expressed in seconds since the UNIX epoch, in UTC. If left out, the owning client is in contact with the server.

#### Services Complete

A `services` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Services Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`services` command `fail` type message MAY contain the following
field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

The `fail-reason` value SHOULD take any of the values among the
[common reason codes](#common-reason-codes).

### Publish Command

**Command:** `publish`  
**Transaction type:** Single response

The `publish` command is used by a client to publish a service.

Upon publication, the service record will become known to other
clients connected to the same server. See the
[subscribe](#subscribe-command) and [services](#services-command) for
details.

A client `publish` MUST be allowed to republish (i.e. overwrite an
already-existing service record) provided:
1. The new service and the existing service has identical service identifiers.
2. One of the two below statements holds true:
  - The generation number, properties and TTL of the new and existing
    service are identical.
  - The generation number is higher than that of the existing service.
3. The `publish` is either performed by the client owning the existing
   service, or a different client possessing appropriate access rights
   as per server discretion.

The server SHOULD reject a request to republish a service with the
same generation number, but different properties and/or TTL.

#### Publish Request

A `publish` `request` message MUST only have the [mandatory message
fields](#mandatory-fields).

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`service-id` | Int>=0 | Yes | Service identifier.
`generation` | Int>=0 | Yes | Generation number.
`service-props` | Props | Yes | The complete set of service properties.
`ttl` | Int>=0 | Yes | Service time-to-live (in seconds).

#### Publish Complete

A `publish` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Publish Fail

In addition to the [mandatory message fields](#mandatory-fields), a
publish command `fail` type message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

##### Publish Fail Reason Codes

In addition to the [common reason codes](#common-reason-codes), the
value of the `fail-reason` field MAY be any one of the following:

Reason Code | Description
------------|------------
`old-generation` | Service record already exists, with newer generation.
`same-generation-but-different` | Service record exists with the same generation number as the new service, but with different properties and/or TTL.

### Unpublish Command

**Command:** `unpublish`  
**Transaction type:** Single response

The `unpublish` command unpublishes a service.

The server MUST allow the owning client and MAY allow a non-owner to
unpublish a service.

#### Unpublish Request

In addition to the [mandatory message fields](#mandatory-fields), a
subscribe command request message MUST contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`service-id` | Int>=0 | Yes | Service identifier of the target service.

#### Unpublish Complete

A `unpublish` `complete` message MUST only have the [mandatory
message fields](#mandatory-fields).

#### Unpublish Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`unpublish` command `fail` type message MAY contain the following
field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason | String` | No | Failure reason code.

##### Unpublish Fail Reason Codes

In addition to the [common reason codes](#common-reason-codes), the
value of the `fail-reason` field MAY be any one of the following:

Reason Code | Description
------------|------------
non-existent-service-id | Unknown service identifier.

### Subscriptions Command

**Command:** `subscriptions`  
**Transaction type:** Multiple response

With the `subscriptions` command, a client asks for information
concerning all currently-known subscriptions on the server, for all
connected clients.

In response to a `subscriptions` command, the server SHOULD only
provide a snapshot of the current state of the server, after which it
terminates the transaction with a `complete` type message.

#### Subscriptions Request

A `subscriptions` `request` message MUST only have the [mandatory
message fields](#mandatory-fields).

#### Subscriptions Accept

A `subscriptions` `accept` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscriptions Notification

In addition to the [mandatory message fields](#mandatory-fields), a
subscriptions command request message MUST/MAY contain the following
fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`subscription-id` | Int>=0 | Yes | Subscription identifier.
`client-id` | Int>=0 | Yes | Client identifier of the subscription owner.
`filter` | String | No | Subscription filter.

#### Subscriptions Complete

A `subscriptions` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Subscriptions Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`subscriptions` command `fail` type message MAY contain the following
field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

The `fail-reason` value SHOULD take any of the values among the
[common reason codes](#common-reason-codes).

### Clients Command

**Command:** `clients`  
**Transaction type:** Multiple response

With the `clients` command, a client asks for information concerning
all currently-connected clients.

In response to a `clients` command, the server SHOULD only provide a
snapshot of the current state of the server, after which it terminates
the transaction with a `complete` type message.

The list provided MUST include the client issuing the command.

#### Clients Request

A `clients` `request` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Clients Accept

A `clients` `accept` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Clients Notification

In addition to the [mandatory message fields](#mandatory-fields), a
`clients` command `notification` message MUST contain the following
fields:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`client-id` | Int>=0 | Yes | Client identifier.
`client-addr` | String | Yes | Source address of the client's transport connection.
`time` | Int>=0 | Yes | Client transport connection establishment time, expressed in seconds since the UNIX epoch, in UTC.

#### Clients Complete

A `clients` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Clients Fail

In addition to the [mandatory message fields](#mandatory-fields), a
`clients` command `fail` type message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

The `fail-reason` value SHOULD take any of the values among the
[common reason codes](#common-reason-codes).

### Ping Command

**Command:** `ping`  
**Transaction type:** Single response

The `ping` command provides a mean for a client to verify a server's
operational status, including measuring the combined network and
server processing latency.

#### Ping Request

A `ping` `request` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Ping Complete

A `ping` `complete` message MUST only have the [mandatory message
fields](#mandatory-fields).

#### Ping Fail

In addition to the [mandatory message fields](#mandatory-fields), a
ping command `fail` type message MAY contain the following field:

Field Name | Value Type | Mandatory | Description
-----------|------------|------------|------------
`fail-reason` | String | No | Failure reason code.

The `fail-reason` value SHOULD take any of the values among the
[common reason codes](#common-reason-codes).

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119.html (RFC 2119)
[RFC4515]: https://www.rfc-editor.org/rfc/rfc4515.html (RFC 4515)
[RFC5234]: https://www.rfc-editor.org/rfc/rfc5234.html (RFC 5234)
[RFC7159]: https://www.rfc-editor.org/rfc/rfc7159.html (RFC 7159)
