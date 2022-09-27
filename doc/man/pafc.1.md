pafc(1) -- service discovery client
===================================

## SYNOPSIS

`pafc` [-i <client_id>] <addr><br>
`pafc` [-i <client_id>] [-n <server-index>>] <domain><br>
`pafc` [-a] <domain><br>
`pafc` -l<br>
`pafc` -h

## DESCRIPTION

pafc is an interactive Pathfinder service discovery client, primarily
intended for test and debugging purposes.

## ARGUMENTS

pafc accept a single argument, which is either a Pathfinder service
discovery domain name, or an address to a server. The argument will
first be treated as a domain name, and then, if no such service
discovery domain can be found, as a server address in XCM format.

pafc looks up domain files in the same manner as a libpaf-based client
would. See the libpaf documentation for details on the format and
location of such domain files.

If the service discovery domain name resolves to multiple addresses
(i.e., the file contains multiple server addresses), the first server
in the list will be used.

## OPTIONS

 * `-i <client_id>`
   Specify the client identifier to be used, in hexadecimal format. If
   not set, pafc uses a randomly generated id.
 * `-n <server-index>`
   Connect to server address at <server-index> in the list of servers
   for the specified domain. Default is 0.
 * `-a`
   List all server addresses of specified domain.
 * `-l`
   List all configured domains (i.e., files in the domains directory).
 * `-h`
   Display pafc usage information.

## INTERACTIVE COMMANDS

If a connection to a Pathfinder server was successfully established,
pafc will present the user with a prompt.

pafc supports the following interactive commands:

 * `hello`
   Send a Pathfinder protocol hello request, and output the result
   of the response (i.e., the result and the protocol version used).
   An interactive hello will always the second protocol-level hello
   command, since pafc automatically performs the initial handshake
   at transport-level connection establishment.
 * `id`
   Print the session's client id. This interactive command requires
   no protocol-level interaction with the server.
 * `ping`
   Send a Pathfinder protocol ping request, and output the result,
   including the latency.
 * `clients`
   List all clients connected to the server, including the pafc
   session itself.
 * `services`
   List all services currently published on the server, including
   orphan services (i.e., services for which there is currently
   no owner), by all clients.
 * `subscriptions`
   List all subscriptions issued to the server, by all clients.
 * `publish [<service-id>] <generation> <ttl> [<prop-name> <prop-value>] ...`
   Publish a new service, or republish an update service.

   The service identifier is optional. If left out, pafc will generate
   a random service id for the published service.

   In case <prop-value> is an integer in decimal format, it will
   be added as an integer. Otherwise, the string property value type
   will be used. To force the use of strings, use '|<integer>|'
   (e.g. |4711|).
 * `unpublish <service-id>`
   Unpublish a service. The server only allows services published by
   the same user as the pafc session belongs to be unpublished.
 * `subscribe [<filter-expression>]`
   Subscribe to changes in services, with an optional filter expression.

   The LDAP search filter-like filter language is described in the
   Pathfinder protocol specification.

   If the filter is left out, pafc will receive notifications for
   changes to any service.
 * `unsubscribe <subscription-id>`
   Unsubscribe a service. The server only allows the removal of
   subscriptions issued by this pafc session.
 * `help [<cmd>]`
   Display a list of command, or in case the optional argument is
   supplied, information on a specific command.
 * `quit`
   Terminate the Pathfinder protocol connection and quit pafc. Any
   services publish as a part of this pafc session will be left as
   orphans on the server.

Most of the pafc interactive commands maps closely to a Pathfinder
protocol-level command. For more information on command requests and
responses, please refer to the appropriate section of the Pathfinder
protocol specification.

## EXAMPLES

In the below session, a user connects to a server serving the service
discovery domain "oam" and checks the latency:

    $ pafc oam
    > ping
    0.4 ms
    OK.
    >

The below is pafc session where the user connects to a server on the
host 192.168.1.59, serving a service discovery on a TLS server socket
bound to port 4711. The user checks pafc's client id, list all
clients, issues a subscription, and list all subscriptions. Another
client adds a matching service, and then modifies the same service.
The user then goes on to list all services.

    $ pafc tls:192.168.1.42:4711
    > id
    Client Id: 0x13b908adda6688d6
    > clients
    Client Id          Remote Address    Session Uptime
    13b908adda6688d6   tls:192.168.1.59:58700 0:00:02
    4e9a2e1d7ab98fcc   tls:192.168.1.59:58692 0:00:04
    OK.
    > subscribe (name=service-a)
    Subscription Id 5f2928fd0233f5c1.
    OK.
    > subscriptions
    Subscription Id   Owner Id           Filter Expression
    1cda481d5018aaeb  4e9a2e1d7ab98fcc   -
    5f2928fd0233f5c1  13b908adda6688d6   (name=service-a)
    OK.
    > Subscription 5f2928fd0233f5c1: Match type: appeared; Service id: 616fcc6f1bf6aabe; Generation: 0; TTL: 60; Client Id: 0x4e9a2e1d7ab98fcc; Properties: {'name': 'service-a', 'address': 'https://1.2.3.4:5555'}
    Subscription 5f2928fd0233f5c1: Match type: modified; Service id: 616fcc6f1bf6aabe; Generation: 1; TTL: 60; Client Id: 0x4e9a2e1d7ab98fcc; Properties: {'name': 'service-a', 'address': 'https://1.2.3.4:5678'}
    >
    > services
	  Service Id  Gen  TTL  Orphan Tmo  Owner              Properties
    616fcc6f1bf6aabe    1   60           -  4e9a2e1d7ab98fcc   {'name': 'service-a', 'address': 'https://1.2.3.4:5678'}
    OK.
    >

In this session, a pafc user connects to a server by address, and
publishes a service, and then modifies that service:

    $ pafc tls:192.168.1.42:4711
    > publish 0 60 name service-a address https://1.2.3.4:5555
    Service Id 616fcc6f1bf6aabe.
    OK.
    > publish 616fcc6f1bf6aabe 1 60 name service-a address https://1.2.3.4:5678
    OK.
    >


## COPYRIGHT

**Pathfinder** is Copyright (c) 2020, Ericsson AB, and released under
the BSD 3-Clause Revised License.

## SEE ALSO

pafd(8).
