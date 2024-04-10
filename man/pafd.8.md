pafd(8) -- service discovery server
===================================

## SYNOPSIS

`pafd` [<options>...] [<domain-addr>...]

## DESCRIPTION

pafd is a server for use in a Pathfinder light-weight service
discovery system.

pafd is designed to be used as a daemon (i.e., a UNIX server process),
but does not daemonize itself (e.g., it does not call fork() and runs
in the background).

## ARGUMENTS

pafd may be supplied zero or more arguments. Each such argument must a
server socket address, in XCM format.

For each address, the server will instantiate a service discovery
domain, and bind to the socket. Thus, a server process may be used to
serve more than one service discovery domain.

pafd may be configured to serve same service discovery domain on
different server sockets simultaneously, using the `-m` option.

In case domains are specified both in the configuration file, and as
arguments or options, all domains in the configuration file will be
ignored, and only the domains specified on the command-line will be
used.

## OPTIONS

 * `-m`
   Instantiate a multi-socket service discovery domain. Each server
   socket address is separated by '+' (which may not be used in the
   address).

 * `-s`
   Enable logging to console (standard error). Console logging is
   disabled by default.

 * `-o <file>`
   Enable logging directly to file.

 * `-b <backup-count>`
   Enable log rotation for direct file logging, and keep <backup-count>
   number of copies.

 * `-x <log-file-max-size>`
   Set the maximum log file size (in bytes) before a file is rolled
   over. Only has effect in case direct file logging with log rotation
   is enabled. Default is 1000000.

 * `-n`
   Disable logging to syslog. Syslog logging is enabled by default.

 * `-y <facility>`
   Set syslog facility to use.

 * `-l <level>`
   Discard log messages with a severity level below <level>.

 * `-c <max-clients>`
   Set the maximum number of allowed connected clients to
   <max-clients>. The default is no limit.

 * `-f <conf-file>`
   Read configuration from <conf-file>.

 * `-v`
   Display pafd version information.

 * `-h`
   Display pafd usage information.

Options override any configuration set by a configuration file.

## EXAMPLES

The below example spawns one server process with two service discovery
domains; one on a UNIX domain socket "foo", and one answering on TCP
port 4711:

    $ pafd ux:foo tcp:*:4711

This example spawns a server process, instantiates one domain, and
enables console logging and disables syslog and log filtering:

    $ pafd -n -s -l debug ux:foo
    INFO:root:Server version 1.0.3 started with configuration: domains: [ ['ux:foo'] ], log: { console: true, syslog: false, filter: debug, facility: daemon }

## RESOURCE LIMITS

pafd may be configured to impose limits on various types of
server-side resources. Such limits may be put in place to protect the
server from CPU overload and memory or other resource exhaustion, and
to mitigate damage caused by malicious or otherwise misbehaving
clients.

There are three types of resources; clients, services and
subscriptions.

The total resource limits specifies the maximum number of objects that
may be instantiated, of a particular resource type, on the level of a
service discovery domain.

The user resource limits specifies per-user limits, within a
particular service discovery domain. All users have the same limits
with same upper bound values. The user is accounted against the number
of objects used *in that domain*, for all clients associated with that
user.

pafd relies on transport protocol level user authentication. For TLS,
the X.509 subject key id (SKI) is used to identify a user. For TCP,
the client's source IP address serves the same role. Clients
connecting using other transport protocols or IPC mechanisms (i.e.,
UNIX domain sockets) all qualifies as the same user.

Each client uses a number of service-side file descriptors (usually
one or two), depending on XCM transport used. For large servers, the
per-process file descriptor limit (see RLIMIT_NOFILE in setrlimit(2))
or the global limit (fs.file-max) may need to be increased.

## CONFIGURATION FILE FORMAT

The configuration file uses YAML as its base format. The root is a
YAML dictionary (also known as a YAML mapping, or hash), with three
keys, all optional; *domains*, *resources*, and *log*.

*domains*, if present, must be a list of service discovery domain
dictionaries. A domain dictionary must either have a key *sockets*,
which value is a list of all sockets (or endpoints) that should be
bound to that domain. *addrs* may be used as an alternative name.

A domain may also optionally be given a name, stored as a string under
the *name* key. This domain name is only used for logging and
documentation and is not seen in the by clients to the Pathfinder
server.

The range of allowed Pathfinder protocol versions for clients
connecting to a particular domain may be administratively limited
(beyond what the server supports) by providing a per-domain
*protocol_version* dictionary, with either/both of the *max* and *min*
keys.

*max* represents an upper bound for the Pathfinder protocol version
used. Must be an integer <= 3.

*min* represents a lower bound for the Pathfinder protocol version
used. Must be an integer >= 2.

The max idle time for Pathfinder Protocol v3 clients may be controlled
on a per-domain basis by configuring a dictionary *idle* with
either/both of the *max* and *min* keys.

*max* represents an upper bound for the maximum idle time (in seconds)
that will ever be employed for any client. This maximum value is also
used as the initial value for clients connecting to that domain. The
actual maximum idle time may be lower (e.g., if a client has published
services with a low time-to-live [TTL]). *max* default is 30 seconds.

*min* represents a lower bound for the maximum idle time.  The default
value is 4 seconds. *min* may not be set lower than 1 second.

Each element of the *sockets* list is a socket. A socket is either the
address in the form of a string (in XCM format), or a dictionary,
where the address must be the value of the key *addr*.

For TLS addresses, an addition key *tls* may be present in the socket
dictionary. *tls*, if present, must be a dictionary with zero or more
of four possible entries. *cert*, *key*, *tc* may be used to override
the default XCM certificate, private key, or trusted CA bundle file
locations. The key *crl* may be included in case certificate
revocation list checking is to be employed. The value of each such key
must be a filename in string format.

*resources*, if present, must be a dictionary, containing either or
both of two keys *total* and *user*.

The value of *total* must be a dictionary with any/all of the
following keys: *clients*, *services*, and/or *subscriptions*. The
values must all be integers, and represent the upper bound for the
number of instances of a particular resource type, for a service
discovery domain.

The value of *user* is a dictionary in the same format as *total*'s.

*log*, if present, must be a dictionary, and may contain any of the
following keys: *console* (with a boolean value), *syslog* (boolean),
*facility* (string), and *filter* (string). See the corresponding
command-line options for details.

Configuration file example:

    domains:
    - name: domain0
      addrs: # Domain which may be access via two server sockets
      - tls:*:4711
      - ux:local
    - name: domain1
      idle:
	max: 15
      addrs: # Second domain, only available at one socket
      - tls:192.168.1.59:5711

    resources:
      total: # Per-domain limits
        clients: 10000
        services: 10000
        subscriptions: 10000
      user: # Limits per "user" (e.g., TLS certificate)
        clients: 1000
        services: 1000
        subscriptions: 1000

    log:
      console: true
      syslog: true # Usually disabled in containized environments
      facility: daemon
      filter: info

## COPYRIGHT

**Pathfinder** is Copyright (c) 2020, Ericsson AB, and released under
the BSD 3-Clause Revised License.

## SEE ALSO

pafc(1).
