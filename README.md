# Pathfinder

## Introduction

Pathfinder (or paf, for short) is a light-weight service discovery
system for embedded or data center use.

## Technical Overview

In a distributed system, such as a Radio Access Network (RAN) or a
large micro service-based web application, a process in need of a
particular service (often known as a consumer) must somehow be wired
up to an server process able to service its requests (often known as a
producer).

This can be done in several ways, such as manual configuration,
orchestration or service discovery.

Pathfinder implements client-side service discovery. In this model,
the producer registers its services (usually in the form of a name and
a set of properties, including an address) in some sort of logical
directory. Consumers will query this service directory, discriminating
among the matches to find the most suitable producer to connect to.

Pathfinder is split into two parts. The `libpaf` client library is
used by the service consumer and producer processes. This client
library communicates with zero or more Pathfinder `pafd` servers.

A solution that might come quickly to mind is to store services in
database (either in a centralized or distributed form), providing
functionality not unlike a LDAP server or the Domain Name System
(DNS). Unlike such a solution, Pathfinder doesn’t keep the
authoritative state in a database, but rather it is kept distributed
among the shared library instances of the consumer and producer
processes. The reason why this makes sense is that when a producer
terminates, any service discovery state related to its services is no
longer of use to anyone. The same goes for consumers and
subscriptions.

A Pathfinder server is acting like domain-specific communication hub,
keeping a copy of all known services and subscriptions. A server may
not always have the most-recent copy of every service record, but will
eventually be consistent. A subscription is a query, where the
consumer expresses a wish to be notified of the appearance,
modification, or disappearance of services matching a certain search
criteria (filter).

In a Pathfinder server, there is no need to store the state in
non-volatile storage, since in the case of a server crash or a
restart, its state is reproduced as the various consumers and
producers reconnect to the new server instance.

Both the Pathfinder server and the client shared library are memory
and CPU resource efficient and designed specifically to allow embedded
use (as well as use in the Cloud). The C shared library is ~6 kLOC,
and the Python server ~2 kLOC.

Pathfinder has a single concern - service discovery - and no other
functionality. It does imply or impose any consumer-producer
communication method, but allows anything such as REST/HTTPS, nng,
gRPC, XCM, a message bus, carrier pigeons, or a combination thereof,
to be used between the producers and the consumers.

Pathfinder relies on TCP Keep-alive for liveness checking. In case the
producer process dies, the servers will notice and mark the service as
an orphan. Such tentatively unavailable services will be removed when
their time-to-live (TTL) has been reached, unless the client
re-connects, and re-claims the service.

Pathfinder supports tens of thousands of clients, services and
subscriptions. It has a push model of subscriptions and a server-side
implementation of the subscription matching, making away for any need
for polling. When service discovery is idle (i.e. no subscriptions or
services coming or going), no CPU resources are used, with the
exception of TCP Keepalive processing in the kernel.

Pathfinder supports high availability and uses an active-active model,
allowing service discovery to still function in the face of networking
outages, server hardware and certain software failures.

## Installation

The Pathfinder server and related tools are implemented in Python.
The code base currently supports both Python 2 and 3, but Python 2
support will be dropped. Beyond the standard modules such as 'json'
and 'logging', it also requires the 'enum' module, which is not
standard for Python 2.

In case a server configuration file is used, and also for running the
test cases, the Python 'yaml' module is needed.

In addition, the Pathfinder server depends on the Extensible
Connection-oriented Messaging (XCM) library, in the form of libxcm.so.

The unit and component-level test suites depends on the py.test
framework.

Pathfinder build system uses autotools.

To install, run:
autoreconf -i && ./configure && make install

Automake tends to install the 'paf' Python module in the
'site-packages', rather than the 'dist-packages' directory.
Debian-based Linux distributions do not include 'site-packages' in the
Python module search path (i.e. sys.path). In that case, or in case a
non-standard (i.e non /usr/local or /usr) prefix is used, the
PYTHONPATH environment variable needs to point to 'site-packages' in
the installation directory tree, in order for the Pathfinder
applications to find the 'paf' module.

## Server

One or more Pathfinder server (daemon) processes are run for each
service discovery domain. One server instance may serve one or more
domains, which translate to one or more server socket endpoints (per
server).

To start the server and bind it to a local UNIX domain socket addrees,
run:
```
./app/pafd ux:test
```

## Command-line Interface

Pathfinder has an interactive command-line interface for
debugging. The following command will instantiate a Pathfinder client
and connect it to a server.
```
./app/pafc ux:test
```

By using the CLI, the user may publish services, subscribe to
services, list connected clients, a domain's subscriptions and
services.

## Python Client API

Pathfinder includes an API for Python-based clients, which is used by
the server test suite and the command-line interface.

While this is a production-quality API implementation, it's not meant
to be the primariy API - this will be a C-based client library
'libpaf', available in a separate repository. It also has a Python
interface.

Compared to `libpaf` and `<paf.h>`, the Python API is more low-level
and maps closely to the Pathfinder wire protocol.

## Test Suite

To run the unit and component-level test suites, run:
```
make check
```
