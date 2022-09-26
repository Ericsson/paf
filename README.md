# Pathfinder

## Introduction

Pathfinder is a light-weight service discovery system for embedded or
cloud use.

This repository contains the server-side implementation of the
[Pathfinder
protocol](https://github.com/Ericsson/paf/blob/master/doc/PROTOCOL.md)
version 2.

## Overview

In a distributed system, such as a Radio Access Network (RAN) or a
large micro service-based web application, a process in need of a
particular service (often known as a consumer) must somehow be wired
up to a server process able to service its requests (often known as a
producer).

This can be done in several ways, such as manual configuration,
orchestration or service discovery.

Pathfinder implements client-side service discovery. In this model,
the producer registers its services (usually in the form of a name and
a set of properties, including an address) in some sort of
directory. Consumers will query this service directory, discriminating
among the matches to find the most suitable producer to connect to.

Pathfinder is split into two parts. The `libpaf` client library is
used by the service consumer and producer processes. This client
library communicates with zero or more Pathfinder `pafd` servers.

A service discovery implementation that might easily come to mind is
to store services in a centralized or distributed database. Unlike
such a design, Pathfinder doesn't keep the authoritative state in a
database in the traditional sense, but rather it's distributed among
the `libpaf` instances of the consumer and producer processes. One
reason to keep the authorative service state in or close to the
producer process, and the subscription state in or close to the
consumer process, is that in case that a client process terminates,
its service discovery-related state is no longer of any use.

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
use (as well as use in the cloud).

Pathfinder has a single concern - service discovery - and no other
functionality. It does imply or impose any consumer-producer
communication method, but allows anything such as REST/HTTPS, nng,
gRPC, XCM, a message bus, carrier pigeons, or a combination thereof,
to be used between the producers and the consumers.

Pathfinder relies on TCP keepalive to track liveness. In case the
producer process dies, the servers will notice and mark the service as
an orphan. Such tentatively unavailable services will be removed when
their time-to-live (TTL) expires, unless the client reconnects, and
reclaims the service.

Pathfinder supports tens of thousands of clients, services and
subscriptions. It has a push model of subscriptions and a server-side
implementation of the subscription matching (i.e. filter evaluation),
making away for any need for polling. When service discovery is idle
(i.e. no subscriptions or services coming or going), no CPU resources
are used, with the exception of TCP keepalive processing in the
kernel.

Pathfinder supports high availability and uses an active-active model,
allowing service discovery to still function in the face of networking
outages, server hardware and certain software failures.

## Installation

The Pathfinder server and related tools are implemented in Python.

Python version 3.5 or later is required. In case a server
configuration file is used, and also for running the test cases, the
`yaml` module is needed.

In addition, the Pathfinder server depends on Extensible
Connection-oriented Messaging (XCM), in the form of the `libxcm`
shared library. The minimum XCM API/ABI version is 0.15 (XCM
v1.1.0). In case 'pafc' or 'paf.client' are used in combination with
per-server TLS certificate file configuration, XCM API/ABI 0.16 (XCM
v1.2.0) or later must be available.

The Pathfinder server build uses Python setuptools.

To build, run:
```
make
```

To install, run:
```
make install PREFIX=<prefix>
```

Issuing the appropriate `setup.py` commands directly also works. The
Makefile is just a simple wrapper.

## Test Suite

The unit and component-level test suites depends on the py.test-3
framework.

`flake8` is used to verify coding style for all Python files, and
needs to be installed.

To run the test suite, issue:
```
make check
```

## Running the Server

One or more Pathfinder server (daemon) processes are run for each
service discovery domain. One server instance may serve one or more
domains, which translate to one or more server sockets (per server).

To start the server and bind it to a local UNIX domain socket address,
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

By using the CLI, the user may publish services, subscribe to services
and list connected clients, subscriptions and services.

## Python Client API

Pathfinder includes an API `paf.client` for Python-based clients, which
is used by the server test suite and the command-line interface.

While this is a production-quality client implementation, it's not
meant to be the primariy API for consumer and producers. Applications
would instead use the `libpaf` client library, available in a separate
repository. It also has a Python interface.

Compared to `libpaf` and `<paf.h>`, the `paf.client` Python API is
more low-level and maps closely to the Pathfinder wire protocol.

## Documentation

The [Pathfinder application protocol
specification](https://github.com/Ericsson/paf/blob/master/doc/PROTOCOL.md),
in particular the data model section, includes a lot of useful
information on how a Pathfinder service discovery system works.

### Manual pages

* [pafd](https://ericsson.github.io/paf/man/pafd.8.html)
* [pafc](https://ericsson.github.io/paf/man/pafc.1.html)
