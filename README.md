# Pathfinder

Pathfinder (or paf, for short) is a minimal name service discovery
system, for embedded or cloud use.

## Installation

The Pathfinder server and related tools are implemented in Python, and
currently supports both Python 2 and 3. Beyond the standard modules
such as 'json' and 'logging', it also requires the 'enum' module,
which is not standard for Python 2. In addition, the libxcm.so shared
library is required.

The unit and component-level test suites depends on the py.test
framework.

Pathfinder build system uses autotools.

To install, run:
autoreconf -i && ./configure && make install

## Server

One Pathfinder server (daemon) process is run for each service
discovery domain. This domain is defined by the XCM address the daemon
has bound to.

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
to be the primariy API - this will be a C-based client library 'libpaf',
available in a separate repository.

Compared to 'libpaf', the Python API is more low-level and maps
closely to the Pathfinder wire protocol.

## Test Suite

To run the unit and component-level test suites, run:
```
make check
```

## More Information

You can find more information about Pathfinder on Ericsson Play.

Introduction and Overview:
https://play.ericsson.net/media/t/0_bewz17us
Command-line Demo:
https://play.ericsson.net/media/t/0_z8c77wsc
Tracing and Debugging:
https://play.ericsson.net/media/t/0_y1h8rkgi
