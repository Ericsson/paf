# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import sys
import getopt
import os

import paf.sd
import paf.server
import paf.eventloop
import paf.conf
from paf.logging import LogCategory, info, exception


def usage(name):
    print("Usage:")
    print("%s [-s] [-l <level>] [-y <facility>] [-c <max-clients>] "
          "[-f <conf-file>]" % name)
    print("%s [-m <addr0>+...+<addrN>] [<domain-addr> ...]" %
          (len(name) * " "))
    print("%s -h" % name)
    print("Arguments:")
    print("  <domain-addr>  The XCM server address of a domain to be "
          "instantiated by the")
    print("                 server.")
    print("Options:")
    print("  -m <addr0>+...+<addrN>  Instantiate a multi-socket domain. The "
          "'+' separator")
    print("                          may not be used in the addresses.")
    print("  -s                      Enable logging to console.")
    print("  -n                      Disable logging to syslog.")
    print("  -y <facility>           Set syslog facility to use.")
    print("  -l <level>              Filter levels below <level>.")
    print("  -c <max-clients>        Set the maximum number of allowed "
          "connected clients")
    print("                          to <max-clients>. The default "
          "is no limit.")
    print("  -f <conf-file>          Read configuration from <conf-file>.")
    print("  -h                      Print this text.")


def early_error(message):
    print(message)
    sys.exit(1)


def run_hook(hook, servers):
    try:
        import importlib
        module_name = '.'.join(hook.split('.')[:-1])
        fun_name = hook.split('.')[-1]
        module = importlib.import_module(module_name)
        fun = getattr(module, fun_name)

        info("Running user hook \"%s\"." % hook, LogCategory.CORE)

        fun(servers)
    except Exception:
        exception("Error while calling the user-supplied hook \"%s\"." %
                  hook)
        sys.exit(1)


def run(conf, hook=None):
    syslog_ident = 'pafd[%d]: ' % os.getpid()
    paf.logging.configure(conf.log.console, conf.log.syslog, syslog_ident,
                          conf.log.facility, conf.log.filter)

    event_loop = paf.eventloop.EventLoop()

    try:
        servers = []
        for domain in conf.domains:
            user = conf.resources.user.resources
            total = conf.resources.total.resources
            server = paf.server.create(domain.addrs, user, total,
                                       event_loop)
            servers.append(server)

        if hook is not None:
            run_hook(hook, servers)

        info("Server version %s started with configuration: %s" %
             (paf.server.VERSION, conf), LogCategory.CORE)

        event_loop.run()

        info("Exiting.", LogCategory.CORE)

        for server in servers:
            server.terminate()

        sys.exit(0)
    except Exception:
        exception("Terminating due to exception.")
        for server in servers:
            server.close_server_socks()
        sys.exit(1)


def main(argv):
    try:
        hook = None

        optlist, args = getopt.getopt(argv[1:], 'f:m:snl:y:c:r:h')

        conf_filename = None
        for opt, optval in optlist:
            if opt == '-f':
                conf_filename = optval
        if conf_filename is None:
            conf = paf.conf.default()
        else:
            conf = paf.conf.load(conf_filename)

        domains = [[domain_addr] for domain_addr in args]

        if len(domains) > 0:
            conf.set_domains(domains)

        for opt, optval in optlist:
            if opt == '-m':
                domain = []
                for addr in optval.split('+'):
                    domain.append(addr)
                domains.append(domain)
            elif opt == '-s':
                conf.log.set_console(True)
            elif opt == '-n':
                conf.log.set_syslog(False)
            elif opt == '-l':
                conf.log.set_filter(optval)
            elif opt == '-y':
                conf.log.set_facility(optval)
            elif opt == '-c':
                try:
                    clients = int(optval)
                    if clients == 0:
                        conf.resources.total.clear_limit("clients")
                    else:
                        conf.resources.total.set_limit("clients", clients)
                except ValueError:
                    early_error("Client limit must be an integer.")
            elif opt == '-r':
                hook = optval
            elif opt == '-h':
                usage(argv[0])
                sys.exit(0)

        if len(domains) > 0:
            conf.set_domains(domains)

        if len(conf.domains) == 0:
            early_error("No domains configured.")

        run(conf, hook)

    except getopt.GetoptError as e:
        early_error("Error parsning command line: %s." % e)
    except (paf.conf.Error, FileNotFoundError) as e:
        early_error("Error reading configuration: %s" % e)


if __name__ == "__main__":
    main(sys.argv)
