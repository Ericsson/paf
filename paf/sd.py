# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB


import contextlib
import enum
import random
import time


class Error(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class PermissionError(Error):
    def __init__(self, message):
        Error.__init__(self, message)


class GenerationError(Error):
    def __init__(self, message):
        Error.__init__(self, message)


class SameGenerationButDifferentError(Error):
    def __init__(self, message):
        Error.__init__(self, message)


class NotFoundError(Error):
    def __init__(self,  obj_type, obj_id):
        Error.__init__(self, "%s id %d not found" % (obj_type, obj_id))


class AlreadyExistsError(Error):
    def __init__(self,  obj_type, obj_id=None):
        if obj_id is not None:
            msg = "%s id %d already exists" % (obj_type, obj_id)
        else:
            msg = "%s already exists" % obj_type
        Error.__init__(self, msg)


class UserIdChanged(PermissionError):
    def __init__(self, client_id, new_user_id, old_user_id):
        PermissionError.__init__(self, "attempt to change client id %d "
                                 "user id from \"%s\" to \"%s\"" %
                                 (client_id, old_user_id, new_user_id))


class ResourceError(Error):
    def __init__(self, message):
        Error.__init__(self, message)


class ChangeType(enum.Enum):
    ADDED = enum.auto()
    MODIFIED = enum.auto()
    REMOVED = enum.auto()


class MatchType(enum.Enum):
    APPEARED = enum.auto()
    MODIFIED = enum.auto()
    DISAPPEARED = enum.auto()


class ResourceType(enum.Enum):
    CLIENT = enum.auto()
    SUBSCRIPTION = enum.auto()
    SERVICE = enum.auto()


DEFAULT_USER_ID = "default"


class Consumer:
    def __init__(self, user_id, max_resources):
        self.user_id = user_id
        self.max_resources = max_resources
        self.used_resources = resources(0, 0, 0)

    def allocate(self, resource_type):
        used = self.used_resources[resource_type]
        max = self.max_resources[resource_type]
        if max is not None and used == max:
            raise ResourceError("user id \"%s\" already allocated max (%d) "
                                "%s resources" % (self.user_id, used,
                                                  resource_type.name.lower()))
        self.used_resources[resource_type] += 1

    def deallocate(self, resource_type):
        self.used_resources[resource_type] -= 1
        assert self.used_resources[resource_type] >= 0

    def has_allocations(self):
        for t in ResourceType:
            if self.used_resources[t] > 0:
                return True
        return False


def resources(clients=None, subscriptions=None, services=None):
    return {
        ResourceType.CLIENT: clients,
        ResourceType.SUBSCRIPTION: subscriptions,
        ResourceType.SERVICE: services
    }


class ResourceManager:
    def __init__(self, max_user_resources, max_total_resources):
        self.max_user_resources = max_user_resources
        self.max_total_resources = max_total_resources
        self.consumers = {}

    def allocate(self, user_id, resource_type):
        self.check_total(resource_type)
        if user_id not in self.consumers:
            self.consumers[user_id] = \
                Consumer(user_id, self.max_user_resources)
        self.consumers[user_id].allocate(resource_type)

    def deallocate(self, user_id, resource_type):
        consumer = self.consumers[user_id]
        consumer.deallocate(resource_type)
        if not consumer.has_allocations():
            del self.consumers[user_id]

    def check_total(self, resource_type):
        limit = self.max_total_resources[resource_type]
        if limit is not None and limit == self.total(resource_type):
            raise ResourceError("total max (%d) of resource type %s already "
                                "reached" % (limit,
                                             resource_type.name.lower()))

    def total(self, resource_type):
        total = 0
        for consumer in self.consumers.values():
            total += consumer.used_resources[resource_type]
        return total

    def transfer(self, from_user_id, to_user_id, resource_type):
        # Deallocation before allocation, to avoid hitting the global
        # limit.
        self.deallocate(from_user_id, resource_type)
        try:
            self.allocate(to_user_id, resource_type)
        except ResourceError as e:
            self.allocate(from_user_id, resource_type)
            raise e


MIN_IDLE_MIN = 1


class IdleLimit:
    def __init__(self, idle_min, idle_max):
        if idle_min < MIN_IDLE_MIN:
            raise ValueError("Lower bound for max idle time must be set "
                             ">= %d" % MIN_IDLE_MIN)
        if idle_min > idle_max:
            raise ValueError("Max idle time lower bound must be equal to "
                             "or lower than the upper bound")
        self.idle_min = idle_min
        self.idle_max = idle_max

    def limit(self, value):
        return min(self.idle_max, max(self.idle_min, value))

    def idle_default(self):
        return self.idle_max


class Subscription:
    def __init__(self, sub_id, filter, client_id, user_id, match_cb):
        self.sub_id = sub_id
        self.filter = filter
        self.client_id = client_id
        self.user_id = user_id
        self.match_cb = match_cb

    def notify(self, change_type, service):
        if change_type == ChangeType.ADDED:
            if self.matches(service.props()):
                self.match_cb(self.sub_id, MatchType.APPEARED, service)
        elif change_type == ChangeType.MODIFIED:
            before = service.prev_props()
            after = service.props()
            if self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.MODIFIED, service)
            elif not self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.APPEARED, service)
            elif self.matches(before) and not self.matches(after):
                self.match_cb(self.sub_id, MatchType.DISAPPEARED, service)
        elif change_type == ChangeType.REMOVED:
            if self.matches(service.prev_props()):
                self.match_cb(self.sub_id, MatchType.DISAPPEARED, service)

    def matches(self, props):
        if self.filter is None:
            return True
        return self.filter.match(props)

    def check_access(self, client_id):
        if client_id != self.client_id:
            raise PermissionError("client id %s may not change subscription "
                                  "owned by client id %s" %
                                  (client_id, self.client_id))


GENERATION_FIELD_NAMES = \
    ('generation', 'props', 'ttl', 'orphan_since', 'client_id', 'user_id')


class Generation:
    def __init__(self):
        pass

    def is_consistent(self):
        for name in GENERATION_FIELD_NAMES:
            if not hasattr(self, name):
                return False
        return True

    def copy(self):
        copy = Generation()
        for name in GENERATION_FIELD_NAMES:
            value = getattr(self, name)
            setattr(copy, name, value)
        return copy


class Service:
    def __init__(self, service_id, change_cb):
        self.service_id = service_id
        self.prev = None
        self.cur = None
        self.change_cb = change_cb

    def has_prev_generation(self):
        return self.prev is not None

    def is_orphan(self):
        return self.orphan_since() is not None

    def orphan_timeout(self):
        return self.orphan_since() + self.ttl()

    def was_orphan(self):
        return self.prev_orphan_since() is not None

    def prev_orphan_timeout(self):
        return self.prev_orphan_since() + self.prev_ttl()

    @contextlib.contextmanager
    def add(self):
        ng = Generation()
        yield ng
        self.commit(ChangeType.ADDED, ng)

    @contextlib.contextmanager
    def modify(self):
        ng = self.cur.copy()
        yield ng
        self.commit(ChangeType.MODIFIED, ng)

    def remove(self):
        self.commit(ChangeType.REMOVED)

    def commit(self, change, ng=None):
        if change == ChangeType.ADDED or change == ChangeType.MODIFIED:
            assert ng.is_consistent()
        else:
            assert change == ChangeType.REMOVED
            assert ng is None

        self.prev = self.cur
        self.cur = ng

        self.change_cb(change, self)

    def check_access(self, user_id):
        if user_id != self.user_id():
            raise PermissionError("user id %s may not change service owned "
                                  "by user id %s" % (user_id, self.user_id()))


for name in GENERATION_FIELD_NAMES:
    setattr(Service, "%s" % name,
            lambda self, name=name: getattr(self.cur, name))

    setattr(Service, "prev_%s" % name,
            lambda self, name=name: getattr(self.prev, name))


def assure_state(fun, is_connected):
    def assure_wrap(self, *args, **kwargs):
        assert self.is_connected() == is_connected
        return fun(self, *args, **kwargs)
    return assure_wrap


def assure_connected(fun):
    return assure_state(fun, True)


def assure_not_connected(fun):
    return assure_state(fun, False)


WARNING_THRESHOLD = 0.5
WARNING_JITTER = 0.1


class IdleState(enum.Enum):
    ACTIVE = enum.auto()
    TENTATIVE = enum.auto()
    TIMED_OUT = enum.auto()


def jitter(base, max_jitter):
    k = 1.0 + (random.random() - 0.5) * 2 * max_jitter
    return k * base


class Connection:
    def __init__(self, client, timer_manager, idle_limit, idle_cb):
        self.client = client
        self.timer_manager = timer_manager
        self.idle_limit = idle_limit
        self.idle_cb = idle_cb
        self.subscriptions = {}
        self.services = {}
        self.connected_at = time.time()
        self.disconnected_at = None
        self.idle_state = IdleState.ACTIVE
        self.idle_timer = None
        self.last_seen = time.time()
        if idle_limit is not None:
            self.install_idle_warning_timer()

    def client_id(self):
        return self.client.client_id

    def user_id(self):
        return self.client.user_id

    @assure_connected
    def add_subscription(self, subscription):
        self.subscriptions[subscription.sub_id] = subscription

    def remove_subscription(self, subscription):
        del self.subscriptions[subscription.sub_id]

    def has_subscription(self, sub_id):
        return sub_id in self.subscriptions

    def get_subscription(self, sub_id):
        return self.subscriptions.get(sub_id)

    def get_subscriptions(self):
        return self.subscriptions.values()

    @assure_connected
    def add_service(self, service):
        self.services[service.service_id] = service

    def remove_service(self, service):
        del self.services[service.service_id]

    def has_service(self, service_id):
        return service_id in self.services

    def get_services(self):
        return self.services.values()

    def get_lowest_ttl(self):
        return min([service.ttl() for service in self.services.values()])

    def is_connected(self):
        return self.disconnected_at is None

    @assure_connected
    def disconnected(self):
        self.disconnected_at = time.time()
        self.uninstall_idle_timer()

    def is_stale(self):
        return not self.is_connected() and len(self.services) == 0

    def active(self):
        if self.idle_state != IdleState.TIMED_OUT:
            self.idle_state = IdleState.ACTIVE

            if self.idle_limit is not None:
                self.install_idle_warning_timer()

        self.last_seen = time.time()

    def check_idle(self):
        assert self.idle_limit is not None

        if self.idle_state == IdleState.ACTIVE:
            self.issue_idle_warning()

    def install_idle_timer(self, t):
        assert self.idle_limit is not None
        self.uninstall_idle_timer()
        self.idle_timer = \
            self.timer_manager.add(self.idle_timer_fired, t, relative=True)

    def max_idle_time(self):
        if self.idle_limit is not None:
            if len(self.services) > 0:
                return self.idle_limit.limit(self.get_lowest_ttl())
            else:
                return self.idle_limit.idle_default()

    def install_idle_warning_timer(self):
        warning_time = jitter(WARNING_THRESHOLD * self.max_idle_time(),
                              WARNING_JITTER)
        self.install_idle_timer(warning_time)

    def install_idle_timeout_timer(self):
        self.install_idle_timer((1 - WARNING_THRESHOLD) * self.max_idle_time())

    def uninstall_idle_timer(self):
        if self.idle_timer is not None:
            self.timer_manager.remove(self.idle_timer)
            self.idle_timer = None

    def issue_idle_warning(self):
        self.idle_state = IdleState.TENTATIVE
        self.idle_cb(self.client, True)
        self.install_idle_timeout_timer()

    def issue_idle_timeout(self):
        self.idle_state = IdleState.TIMED_OUT
        self.idle_cb(self.client, False)

    def idle_timer_fired(self):
        self.idle_timer = None

        if self.idle_state == IdleState.ACTIVE:
            self.issue_idle_warning()
        else:
            assert self.idle_state == IdleState.TENTATIVE
            self.issue_idle_timeout()


class DB:
    def __init__(self):
        self.subscriptions = {}
        self.services = {}
        self.clients = {}

    def has_client(self, client_id):
        return client_id in self.clients

    def get_client(self, client_id):
        return self.clients.get(client_id)

    def get_clients(self):
        return self.clients.values()

    def add_client(self, client):
        self.clients[client.client_id] = client

    def remove_client(self, client):
        del self.clients[client.client_id]

    def has_service(self, service_id):
        return service_id in self.services

    def get_service(self, service_id):
        return self.services.get(service_id)

    def get_services(self):
        return self.services.values()

    def add_service(self, service):
        self.services[service.service_id] = service

    def remove_service(self, service):
        del self.services[service.service_id]

    def has_subscription(self, sub_id):
        return sub_id in self.subscriptions

    def get_subscription(self, sub_id):
        return self.subscriptions.get(sub_id)

    def get_subscriptions(self):
        return self.subscriptions.values()

    def add_subscription(self, subscription):
        self.subscriptions[subscription.sub_id] = subscription

    def remove_subscription(self, subscription):
        del self.subscriptions[subscription.sub_id]


class Client:
    def __init__(self, client_id, user_id, db, resource_manager,
                 timer_manager):
        self.client_id = client_id
        self.user_id = user_id
        self.db = db
        self.resource_manager = resource_manager
        self.timer_manager = timer_manager
        self.active_connection = None
        self.inactive_connections = []

    def is_connected(self):
        return self.active_connection is not None

    def last_seen(self):
        if self.is_connected():
            return self.active_connection.last_seen

    def is_stale(self):
        for connection in self.get_connections():
            if not connection.is_stale():
                return False
        return True

    def connect(self, user_id, idle_limit, conn_idle_cb):
        if self.is_connected():
            # The active connection may be down but this has not yet
            # noticed by the server.
            self.check_idle()

            raise AlreadyExistsError("client", self.client_id)
        elif self.user_id != user_id:
            raise UserIdChanged(self.client_id, user_id, self.user_id)

        self.resource_manager.allocate(self.user_id, ResourceType.CLIENT)

        self.db.add_client(self)

        self.active_connection = \
            Connection(self, self.timer_manager, idle_limit, conn_idle_cb)

    @assure_connected
    def disconnect(self):
        inactivated = self.active_connection
        self.active_connection = None
        self.inactive_connections.append(inactivated)

        inactivated.disconnected()

        for subscription in list(inactivated.get_subscriptions()):
            self.remove_subscription(subscription)

        if inactivated.is_stale():
            self.remove_connection(inactivated)

        for service in inactivated.get_services():
            with service.modify() as change:
                change.orphan_since = inactivated.disconnected_at

        self.resource_manager.deallocate(self.user_id, ResourceType.CLIENT)

        if self.is_stale():
            self.db.remove_client(self)

    @assure_connected
    def publish(self, service_id, generation, service_props, ttl,
                service_change_cb):
        service = self.db.get_service(service_id)

        if service is not None:
            service.check_access(self.user_id)

            if generation == service.generation():
                if service_props != service.props() or ttl != service.ttl():
                    raise SameGenerationButDifferentError(
                        "properties/TTL changed, but generation is left at "
                        "%d" % generation
                    )

                prev_client_id = service.client_id()

                if prev_client_id != self.client_id:
                    self.capture_service(service)

                    with service.modify() as change:
                        # The client id is allowed to change (though you might
                        # argue against this permissive behavior), however the
                        # assumption is that the user stays the same, so no
                        # need to transfer resource tokens.
                        change.orphan_since = None
                        change.client_id = self.client_id
                elif service.is_orphan():
                    # previous owner is back
                    with service.modify() as change:
                        change.orphan_since = None

            elif generation > service.generation():
                if self.client_id != service.client_id():
                    self.capture_service(service)

                with service.modify() as change:
                    change.generation = generation
                    change.props = service_props
                    change.ttl = ttl
                    change.orphan_since = None
                    change.client_id = self.client_id
                    change.user_id = self.user_id
            else:
                raise GenerationError("invalid generation %d: existing "
                                      "service already at generation %d" %
                                      (generation, service.generation()))
        else:
            self.resource_manager.allocate(self.user_id,
                                           ResourceType.SERVICE)
            service = Service(service_id, service_change_cb)

            with service.add() as change:
                change.generation = generation
                change.props = service_props
                change.ttl = ttl
                change.orphan_since = None
                change.client_id = self.client_id
                change.user_id = self.user_id

                self.active_connection.add_service(service)

                self.db.add_service(service)

        # give opportunity to recalcute idle timer based on changed TTL
        self.active()

        return service

    @assure_connected
    def unpublish(self, service_id):

        service = self.db.get_service(service_id)
        if service is None:
            raise NotFoundError("service", service_id)

        service.check_access(self.user_id)

        # A non-owning client may unpublish a service, in which case
        # it first becomes the owner.
        new_owner = service.client_id() != self.client_id
        if new_owner or service.is_orphan():
            if new_owner:
                self.capture_service(service)
            with service.modify() as change:
                change.orphan_since = None
                change.client_id = self.client_id

        self.remove_service(service)

        # give opportunity to recalcute idle timer based on changed TTL
        self.active()

    @assure_connected
    def active(self):
        self.active_connection.active()

    @assure_connected
    def check_idle(self):
        self.active_connection.check_idle()

    def get_connections(self):
        if self.active_connection is not None:
            yield self.active_connection
        for connection in self.inactive_connections:
            yield connection

    def get_service_connection(self, service):
        for connection in self.get_connections():
            if connection.has_service(service.service_id):
                return connection

    def purge_orphan(self, service):
        self.remove_service(service)

    def capture_service(self, service):
        victim_client = self.db.get_client(service.client_id())
        victim_connection = \
            victim_client.get_service_connection(service)
        victim_connection.remove_service(service)
        self.active_connection.add_service(service)

    def remove_service(self, service):
        connection = self.get_service_connection(service)
        connection.remove_service(service)

        self.resource_manager.deallocate(self.user_id, ResourceType.SERVICE)

        if connection.is_stale():
            self.remove_connection(connection)

        if self.is_stale():
            self.db.remove_client(self)

        self.db.remove_service(service)

        service.remove()

    def create_subscription(self, sub_id, filter, match_cb):
        if self.db.has_subscription(sub_id):
            raise AlreadyExistsError("subscription", sub_id)

        self.resource_manager.allocate(self.user_id,
                                       ResourceType.SUBSCRIPTION)

        subscription = \
            Subscription(sub_id, filter, self.client_id, self.user_id,
                         match_cb)

        self.active_connection.add_subscription(subscription)
        self.db.add_subscription(subscription)

    def activate_subscription(self, sub_id):
        subscription = self.active_connection.get_subscription(sub_id)

        for service in self.db.get_services():
            subscription.notify(ChangeType.ADDED, service)

    def unsubscribe(self, sub_id):
        subscription = self.db.get_subscription(sub_id)
        if subscription is None:
            raise NotFoundError("subscription", sub_id)

        subscription.check_access(self.client_id)

        self.remove_subscription(subscription)

    def get_subscription_connection(self, subscription):
        for connection in self.get_connections():
            if connection.has_subscription(subscription.sub_id):
                return connection

    def remove_subscription(self, subscription):
        connection = self.get_subscription_connection(subscription)

        connection.remove_subscription(subscription)

        self.db.remove_subscription(subscription)

        self.resource_manager.deallocate(subscription.user_id,
                                         ResourceType.SUBSCRIPTION)

    def remove_connection(self, connection):
        self.inactive_connections.remove(connection)


class ServiceDiscovery:
    def __init__(self, name, timer_manager, max_user_resources,
                 max_total_resources):
        self.name = name
        self.timer_manager = timer_manager
        self.resource_manager = \
            ResourceManager(max_user_resources, max_total_resources)
        self.db = DB()
        self.orphan_timers = {}

    def client_connect(self, client_id, user_id, idle_limit, conn_idle_cb):
        client = self.db.get_client(client_id)

        if client is None:
            client = Client(client_id, user_id, self.db, self.resource_manager,
                            self.timer_manager)

        client.connect(user_id, idle_limit, conn_idle_cb)

    def client_disconnect(self, client_id):
        client = self._get_connected_client(client_id)

        client.disconnect()

    def client_active(self, client_id):
        client = self.db.get_client(client_id)
        client.active()

    def has_client(self, client_id):
        return self.db.has_client(client_id)

    def client_last_seen(self, client_id):
        client = self.db.get_client(client_id)
        return client.last_seen()

    def max_total_clients(self):
        return self.resource_manager.max_total_resources[ResourceType.CLIENT]

    def _get_connected_client(self, client_id):
        client = self.db.get_client(client_id)

        if client is None:
            raise NotFoundError("client", client_id)

        return client

    def track(self, client_id, query_cb):
        client = self._get_connected_client(client_id)

        client.track(query_cb)

    def publish(self, client_id, service_id, generation, service_props, ttl):
        client = self._get_connected_client(client_id)

        service = client.publish(service_id, generation, service_props, ttl,
                                 self.service_changed)
        return service

    def purge_orphan(self, service_id):
        service = self.db.get_service(service_id)
        client = self.db.get_client(service.client_id())
        client.purge_orphan(service)

    def next_orphan_timeout(self):
        return self.orphans.next_timeout()

    def unpublish(self, client_id, service_id):
        client = self._get_connected_client(client_id)

        client.unpublish(service_id)

    def has_service(self, service_id):
        return self.db.has_service(service_id)

    def get_service(self, service_id):
        service = self.db.get_service(service_id)
        if service is None:
            raise NotFoundError("service", service_id)
        return service

    def get_services(self):
        return self.db.get_services()

    def create_subscription(self, client_id, sub_id, filter, match_cb):
        client = self._get_connected_client(client_id)

        return client.create_subscription(sub_id, filter, match_cb)

    def activate_subscription(self, client_id, sub_id):
        client = self.db.clients[client_id]

        client.activate_subscription(sub_id)

    def get_subscription(self, sub_id):
        return self.db.get_subscription(sub_id)

    def get_subscriptions(self):
        return self.db.get_subscriptions()

    def unsubscribe(self, client_id, sub_id):
        client = self._get_connected_client(client_id)

        client.unsubscribe(sub_id)

    def orphan_timeout(self, service_id):
        del self.orphan_timers[service_id]
        self.purge_orphan(service_id)

    def add_orphan_timer(self, service):
        service_id = service.service_id

        def handler():
            self.orphan_timeout(service_id)

        timer = self.timer_manager.add(handler, service.orphan_timeout())
        self.orphan_timers[service_id] = timer

    def remove_orphan_timer(self, service):
        timer = self.orphan_timers.get(service.service_id)
        # The orphan being removed may or may not be caused by the
        # orphan timer firing.
        if timer is not None:
            del self.orphan_timers[service.service_id]
            self.timer_manager.remove(timer)

    def update_orphan_timer(self, service):
        timeout = service.orphan_timeout()
        prev_timeout = service.prev_orphan_timeout()

        if timeout != prev_timeout:
            self.remove_orphan_timer(service)
            self.add_orphan_timer(service)

    def maintain_orphans(self, change, service):
        if change == ChangeType.ADDED and service.is_orphan():
            self.add_orphan_timer(service)
        elif change == ChangeType.MODIFIED:
            is_orphan = service.is_orphan()
            was_orphan = service.was_orphan()
            if was_orphan and not is_orphan:
                self.remove_orphan_timer(service)
            elif not was_orphan and is_orphan:
                self.add_orphan_timer(service)
            elif was_orphan and is_orphan:
                self.update_orphan_timer(service)
        elif change == ChangeType.REMOVED and service.was_orphan():
            self.remove_orphan_timer(service)

    def service_changed(self, change, service):
        for subscription in self.db.get_subscriptions():
            subscription.notify(change, service)

        self.maintain_orphans(change, service)
