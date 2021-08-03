# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB


import collections
import copy
import enum
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
    def __init__(self,  obj_type, obj_id):
        Error.__init__(self, "%s id %d already exists" % (obj_type, obj_id))


class ResourceError(Error):
    def __init__(self, message):
        Error.__init__(self, message)


class ChangeType(enum.Enum):
    ADDED = 1
    MODIFIED = 2
    REMOVED = 3


class MatchType(enum.Enum):
    APPEARED = 1
    MODIFIED = 2
    DISAPPEARED = 3


class ResourceType(enum.Enum):
    CLIENT = 0
    SUBSCRIPTION = 1
    SERVICE = 2


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
            before = service.had_props()
            after = service.props()
            if self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.MODIFIED, service)
            elif not self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.APPEARED, service)
            elif self.matches(before) and not self.matches(after):
                self.match_cb(self.sub_id, MatchType.DISAPPEARED, service)
        elif change_type == ChangeType.REMOVED:
            if self.matches(service.had_props()):
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
        return copy.copy(self)


def assure_allowed(fun, allowed_change_types):
    def assure_wrap(self, *args, **kwargs):
        assert self.change in allowed_change_types
        return fun(self, *args, **kwargs)
    return assure_wrap


def assure_changing(fun):
    return assure_allowed(fun, (ChangeType.ADDED, ChangeType.MODIFIED,
                                ChangeType.REMOVED))


def assure_writable(fun):
    return assure_allowed(fun, (ChangeType.ADDED, ChangeType.MODIFIED))


def assure_not_changing(fun):
    return assure_allowed(fun, (None,))


class Service:
    def __init__(self, service_id, change_cb):
        self.service_id = service_id
        self.prev = None
        self.cur = None
        self.next = None
        self.change = None
        self.change_cb = change_cb

    def has_prev_generation(self):
        return self.prev is not None

    def is_orphan(self):
        return self.orphan_since() is not None

    def make_orphan(self, now):
        self.set_orphan_since(now)

    def adopted(self):
        self.set_orphan_since(None)

    def orphan_timeout(self):
        return self.orphan_since() + self.ttl()

    def was_orphan(self):
        return self.had_orphan_since() is not None

    @assure_not_changing
    def add(self):
        self.change = ChangeType.ADDED
        self.next = Generation()
        return self

    @assure_not_changing
    def modify(self):
        self.change = ChangeType.MODIFIED
        self.next = self.cur.copy()
        return self

    @assure_not_changing
    def remove(self):
        self.change = ChangeType.REMOVED
        return self

    @assure_changing
    def commit(self):
        if self.change == ChangeType.ADDED or \
           self.change == ChangeType.MODIFIED:
            assert self.next.is_consistent()
        self.prev = self.cur
        self.cur = self.next
        self.next = None
        change = self.change
        self.change = None

        self.change_cb(change, self)

    @assure_changing
    def rollback(self):
        self.change = None
        self.next = None

    def __enter__(self):
        assert self.change is not None
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_value is None:
            self.commit()
        else:
            self.rollback()

    def check_access(self, user_id):
        if user_id != self.user_id():
            raise PermissionError("user id %s may not changed service owned "
                                  "by user id %s" % (user_id, self.user_id))


for name in GENERATION_FIELD_NAMES:
    setattr(Service, "%s" % name,
            lambda self, name=name: getattr(self.cur, name))

    setattr(Service, "had_%s" % name,
            lambda self, name=name: getattr(self.prev, name))

    setattr(Service, "set_%s" % name,
            assure_writable(lambda self, value, name=name:
                            setattr(self.next, name, value)))


class TimeoutQueue:
    def __init__(self):
        self.queue = collections.deque()

    def empty(self):
        return len(self.queue) == 0

    def add(self, new_timeout_id, new_timeout_value):
        for i in range(len(self.queue) - 1, -1, -1):
            timeout_id, timeout_value = self.queue[i]
            if new_timeout_value >= timeout_value:
                self.queue.insert(i + 1, (new_timeout_id, new_timeout_value))
                return
        self.queue.appendleft((new_timeout_id, new_timeout_value))

    def update(self, timeout_id, new_timeout_value):
        self.remove(timeout_id)
        self.add(timeout_id, new_timeout_value)

    def remove(self, target_timeout_id):
        for i, (timeout_id, timeout_value) in enumerate(self.queue):
            if timeout_id == target_timeout_id:
                del self.queue[i]
                return

    def next_timeout(self):
        if self.empty():
            return None
        timeout_id, timeout_value = self.queue[0]
        return timeout_value

    def __iter__(self):
        return iter(self.queue)


class ServiceDiscovery:
    def __init__(self, max_user_resources, max_total_resources,
                 service_change_cb=None):
        self.resource_manager = \
            ResourceManager(max_user_resources, max_total_resources)
        self.services = {}
        self.subscriptions = {}
        self.clients = {}
        self.service_change_cb = service_change_cb
        self.orphans = TimeoutQueue()

    def client_connect(self, client_id, user_id):
        if client_id in self.clients:
            raise AlreadyExistsError("client", client_id)
        self.resource_manager.allocate(user_id, ResourceType.CLIENT)
        self.clients[client_id] = user_id

    def has_client(self, client_id):
        return client_id in self.clients

    def client_disconnect(self, client_id):
        user_id = self._get_user_id(client_id)
        self.resource_manager.deallocate(user_id, ResourceType.CLIENT)
        now = time.time()
        for service in self.get_services_with_client_id(client_id):
            with service.modify():
                service.make_orphan(now)
        client_subscriptions = \
            list(self.get_subscriptions_with_client_id(client_id))
        for subscription in client_subscriptions:
            self._remove_subscription(subscription)
        del self.clients[client_id]

    def max_total_clients(self):
        return self.resource_manager.max_total_resources[ResourceType.CLIENT]

    def _get_user_id(self, client_id):
        try:
            return self.clients[client_id]
        except KeyError:
            raise NotFoundError("client", client_id)

    def publish(self, service_id, generation, service_props, ttl, client_id):
        user_id = self._get_user_id(client_id)
        if service_id in self.services:
            service = self.services[service_id]
            service.check_access(user_id)
            if generation == service.generation():
                if service_props != service.props() or ttl != service.ttl():
                    raise SameGenerationButDifferentError(
                        "properties/TTL changed, but generation is left at "
                        "%d" % generation
                    )
                if service.client_id() != client_id or service.is_orphan():
                    # owner comes back - with new or reused client id
                    with service.modify():
                        # The client id is allowed to change (though you might
                        # argue against this permissive behavior), however the
                        # assumption is that the user stays the same, so no
                        # need to transfer resource tokens.
                        service.set_client_id(client_id)
                        service.adopted()
                return service
            elif generation > service.generation():
                with service.modify():
                    service.set_generation(generation)
                    service.set_props(service_props)
                    service.set_ttl(ttl)
                    service.adopted()
                    service.set_client_id(client_id)
                return service
            else:
                raise GenerationError("invalid generation %d: existing "
                                      "service already at generation %d" %
                                      (generation, service.generation()))
        else:
            self.resource_manager.allocate(user_id, ResourceType.SERVICE)
            service = Service(service_id, self._service_commit)
            with service.add():
                service.set_generation(generation)
                service.set_props(service_props)
                service.set_ttl(ttl)
                service.set_client_id(client_id)
                service.adopted()
                service.set_user_id(user_id)
                self.services[service_id] = service
            return service

    def purge_orphans(self):
        now = time.time()
        timed_out = []
        for orphan_id, orphan_timeout in self.orphans:
            if orphan_timeout > now:
                break
            timed_out.append(orphan_id)
        for orphan_id in timed_out:
            self._unpublish(orphan_id)
        return timed_out

    def next_orphan_timeout(self):
        return self.orphans.next_timeout()

    def _unpublish(self, service_id):
        service = self.services.pop(service_id)
        with service.remove():
            self.resource_manager.deallocate(service.user_id(),
                                             ResourceType.SERVICE)

    def unpublish(self, service_id, client_id):
        user_id = self._get_user_id(client_id)
        service = self.services.get(service_id)
        if service is None:
            raise NotFoundError("service", service_id)
        service.check_access(user_id)
        self._unpublish(service_id)

    def has_service(self, service_id):
        return service_id in self.services

    def get_service(self, service_id):
        service = self.services.get(service_id)
        if service is None:
            raise NotFoundError("service", service_id)
        return service

    def get_services(self):
        return self.services.values()

    def get_services_with_client_id(self, client_id):
        for service in self.services.values():
            if service.client_id() == client_id:
                yield service

    def create_subscription(self, sub_id, filter, client_id, match_cb):
        user_id = self._get_user_id(client_id)
        if sub_id in self.subscriptions:
            raise AlreadyExistsError("subscription", sub_id)
        self.resource_manager.allocate(user_id, ResourceType.SUBSCRIPTION)
        subscription = \
            Subscription(sub_id, filter, client_id, user_id, match_cb)
        self.subscriptions[sub_id] = subscription

    def activate_subscription(self, sub_id):
        subscription = self.subscriptions[sub_id]
        for service in self.services.values():
            subscription.notify(ChangeType.ADDED, service)

    def get_subscription(self, sub_id):
        return self.subscriptions[sub_id]

    def get_subscriptions(self):
        return self.subscriptions.values()

    def get_subscriptions_with_client_id(self, client_id):
        for subscription in self.subscriptions.values():
            if subscription.client_id == client_id:
                yield subscription

    def _remove_subscription(self, subscription):
        del self.subscriptions[subscription.sub_id]
        self.resource_manager.deallocate(subscription.user_id,
                                         ResourceType.SUBSCRIPTION)

    def remove_subscription(self, sub_id, client_id):
        assert client_id in self.clients
        subscription = self.subscriptions.get(sub_id)
        if subscription is None:
            raise NotFoundError("subscription", sub_id)
        subscription.check_access(client_id)
        self._remove_subscription(subscription)

    def _maintain_orphans(self, change, service):
        if change == ChangeType.ADDED and service.is_orphan():
            self.orphans.add(service.service_id, service.orphan_timeout())
        elif change == ChangeType.MODIFIED:
            is_orphan = service.is_orphan()
            was_orphan = service.was_orphan()
            if was_orphan and not is_orphan:
                self.orphans.remove(service.service_id)
            elif not was_orphan and is_orphan:
                self.orphans.add(service.service_id,
                                 service.orphan_timeout())
            elif was_orphan and is_orphan:
                cur_timeout = service.orphan_timeout()
                prev_timeout = service.had_orphan_timeout()
                if cur_timeout != prev_timeout:
                    self.orphans.update(service.service_id, cur_timeout)
        elif change == ChangeType.REMOVED and service.was_orphan():
            self.orphans.remove(service.service_id)

    def _service_commit(self, change, service):
        for subscription in self.subscriptions.values():
            subscription.notify(change, service)

        self._maintain_orphans(change, service)

        if self.service_change_cb is not None:
            self.service_change_cb(change, service)
