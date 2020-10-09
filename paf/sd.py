# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import enum
import time
import copy

class Error(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class PermissionError(Error):
    def __init__(self, message):
        Error.__init__(self, message)

class GenerationError(Error):
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
    ADDED=1
    MODIFIED=2
    REMOVED=3

class MatchType(enum.Enum):
    APPEARED=1
    MODIFIED=2
    DISAPPEARED=3

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
        if max != None and used == max:
            raise ResourceError("user id \"%s\" already allocated max (%d) " \
                                "%s resources" % (self.user_id, used, \
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

def resources(clients = None, subscriptions = None, services = None):
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
        if not user_id in self.consumers:
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
        if limit != None and limit == self.total(resource_type):
            raise ResourceError("total max (%d) of resource type %s already "
                                "reached" % (limit, \
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
    def notify(self, change_type, before, after):
        if change_type == ChangeType.ADDED:
            if self.matches(after):
                self.match_cb(self.sub_id, MatchType.APPEARED, after)
        elif change_type == ChangeType.MODIFIED:
            if self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.MODIFIED, after)
            elif not self.matches(before) and self.matches(after):
                self.match_cb(self.sub_id, MatchType.APPEARED, after)
            elif self.matches(before) and not self.matches(after):
                self.match_cb(self.sub_id, MatchType.DISAPPEARED, before)
        elif change_type == ChangeType.REMOVED:
            if self.matches(before):
                self.match_cb(self.sub_id, MatchType.DISAPPEARED, before)
    def matches(self, service):
        if self.filter == None:
            return True
        return self.filter.match(service.props)
    def check_access(self, client_id):
        if client_id != self.client_id:
            raise PermissionError("client id %s may not changed subscription "
                                  "owned by client id %s" % \
                                  (client_id, self.client_id))

class Service:
    def __init__(self, service_id, generation, props, ttl, orphan_since,
                 client_id, user_id, change_cb):
        self.service_id = service_id
        self.generation = generation
        self.props = props
        self.ttl = ttl
        self.orphan_since = orphan_since
        self.client_id = client_id
        self.user_id = user_id
        self.before = None
        self.change_cb = change_cb
    def is_orphan(self):
        return self.orphan_since != None
    def make_orphan(self, now):
        self.orphan_since = now
    def adopted(self):
        self.orphan_since = None
    def orphan_timeout(self):
        return self.orphan_since + self.ttl
    def prepare(self):
        self.before = Service(self.service_id, self.generation,
                              copy.deepcopy(self.props), self.ttl,
                              self.orphan_since, self.client_id,
                              self.user_id, None)
    def commit(self, change_type):
        if change_type == ChangeType.ADDED:
            before = None
            after = self
        elif change_type == ChangeType.MODIFIED:
            assert self.before != None
            before = self.before
            after = self
        elif change_type == ChangeType.REMOVED:
            before = self
            after = None
        self.change_cb(change_type, before, after)
    def check_access(self, user_id):
        if user_id != self.user_id:
            raise PermissionError("user id %s may not changed service owned "
                                  "by user id %s" % (user_id, self.user_id))

class ServiceDiscovery:
    def __init__(self, max_user_resources, max_total_resources,
                 service_change_cb=None):
        self.resource_manager = \
            ResourceManager(max_user_resources, max_total_resources)
        self.services = {}
        self.subscriptions = {}
        self.clients = {}
        self.service_change_cb = service_change_cb
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
            service.prepare()
            service.make_orphan(now)
            service.commit(ChangeType.MODIFIED)
        for subscription in \
            list(self.get_subscriptions_with_client_id(client_id)):
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
            if generation == service.generation:
                if service.client_id != client_id or service.is_orphan():
                    # owner comes back - with new or reused client id
                    service.prepare()
                    # The client id is allowed to change (though you might
                    # argue against this permissive behavior), however the
                    # assumption is that the user stays the same, so no
                    # need to transfer resource tokens.
                    service.client_id = client_id
                    service.adopted()
                    service.commit(ChangeType.MODIFIED)
                return service
            elif generation > service.generation:
                service.prepare()
                service.generation = generation
                service.ttl = ttl
                service.client_id = client_id
                service.props = service_props
                service.adopted()
                service.commit(ChangeType.MODIFIED)
                return service
            else:
                raise GenerationError("invalid generation %d: existing service "
                                      "already at generation %d" % \
                                      (generation, service.generation))
        else:
            self.resource_manager.allocate(user_id, ResourceType.SERVICE)
            service = Service(service_id, generation, service_props, ttl,
                              None, client_id, user_id, self._service_commit)
            self.services[service_id] = service
            service.commit(ChangeType.ADDED)
            return service
    def purge_orphans(self):
        now = time.time()
        timed_out = set()
        for service in self.services.values():
            if service.is_orphan() and now > service.orphan_timeout():
                timed_out.add(service.service_id)
        for orphan_id in timed_out:
            self._unpublish(orphan_id)
        return timed_out
    def next_orphan_timeout(self):
        candidate = None
        for service in self.services.values():
            if service.is_orphan():
                if candidate == None or service.orphan_timeout() < candidate:
                    candidate = service.orphan_timeout()
        return candidate
    def _unpublish(self, service_id):
        service = self.services.pop(service_id)
        self.resource_manager.deallocate(service.user_id, ResourceType.SERVICE)
        service.commit(ChangeType.REMOVED)
    def unpublish(self, service_id, client_id):
        user_id = self._get_user_id(client_id)
        service = self.services.get(service_id)
        if service == None:
            raise NotFoundError("service", service_id)
        service.check_access(user_id)
        self._unpublish(service_id)
    def has_service(self, service_id):
        return service_id in self.services
    def get_service(self, service_id):
        service = self.services.get(service_id)
        if service == None:
            raise NotFoundError("service", service_id)
        return service
    def get_services(self):
        return self.services.values()
    def get_services_with_client_id(self, client_id):
        for service in self.services.values():
            if service.client_id == client_id:
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
            subscription.notify(ChangeType.ADDED, None, service)
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
        if subscription == None:
            raise NotFoundError("subscription", sub_id)
        subscription.check_access(client_id)
        self._remove_subscription(subscription)
    def _service_commit(self, change_type, before, after):
        for subscription in self.subscriptions.values():
            subscription.notify(change_type, before, after)
        if self.service_change_cb != None:
            self.service_change_cb(change_type, before, after)
