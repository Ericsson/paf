import enum
import time
import copy

class ChangeType(enum.Enum):
    ADDED=1
    MODIFIED=2
    REMOVED=3

class MatchType(enum.Enum):
    APPEARED=1
    MODIFIED=2
    DISAPPEARED=3

class Subscription:
    def __init__(self, sub_id, filter, owner, match_cb):
        self.sub_id = sub_id
        self.filter = filter
        self.owner = owner
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

class Service:
    def __init__(self, service_id, generation, props, ttl, orphan_since,
                 owner, change_cb):
        self.service_id = service_id
        self.generation = generation
        self.props = props
        self.ttl = ttl
        self.orphan_since = orphan_since
        self.owner = owner
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
                              self.orphan_since, self.owner, None)
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

class ServiceDiscovery:
    def __init__(self, service_change_cb=None):
        self.services = {}
        self.subscriptions = {}
        self.service_change_cb = service_change_cb
    def publish(self, service_id, generation, service_props, ttl, owner):
        if service_id in self.services:
            service = self.services[service_id]
            if generation == service.generation and service.is_orphan():
                service.prepare()
                service.owner = owner
                service.adopted()
                service.commit(ChangeType.MODIFIED)
                return service
            elif generation > service.generation:
                service.prepare()
                service.generation = generation
                service.ttl = ttl
                service.owner = owner
                service.props = service_props
                service.adopted()
                service.commit(ChangeType.MODIFIED)
                return service
            else:
                return None
        else:
            service = Service(service_id, generation, service_props, ttl,
                              None, owner, self._service_commit)
            self.services[service_id] = service
            service.commit(ChangeType.ADDED)
            return service
    def client_disconnect(self, client_id):
        now = time.time()
        for service in self.get_services_with_owner(client_id):
            service.prepare()
            service.make_orphan(now)
            service.commit(ChangeType.MODIFIED)
        for sub_id, subscription in list(self.subscriptions.items()):
            if subscription.owner == client_id:
                del self.subscriptions[sub_id]
    def purge_orphans(self):
        now = time.time()
        timed_out = set()
        for service in self.services.values():
            if service.is_orphan() and now > service.orphan_timeout():
                timed_out.add(service.service_id)
        for orphan_id in timed_out:
            self.unpublish(orphan_id)
        return timed_out
    def next_orphan_timeout(self):
        candidate = None
        for service in self.services.values():
            if service.is_orphan():
                if candidate == None or service.orphan_timeout() < candidate:
                    candidate = service.orphan_timeout()
        return candidate
    def unpublish(self, service_id):
        service = self.services.pop(service_id)
        service.commit(ChangeType.REMOVED)
    def has_service(self, service_id):
        return service_id in self.services
    def get_service(self, service_id):
        return self.services[service_id]
    def get_services(self):
        return self.services.values()
    def get_services_with_owner(self, owner):
        owned = []
        for service in self.services.values():
            if service.owner == owner:
                owned.append(service)
        return owned
    def has_subscription(self, sub_id):
        return sub_id in self.subscriptions
    def create_subscription(self, sub_id, filter, owner, match_cb):
        assert not self.has_subscription(sub_id)
        subscription = Subscription(sub_id, filter, owner, match_cb)
        self.subscriptions[sub_id] = subscription
    def activate_subscription(self, sub_id):
        subscription = self.subscriptions[sub_id]
        for service in self.services.values():
            subscription.notify(ChangeType.ADDED, None, service)
    def get_subscription(self, sub_id):
        return self.subscriptions[sub_id]
    def remove_subscription(self, sub_id):
        del self.subscriptions[sub_id]
    def _service_commit(self, change_type, before, after):
        for subscription in self.subscriptions.values():
            subscription.notify(change_type, before, after)
        if self.service_change_cb != None:
            self.service_change_cb(change_type, before, after)
