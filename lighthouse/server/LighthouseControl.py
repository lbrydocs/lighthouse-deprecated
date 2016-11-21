import logging.handlers
from twisted.internet import reactor
from txjsonrpc.web import jsonrpc

log = logging.getLogger(__name__)


class LighthouseController(jsonrpc.JSONRPC):
    def __init__(self, l):
        jsonrpc.JSONRPC.__init__(self)
        self.lighthouse = l

    def jsonrpc_dump_sessions(self):
        return self.lighthouse.unique_clients

    def jsonrpc_dump_metadata_cache(self):
        return self.lighthouse.database_updater.metadata

    def jsonrpc_dump_indexes(self):
        r = {}
        for i in self.lighthouse.search_engine.indexes:
            r.update({i: self.lighthouse.search_engine.indexes[i].results_cache})
        return r

    def jsonrpc_dump_size_cache(self):
        return self.lighthouse.database_updater.size_cache

    def jsonrpc_dump_availability_cache(self):
        return self.lighthouse.database_updater.availability

    def jsonrpc_stop(self):
        reactor.callLater(0.0, reactor.stop)
        return True

    def jsonrpc_is_running(self):
        return self.lighthouse.running