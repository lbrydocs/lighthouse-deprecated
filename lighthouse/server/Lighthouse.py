import logging.handlers
import time

from txjsonrpc import jsonrpclib
from txjsonrpc.web.jsonrpc import Handler
from decimal import Decimal
from twisted.internet import defer, reactor
from twisted.web import server
from txjsonrpc.web import jsonrpc
from fuzzywuzzy import process
from lighthouse.updater.MetadataUpdater import MetadataUpdater
from lighthouse.conf import CACHE_SIZE, MAX_RETURNED_RESULTS, DEFAULT_SEARCH_KEYS
from lighthouse.search.search import LighthouseSearch

log = logging.getLogger()


class Lighthouse(jsonrpc.JSONRPC):
    def __init__(self):
        jsonrpc.JSONRPC.__init__(self)
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        self.metadata_updater = MetadataUpdater()
        self.search_engine = LighthouseSearch(self.metadata_updater)
        self.fuzzy_name_cache = []
        self.fuzzy_ratio_cache = {}
        self.unique_clients = {}
        self.sd_cache = {}
        self.running = False

    def render(self, request):
        request.content.seek(0, 0)
        # Unmarshal the JSON-RPC data.
        content = request.content.read()
        parsed = jsonrpclib.loads(content)
        functionPath = parsed.get("method")
        if functionPath not in ["search", "announce_sd", "check_available"]:
            return server.failure
        args = parsed.get('params')
        if len(args) != 1:
            return server.failure
        arg = args[0]
        id = parsed.get('id')
        version = parsed.get('jsonrpc')
        try:
            log.info("%s@%s: %s" % (functionPath, request.getClientIP(), arg))
        except Exception as err:
            log.error(err.message)

        if self.unique_clients.get(request.getClientIP(), None) is None:
            self.unique_clients[request.getClientIP()] = [[functionPath, arg, time.time()]]
        else:
            self.unique_clients[request.getClientIP()].append([functionPath, arg, time.time()])
        if version:
            version = int(float(version))
        elif id and not version:
            version = jsonrpclib.VERSION_1
        else:
            version = jsonrpclib.VERSION_PRE1
        # XXX this all needs to be re-worked to support logic for multiple
        # versions...
        try:
            function = self._getFunction(functionPath)
        except jsonrpclib.Fault, f:
            self._cbRender(f, request, id, version)
        else:
            request.setHeader("access-control-allow-origin", "*")
            request.setHeader("content-type", "text/json")
            d = defer.maybeDeferred(function, arg)
            d.addErrback(self._ebRender, id)
            d.addCallback(self._cbRender, request, id, version)
        return server.NOT_DONE_YET

    def _cbRender(self, result, request, id, version):
        def default_decimal(obj):
            if isinstance(obj, Decimal):
                return float(obj)

        if isinstance(result, Handler):
            result = result.result

        if version == jsonrpclib.VERSION_PRE1:
            if not isinstance(result, jsonrpclib.Fault):
                result = (result,)
            # Convert the result (python) to JSON-RPC
        try:
            s = jsonrpclib.dumps(result, version=version, default=default_decimal)
        except:
            f = jsonrpclib.Fault(self.FAILURE, "can't serialize output")
            s = jsonrpclib.dumps(f, version=version)
        request.setHeader("content-length", str(len(s)))
        request.write(s)
        request.finish()

    def start(self):
        self.running = True
        self.metadata_updater.start()

    def shutdown(self):
        self.running = False
        self.metadata_updater.stop()

    # def _get_dict_for_return(self, name):
    #     r = {
    #             'name': name,
    #             'value': self.metadata_updater.metadata[name],
    #             'cost': self.metadata_updater.cost_and_availability[name]['cost'],
    #             'available': self.metadata_updater.cost_and_availability[name]['available'],
    #     }
    #     return r
    #
    # def _process_search(self, search, keys):
    #     log.info("Processing search: %s" % search)
    #     results = []
    #     for search_by in keys:
    #         r = process.extract(search, [self.metadata_updater.metadata[m][search_by] for m in self.metadata_updater.metadata], limit=10)
    #         r2 = [i[0] for i in r]
    #         r3 = [self._get_dict_for_return(m) for m in self.metadata_updater.metadata if self.metadata_updater.metadata[m][search_by] in r2]
    #         results += [next(i for i in r3 if i['value'][search_by] == n) for n in r2]
    #
    #     final_results = []
    #     for result in results:
    #         if result['value'] not in [v['value'] for v in final_results]:
    #             final_results.append(result)
    #         if len(final_results) >= MAX_RETURNED_RESULTS:
    #             break
    #
    #     return final_results

    def jsonrpc_search(self, search, search_by=DEFAULT_SEARCH_KEYS):
        return self.search_engine.search(search)

    def jsonrpc_announce_sd(self, sd_hash):
        sd = self.metadata_updater.sd_cache.get(sd_hash, False)
        if sd:
            return "Already announced"
        self.metadata_updater.sd_attempts[sd_hash] = 0
        self.metadata_updater.descriptors_to_download.append(sd_hash)
        return "Pending"

    def jsonrpc_check_available(self, sd_hash):
        if self.metadata_updater.sd_cache.get(sd_hash, False):
            return True
        else:
            return False