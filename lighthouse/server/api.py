import time
import logging.handlers
from decimal import Decimal
from txjsonrpc import jsonrpclib
from txjsonrpc.web.jsonrpc import Handler
from txjsonrpc.web import jsonrpc
from twisted.internet import defer, reactor, error, threads
from twisted.web import server
from lighthouse.conf import REFLECTOR_PORT
from lighthouse.server.reflector import SDReflectorServerFactory
from lighthouse.updater.Availability import StreamAvailabilityManager
from lighthouse.search.search import LighthouseSearch

log = logging.getLogger(__name__)


class Lighthouse(jsonrpc.JSONRPC):
    def __init__(self, claim_manager):
        jsonrpc.JSONRPC.__init__(self)
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        self.claim_manager = claim_manager
        self.availability_manager = StreamAvailabilityManager(claim_manager.storage)
        self.search_engine = LighthouseSearch(self.claim_manager)

    def _log_response(self, result, peer, time_in, method, params):
        log.info("%s - %s(%s): %f", peer, method, str(params), round(time.time()-time_in, 5))
        return result

    def render(self, request):
        t_in = time.time()
        request.content.seek(0, 0)
        # Unmarshal the JSON-RPC data.
        content = request.content.read()
        try:
            parsed = jsonrpclib.loads(content)
        except ValueError:
            return server.failure
        functionPath = parsed.get("method")
        args = parsed.get('params')
        id = parsed.get('id')
        version = parsed.get('jsonrpc')
        peer = request.transport.getPeer()

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
            d = defer.maybeDeferred(function, *args)
            d.addErrback(self._ebRender, id)
            d.addCallback(self._cbRender, request, id, version)
            d.addCallback(self._log_response, peer, t_in, functionPath, args)
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


    def _start_reflector(self):
        log.info("Starting sd reflector")
        reflector_factory = SDReflectorServerFactory(
                    self.availability_manager.peer_manager,
                    self.availability_manager.blob_manager,
                    self.claim_manager.claim_cache,
                    self.claim_manager.storage
        )
        try:
            self.reflector_server_port = reactor.listenTCP(REFLECTOR_PORT, reflector_factory)
            log.info('Started sd reflector on port %i', REFLECTOR_PORT)
        except error.CannotListenError as e:
            log.exception("Couldn't bind sd reflector to port %d", self.reflector_port)
            raise ValueError("{} lighthouse may already be running on your computer.".format(e))

    @defer.inlineCallbacks
    def start(self):
        yield self.claim_manager.start()
        yield self.availability_manager.start()
        self._start_reflector()

    def shutdown(self):
        yield self.claim_manager.stop()
        yield self.availability_manager.stop()
        log.info("Shutting down")

    @defer.inlineCallbacks
    def jsonrpc_search(self, search, search_by=None, channel_filter=None):

        if channel_filter is not None:
            claim_ids = yield self.claim_manager.storage.get_channel_claim_ids(channel_filter)
        else:
            claim_ids = None

        result = yield threads.deferToThread(self.search_engine.search, search, search_by, claim_ids)
        response = yield self.search_engine.format_response(result)
        defer.returnValue(response)

    def jsonrpc_get_size_for_name(self, name):
        return self.claim_manager.storage.get_stream_size_for_name(name)

    def jsonrpc_get_size_for_claim(self, claim_id):
        return self.claim_manager.storage.get_stream_size_for_claim(claim_id)

    def jsonrpc_get_size_for_stream(self, sd_hash):
        return self.claim_manager.storage.get_stream_size(sd_hash)
