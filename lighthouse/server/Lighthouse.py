import time
import logging.handlers
from decimal import Decimal
from txjsonrpc import jsonrpclib
from txjsonrpc.web.jsonrpc import Handler
from txjsonrpc.web import jsonrpc
from twisted.internet import defer, reactor, error
from twisted.web import server
from lighthouse.conf import REFLECTOR_PORT
from lighthouse.server.reflector import SDReflectorServerFactory
from lighthouse.updater.Updater import DBUpdater
from lighthouse.search.search import LighthouseSearch

log = logging.getLogger(__name__)


class Lighthouse(jsonrpc.JSONRPC):
    def __init__(self):
        jsonrpc.JSONRPC.__init__(self)
        reactor.addSystemEventTrigger('before', 'shutdown', self.shutdown)
        self.database_updater = DBUpdater()
        self.search_engine = LighthouseSearch(self.database_updater)
        self.fuzzy_name_cache = []
        self.fuzzy_ratio_cache = {}
        self.unique_clients = {}
        self.sd_cache = {}

    def render(self, request):
        request.content.seek(0, 0)
        # Unmarshal the JSON-RPC data.
        content = request.content.read()
        try:
            parsed = jsonrpclib.loads(content)
        except ValueError:
            return server.failure
        functionPath = parsed.get("method")
        if functionPath not in ["search"]:
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

    def _start_reflector(self):
        log.info("Starting sd reflector")
        reflector_factory = SDReflectorServerFactory(
                    self.database_updater.availability_manager.peer_manager,
                    self.database_updater.availability_manager.blob_manager,
                    self.database_updater
        )
        try:
            self.reflector_server_port = reactor.listenTCP(REFLECTOR_PORT, reflector_factory)
            log.info('Started sd reflector on port %i', REFLECTOR_PORT)
        except error.CannotListenError as e:
            log.exception("Couldn't bind sd reflector to port %d", self.reflector_port)
            raise ValueError("{} lighthouse may already be running on your computer.".format(e))

    def start(self):
        d = self.database_updater.start()
        d.addCallback(lambda _: self._start_reflector())
        d.addErrback(log.exception)

    def shutdown(self):
        self.database_updater.stop()

    def jsonrpc_search(self, search):
        return self.search_engine.search(search)