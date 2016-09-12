import logging.handlers
from twisted.web import resource
from lighthouse.server.Lighthouse import Lighthouse
from lighthouse.server.LighthouseControl import LighthouseController

log = logging.getLogger(__name__)


class Index(resource.Resource):
    def __init__(self):
        resource.Resource.__init__(self)

    isLeaf = False

    def _delayed_render(self, request, results):
        request.write(str(results))
        request.finish()

    def getChild(self, name, request):
        if name == '':
            return self
        return resource.Resource.getChild(self, name, request)


class LighthouseServer(object):
    def __init__(self):
        self.root = Index()
        self.search_engine = Lighthouse()
        self.root.putChild("", self.search_engine)

    def start(self):
        self.search_engine.start()


class LighthouseControllerServer(object):
    def __init__(self, engine):
        self.root = Index()
        self._controller = LighthouseController(engine)
        self.root.putChild("", self._controller)