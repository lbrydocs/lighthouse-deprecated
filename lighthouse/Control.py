from lighthouse.server.LighthouseServer import LighthouseControllerServer, LighthouseServer
from twisted.web import server
from twisted.internet import reactor
from jsonrpc.proxy import JSONRPCProxy
import logging.handlers
import sys
import os


DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s"
DEFAULT_FORMATTER = logging.Formatter(DEFAULT_FORMAT)

log = logging.getLogger()
console_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler(os.path.join(os.path.expanduser("~/"), "lighthouse.log"))
console_handler.setFormatter(DEFAULT_FORMATTER)
file_handler.setFormatter(DEFAULT_FORMATTER)
log.addHandler(console_handler)
log.addHandler(file_handler)
log.setLevel(logging.INFO)
logging.getLogger("lbrynet").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)



def cli():
    ecu = JSONRPCProxy.from_url("http://localhost:50004")
    try:
        s = ecu.is_running()
    except:
        print "lighthouse isn't running"
        sys.exit(1)
    args = sys.argv[1:]
    meth = args[0]
    if args:
        print ecu.call(meth)
    else:
        print ecu.call(meth, args)


def start():
    engine = LighthouseServer()
    ecu = LighthouseControllerServer(engine.search_engine)
    engine.start()
    s = server.Site(engine.root)
    e = server.Site(ecu.root)

    reactor.listenTCP(50005, s)
    reactor.listenTCP(50004, e, interface="localhost")
    reactor.run()


def stop():
    ecu = JSONRPCProxy.from_url("http://localhost:50004")
    try:
        r = ecu.is_running()
        ecu.stop()
        print "lighthouse stopped"
    except:
        print "lighthouse wasn't running"
