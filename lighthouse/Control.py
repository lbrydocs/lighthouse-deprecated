import argparse
import sys

from twisted.web import server
from twisted.internet import reactor
from jsonrpc.proxy import JSONRPCProxy

from lbrynet import conf as lbrynet_conf
from lighthouse.server.api import Lighthouse
from lighthouse.server.LighthouseServer import LighthouseControllerServer, LighthouseServer
from lighthouse.updater.Claims import LBRYcrdManager


RPC_PORT = 50004


def cli():
    ecu = JSONRPCProxy.from_url("http://localhost:%i" % RPC_PORT)
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--lbrycrdd-data-dir')
    args = parser.parse_args()
    lbrynet_conf.initialize_settings()
    lbrynet_conf.settings.ensure_data_dir()
    claim_manager = LBRYcrdManager(args.lbrycrdd_data_dir)
    engine = Lighthouse(claim_manager)
    lighthouse_server = LighthouseServer(engine)
    ecu = LighthouseControllerServer(engine)
    engine.start()
    reactor.listenTCP(50005, server.Site(lighthouse_server.root))
    reactor.listenTCP(RPC_PORT, server.Site(ecu.root), interface="localhost")
    reactor.run()


def stop():
    ecu = JSONRPCProxy.from_url("http://localhost:%i" % RPC_PORT)
    try:
        r = ecu.is_running()
        ecu.stop()
        print "lighthouse stopped"
    except:
        print "lighthouse wasn't running"


if __name__ == "__main__":
    start()
