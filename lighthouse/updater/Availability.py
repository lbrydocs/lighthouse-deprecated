import os
import logging
import base64
from time import time
from twisted.internet import reactor, defer
from twisted.internet.task import LoopingCall
from lbrynet.core.client.DHTPeerFinder import DHTPeerFinder
from lbrynet.core.server.DHTHashAnnouncer import DHTHashAnnouncer
from lbrynet.core.PeerManager import PeerManager
from lbrynet.core.BlobManager import DiskBlobManager
from lbrynet.core.StreamDescriptor import BlobStreamDescriptorReader
from lbrynet.conf import settings
from lbrynet.dht.node import Node
from lighthouse.conf import LBRYID

log = logging.getLogger(__name__)


class StreamAvailabilityManager(object):
    def __init__(self, db):
        self.db = db
        self.lbryid = base64.decodestring(LBRYID)
        self.peer_manager = None
        self.peer_finder = None
        self.dht_node = None
        self.hash_announcer = None
        self.blob_manager = None
        self.dht_node_port = 4444
        self.blob_data_dir = settings.data_dir
        self.blob_dir = os.path.join(self.blob_data_dir, settings.BLOBFILES_DIR)

        self.peer_port = 3333
        self.known_dht_nodes = [
            ('lbrynet1.lbry.io', 4444),
            ('lbrynet2.lbry.io', 4444),
            ('lbrynet3.lbry.io', 4444)
        ]
        self.external_ip = '127.0.0.1'

    def _update_availability_db(self, claim_id, sd_hash, peers):
        d = self.db.runQuery("insert or replace into stream_availability values (?, ?, ?, ?)",
                             (claim_id, sd_hash, peers, int(time())))
        return d

    def _update_stream_size_db(self, claim_id, sd_hash, total_bytes):
        d = self.db.runQuery(
            "insert or replace into stream_size values (?, ?, ?)",
            (claim_id, sd_hash, total_bytes))
        return d

    def get_size_for_name(self, name):
        d = self.db.runQuery(
            "select total_bytes from stream_size "
            "where claim_id=(select claim_id from claims where uri=?)",
            (name, ))
        d.addCallback(lambda total_size: None if not total_size else next(s[0] for s in total_size))
        return d

    def get_total_unavailable(self):
        d = self.db.runQuery("select claim_id from stream_availability where peers=0")
        d.addCallback(lambda (claims, ): len(claims))
        return d

    def get_mean_availability(self):
        d = self.db.runQuery("select peers from stream_availability where peers>0")
        d.addCallback(lambda (peers, ): float(sum(peers)) / float(len(peers)))
        return d

    def get_availability_for_name(self, name):
        d = self.db.runQuery("select claim_id from claims where uri=?", (name, ))
        d.addCallback(
            lambda (claim_id, ): self.db.runQuery(
                "select peers from stream_availability where claim_id=?",
                (claim_id[0], )))
        d.addCallback(lambda (peers, ): peers[0])
        return d

    def update_stream_size(self, claim_id, sd_hash):
        def _should_update_size(sd_hash):
            if sd_hash:
                d = self.blob_manager.get_blob(sd_hash[0], True)
                d.addCallback(BlobStreamDescriptorReader)
                d.addCallback(lambda sd_blob: sd_blob.get_info())
                d.addCallback(lambda blob_info: sum(blob['length'] for blob in blob_info['blobs']))
                d.addCallback(
                    lambda total_bytes: self._update_stream_size_db(
                        claim_id, sd_hash[0], total_bytes))
                return d
            return None
        d = self.blob_manager.completed_blobs([sd_hash])
        d.addCallback(_should_update_size)
        return d

    def update_availability(self, claim_id, sd_hash):
        d = self.get_peers_for_hash(sd_hash)
        d.addCallback(lambda peers: self._update_availability_db(claim_id, sd_hash, len(peers)))
        d.addCallback(lambda _: self.update_stream_size(claim_id, sd_hash))
        return d

    def iter_update(self, stream_infos):
        log.debug("Updating availabilities for %i streams", len(stream_infos))
        for claim_id, sd_hash in stream_infos:
            yield self.update_availability(claim_id, sd_hash)

    def update(self):
        d = self.db.runQuery("select claim_id, sd_hash from metadata")
        d.addCallback(lambda r: list(self.iter_update(r)))
        return d

    def start(self):
        if self.peer_manager is None:
            self.peer_manager = PeerManager()

        def match_port(h, p):
            return h, p

        def join_resolved_addresses(result):
            addresses = []
            for success, value in result:
                if success is True:
                    addresses.append(value)
            return addresses

        def start_dht(addresses):
            log.info("Starting the dht")
            log.info("lbry id: %s", base64.encodestring(self.lbryid).strip("\n"))
            self.dht_node.joinNetwork(addresses)
            self.peer_finder.run_manage_loop()
            self.hash_announcer.run_manage_loop()
            return self.blob_manager.setup()

        ds = []

        for host, port in self.known_dht_nodes:
            d = reactor.resolve(host)
            d.addCallback(match_port, port)
            ds.append(d)

        if self.dht_node is None:
            self.dht_node = Node(
                udpPort=self.dht_node_port,
                lbryid=self.lbryid,
                externalIP=self.external_ip
            )
        if self.peer_finder is None:
            self.peer_finder = DHTPeerFinder(self.dht_node, self.peer_manager)
        if self.hash_announcer is None:
            self.hash_announcer = DHTHashAnnouncer(self.dht_node, self.peer_port)
        if self.blob_manager is None:
            self.blob_manager = DiskBlobManager(
                self.hash_announcer, self.blob_dir, self.blob_data_dir)

        d1 = defer.DeferredList(ds)
        d1.addCallback(join_resolved_addresses)
        d1.addCallback(start_dht)
        d2 = self.blob_manager.setup()
        dl = defer.DeferredList([d1, d2], fireOnOneErrback=True, consumeErrors=True)
        return dl

    def stop(self):
        log.info("Shutting down availability manager")
        ds = []
        if self.blob_manager is not None:
            ds.append(defer.maybeDeferred(self.blob_manager.stop))
        if self.dht_node is not None:
            ds.append(defer.maybeDeferred(self.dht_node.stop))
        if self.peer_finder is not None:
            ds.append(defer.maybeDeferred(self.peer_finder.stop))
        if self.hash_announcer is not None:
            ds.append(defer.maybeDeferred(self.hash_announcer.stop))
        return defer.DeferredList(ds)

    def get_peers_for_hash(self, blob_hash):
        return self.peer_finder.find_peers_for_blob(blob_hash)
