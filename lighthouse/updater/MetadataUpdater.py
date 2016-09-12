import json
import os
import time
import base64
import logging.handlers

from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.internet.task import LoopingCall
from jsonrpc.proxy import JSONRPCProxy
from lbrynet.conf import API_CONNECTION_STRING, MIN_BLOB_DATA_PAYMENT_RATE
from lbrynet.metadata.LBRYMetadata import Metadata, verify_name_characters
from lbrynet.lbrynet_daemon.LBRYExchangeRateManager import ExchangeRateManager
from lighthouse.conf import MAX_SD_TRIES, CACHE_DIR

log = logging.getLogger()


class MetadataUpdater(object):
    def __init__(self):
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        self.api = JSONRPCProxy.from_url(API_CONNECTION_STRING)
        self.cache_dir = CACHE_DIR
        self.cache_file = os.path.join(self.cache_dir, "lighthouse.sqlite")
        self.cost_updater = LoopingCall(self._update_costs)
        self.exchange_rate_manager = ExchangeRateManager()
        self.name_refresher = LoopingCall(self._initialize_metadata)
        self.db = None
        self._is_running = False
        self._claims_to_check = []
        self.claimtrie = {}
        self._last_time = time.time()
        self.descriptors_to_download = []
        self._claims = {}
        self.metadata = {}
        self.size_cache = {}
        self.sd_attempts = {}
        self.stream_size = {}
        self.bad_uris = []
        self.cost_and_availability = {n: {'cost': 0.0, 'available': False} for n in self.metadata}

    def _open_db(self):
        log.info("open db")
        self.db = adbapi.ConnectionPool('sqlite3', self.cache_file, check_same_thread=False)

        def create_tables(transaction):
            transaction.execute("create table if not exists blocks (" +
                                "    block_height integer, " +
                                "    block_hash text, " +
                                "    claim_txid text)")
            transaction.execute("create table if not exists claims (" +
                                "    claimid text, " +
                                "    name text, " +
                                "    txid text," +
                                "    n integer)")
            transaction.execute("create table if not exists metadata (" +
                                "    claimid text, " +
                                "    txid text, " +
                                "    value text)")
            transaction.execute("create table if not exists stream_size (" +
                                "    sd_hash text, " +
                                "    total_bytes integer)")
            transaction.execute("create table if not exists blob_attempts (" +
                                "    hash text, " +
                                "    num_tries integer)")

        return self.db.runInteraction(create_tables)

    def _get_cached_height(self):
        d = self.db.runQuery("select max(block_height) from blocks")
        d.addCallback(lambda r: r[0][0] if r[0][0] else 0)
        return d

    def _add_claim(self, block_hash, height, name, claim, txid):
        d = self.db.runQuery("select * from blocks where claim_txid=?", (txid,))
        d.addCallback(lambda r: self._update_claim_database(block_hash, height, name, claim, txid) if not r else True)
        return d

    def _update_claim_database(self, block_hash, height, name, claim, txid):
        if 'supported claimId' in claim:
            log.info("Skipping claim support %s for lbry://%s, claimid: %s", txid, name, claim['supported claimId'])
            return defer.succeed(None)

        claim_value = base64.b64encode(claim['value'])
        nout = claim['nOut']
        claim_id = claim['claimId']
        try:
            self._claims.update({txid: {'name': name, 'claim_id': claim_id, 'nout': nout, 'metadata': Metadata(json.loads(claim['value']), process_now=False)}})
        except AssertionError:
            self._claims.update({txid: {'name': name, 'claim_id': claim_id, 'nout': nout}})

        log.info("Add claim for lbry://%s, id: %s, tx: %s", name, claim_id, txid)

        d = self.db.runQuery("insert into blocks values (?, ?, ?)", (height, block_hash, txid))
        d.addCallback(lambda _: self.db.runQuery("insert into claims values (?, ?, ?, ?)", (claim_id, name, txid, nout)))
        d.addCallback(lambda _: self.db.runQuery("insert into metadata values (?, ?, ?)", (claim_id, txid, claim_value)))
        return d

    def _load_stream_size(self, sd_hash):
        def _save(size):
            if size:
                log.debug("Load size for %s", sd_hash)
                self.size_cache[sd_hash] = size
            if not self.size_cache.get(sd_hash, False) and self.sd_attempts.get(sd_hash, 0) < MAX_SD_TRIES and sd_hash not in self.descriptors_to_download:
                self.descriptors_to_download.append(sd_hash)
            return defer.succeed(True)

        d = self.db.runQuery("select total_bytes from stream_size where sd_hash=?", (sd_hash,))
        d.addCallback(lambda r: False if not len(r) else r[0][0])
        d.addCallback(_save)
        return d

    def _load_sd_attempts(self, sd_hash):
        def _save(attempts):
            if attempts:
                log.debug("Loaded %i attempts for %s", attempts, sd_hash)
                self.sd_attempts[sd_hash] = attempts
            return defer.succeed(True)

        d = self.db.runQuery("select num_tries from blob_attempts where hash=?", (sd_hash,))
        d.addCallback(lambda r: False if not len(r) else r[0][0])
        d.addCallback(_save)
        return d

    def _load_claim(self, tx):
        txid = tx[0]

        def _add_metadata(claim_id, name, txid, n):
            d = self.db.runQuery("select * from metadata where txid=? and claimid=?", (txid, claim_id))
            d.addCallback(lambda r: r[0][2])
            d.addCallback(lambda m: _save(claim_id, name, txid, n, m))
            return d

        def _save(claim_id, name, tx, n, encoded_meta):
            try:
                decoded = json.loads(base64.b64decode(encoded_meta))
                verify_name_characters(name)
                meta = Metadata(decoded, process_now=False)
                ver = meta.get('ver', '0.0.1')
                log.debug("lbry://%s conforms to metadata version %s" % (name, ver))
                self._claims.update({tx: {'name': name, 'claim_id': claim_id, 'nout': n, 'metadata': meta}})
                sd_hash = meta['sources']['lbry_sd_hash']
                d = self._load_sd_attempts(sd_hash)
                d.addCallback(lambda _: self._load_stream_size(sd_hash))
                return d
            except:
                self._claims.update({txid: {'name': name, 'claim_id': claim_id, 'nout': n}})
                return self._notify_bad_metadata(name, txid)

        d = self.db.runQuery("select * from claims where txid=?", (txid,))
        d.addCallback(lambda r: r[0])
        d.addCallback(lambda (c, nm, t, n): _add_metadata(c, nm, t, n))
        return d

    def load_claims(self):
        log.info("Loading blocks")
        d = self.db.runQuery("select claim_txid from blocks where claim_txid!=''")
        d.addCallback(lambda r: defer.DeferredList([self._load_claim(tx) for tx in r]))
        return d

    def _add_claims_for_height(self, height):
        to_add = []
        block = self.api.get_block({'height': height})
        transactions = block['tx']
        block_hash = block['hash']
        for tx in transactions:
            claims = self.api.get_claims_for_tx({'txid': tx})
            if claims:
                for claim in claims:
                    self._claims_to_check.append(claim['name'])
                    to_add.append((claim['name'], claim, tx))

        if height % 100 == 0:
            bps = 100/float(time.time() - self._last_time)
            self._last_time = time.time()
            log.debug("Imported %i blocks, %f blocks/s", height, bps)

        d = defer.DeferredList([self._add_claim(block_hash, height, name, claim, tx) for (name, claim, tx) in to_add])
        d.addCallback(lambda _: self.db.runQuery("select * from blocks where block_height=?", (height,)))
        d.addCallback(lambda r: self.db.runQuery("insert into blocks values (?, ?, ?)", (height, block_hash, "")) if not r else True)
        return d

    def _catch_up_claims(self, cache_height):
        chain_height = self.api.get_block({'blockhash': self.api.get_best_blockhash()})['height']
        if chain_height > cache_height:
            log.debug("Catching up with blockchain")
            d = self._add_claims_for_height(cache_height + 1)
            d.addCallback(lambda _: reactor.callLater(1, self.catchup))
        else:
            if not self._is_running:
                self._is_running = True
                self._start()
            d = defer.succeed(None)
            d.addCallback(lambda _: reactor.callLater(60, self.catchup))

    def _initialize_metadata(self):
        log.info("initializing metadata")
        nametrie = self.api.get_nametrie()
        for c in nametrie:
            name = c['name']
            txid = c['txid']
            claim = self._claims.get(txid, {})
            if 'metadata' in claim:
                try:
                    meta = Metadata(claim['metadata'], process_now=False)
                    self.metadata[name] = meta
                except AssertionError:
                    log.info("Bad metadata for lbry://%s", name)
            else:
                log.debug("Missing metadata for lbry://%s", name)

    def refresh_winning_name(self):
        def _delayed_add(name):
            log.info("Checking lbry://%s", name)
            self._claims_to_check.append(name)

        if len(self._claims_to_check):
            name = self._claims_to_check.pop()
            log.info("Checking winning claim for lbry://%s", name)
            current = self.api.get_claim_info({'name': name})
            if current:
                try:
                    self.metadata[name] = self._claims[current['txid']]['metadata']
                    self.claimtrie[name] = self._claims[current['txid']]
                    log.info("Updated lbry://%s", name)
                except KeyError:
                    reactor.callLater(30, _delayed_add, name)
            reactor.callLater(1, self.refresh_winning_name)
        else:
            reactor.callLater(30, self.refresh_winning_name)

    def _add_sd_attempt(self, sd_hash, n):
        d = self.db.runQuery("delete from blob_attempts where hash=?", (sd_hash,))
        d.addCallback(lambda _: self.db.runQuery("insert into blob_attempts values (?, ?)", (sd_hash, n)))

    def _get_stream_descriptor(self, sd_hash):
        log.info("trying to get sd %s", sd_hash)
        if self.stream_size.get(sd_hash, False):
            return
        sd = self.api.download_descriptor({'sd_hash': sd_hash})
        if not sd:
            self.sd_attempts[sd_hash] = self.sd_attempts.get(sd_hash, 0) + 1
            if self.sd_attempts[sd_hash] < MAX_SD_TRIES:
                self.descriptors_to_download.append(sd_hash)
            self._add_sd_attempt(sd_hash, self.sd_attempts[sd_hash])

        else:
            stream_size = sum([blob['length'] for blob in sd['blobs']])
            self._save_stream_size(sd_hash, stream_size)
        return defer.succeed(None)

    def _save_stream_size(self, sd_hash, total_bytes):
        log.info("Saving size info for %s", sd_hash)
        self.stream_size.update({sd_hash: total_bytes})
        d = self.db.runQuery("delete from stream_size where sd_hash=?", (sd_hash,))
        d.addCallback(lambda _: self.db.runQuery("insert into stream_size values (?, ?)", (sd_hash, total_bytes)))

    def _notify_bad_metadata(self, name, txid):
        log.debug("claim for lbry://%s does not conform to any specification", name)
        if txid not in self.bad_uris:
            self.bad_uris.append(txid)
        return defer.succeed(True)

    def update_descriptors(self):
        if len(self.descriptors_to_download):
            sd_hash = self.descriptors_to_download.pop()
            d = self._get_stream_descriptor(sd_hash)
            d.addCallback(lambda _: reactor.callLater(1, self.update_descriptors))
        else:
            reactor.callLater(30, self.update_descriptors)

    def _update_costs(self):
        d = defer.DeferredList([self._get_cost(n) for n in self.metadata], consumeErrors=True)

    def _get_cost(self, name):
        size = self.size_cache.get(self.metadata[name]['sources']['lbry_sd_hash'], None)

        if self.metadata[name].get('fee', False):
            fee = self.exchange_rate_manager.to_lbc(self.metadata[name]['fee']).amount
        else:
            fee = 0.0

        if size:
            if isinstance(MIN_BLOB_DATA_PAYMENT_RATE, float):
                min_data_rate = {'LBC': {'amount': MIN_BLOB_DATA_PAYMENT_RATE, 'address': ''}}
            else:
                min_data_rate = MIN_BLOB_DATA_PAYMENT_RATE
            stream_size = size / 1000000.0
            data_cost = self.exchange_rate_manager.to_lbc(min_data_rate).amount * stream_size
            available = True
        else:
            data_cost = 0.0
            available = False
        self.cost_and_availability[name] = {'cost': data_cost + fee, 'available': available, 'ts': time.time()}
        return defer.succeed(None)

    def catchup(self):
        d = self._get_cached_height()
        d.addCallback(self._catch_up_claims)
        return d

    def _start(self):
        self.update_descriptors()
        self._initialize_metadata()
        self.refresh_winning_name()
        self.exchange_rate_manager.start()
        self.cost_updater.start(60)
        log.info("*********************")
        log.info("Loaded %i names", len(self.metadata))
        log.info("Loaded %i claims", len(self._claims))
        log.info("Started!")
        log.info("*********************")

    def start(self):
        d = self._open_db()
        d.addCallback(lambda _: self.load_claims())
        d.addCallback(lambda _: self.catchup())

    def stop(self):
        log.info("*********************")
        log.info("Stopping updater")
        if self.cost_updater.running:
            self.cost_updater.stop()
        self.exchange_rate_manager.stop()
