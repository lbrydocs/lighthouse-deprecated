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
from lbrynet.metadata.Metadata import Metadata, verify_name_characters
from lbrynet.lbrynet_daemon.ExchangeRateManager import ExchangeRateManager
from lighthouse.conf import MAX_SD_TRIES, CACHE_DIR

log = logging.getLogger(__name__)


class MetadataUpdater(object):
    def __init__(self):
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        self.api = JSONRPCProxy.from_url(API_CONNECTION_STRING)
        self.cache_dir = CACHE_DIR
        self.cache_file = os.path.join(self.cache_dir, "lighthouse.sqlite")
        self.cost_updater = LoopingCall(self._update_costs)
        self.exchange_rate_manager = ExchangeRateManager()
        self.show_status = LoopingCall(self._display_catchup_status)
        self.name_checker = LoopingCall(self._initialize_metadata)
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
        self.non_complying_claims = []
        self.bad_uris = []
        self._chain_height = 0
        self._blocks_to_go = False
        self.cost_and_availability = {n: {'cost': 0.0, 'available': False} for n in self.metadata}

    def _display_catchup_status(self):
        if self._blocks_to_go:
            log.info("%i blocks to go", self._blocks_to_go)

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
            assert name == name.lower()
            claim_info = {
                'name': name,
                'claim_id': claim_id,
                'nout': nout,
                'metadata': Metadata(json.loads(claim['value']), process_now=False)
            }
            self._claims.update({txid: claim_info})
            if claim_id in self.non_complying_claims:
                self.non_complying_claims.remove(claim_id)
        except ValueError:
            log.info("Claim %s has invalid metadata json", claim_id)
            if claim_id not in self.non_complying_claims:
                self.non_complying_claims.append(claim_id)
            self._claims.update({txid: {'name': name, 'claim_id': claim_id, 'nout': nout}})
        except AssertionError:
            log.info("Claim %s has non conforming metadata", claim_id)
            if claim_id not in self.non_complying_claims:
                self.non_complying_claims.append(claim_id)
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
                assert name == name.lower()
                decoded = json.loads(base64.b64decode(encoded_meta))
                verify_name_characters(name)
                meta = Metadata(decoded, process_now=False)
                ver = meta.get('ver', '0.0.1')
                log.debug("lbry://%s conforms to metadata version %s" % (name, ver))
                self._claims.update({tx: {'name': name, 'claim_id': claim_id, 'nout': n, 'metadata': meta}})
                sd_hash = meta['sources']['lbry_sd_hash']
                self._check_name_at_height(self._chain_height, name)
                if claim_id in self.non_complying_claims:
                    self.non_complying_claims.remove(claim_id)
                d = self._load_sd_attempts(sd_hash)
                d.addCallback(lambda _: self._load_stream_size(sd_hash))
                return d
            except:
                if claim_id not in self.non_complying_claims:
                    self.non_complying_claims.append(claim_id)
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

    def _get_valid_at_height(self, name, claim_id):
        claims = self.api.get_claims_for_name({'name': name})['claims']
        for claim in claims:
            if claim['claimId'] == claim_id:
                return claim['nValidAtHeight']
        return None

    def _add_claims_for_height(self, height):
        to_add = []
        block = self.api.get_block({'height': height})
        transactions = block['tx']
        block_hash = block['hash']
        for tx in transactions:
            claims = self.api.get_claims_for_tx({'txid': tx})
            if claims:
                for claim in claims:
                    if claim.get('in claim trie', False):
                        next_height = self._get_valid_at_height(claim['name'], claim['claimId'])
                        log.info("%s, %i", claim['name'], next_height)
                        self._claims_to_check.append(claim['name'])
                        to_add.append((claim['name'], claim, tx))
                        self._check_name_at_height(self._chain_height + 1, claim['name'])
                        self._check_name_at_height(next_height, claim['name'])

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
        self._chain_height = chain_height
        if chain_height > cache_height:
            log.debug("Catching up with blockchain")
            self._blocks_to_go = chain_height - cache_height
            if not self.show_status.running:
                self.show_status.start(30)
            d = self._add_claims_for_height(cache_height + 1)
            d.addCallback(lambda _: reactor.callLater(0, self.catchup))
        else:
            log.info("Up to date with blockchain")
            self._blocks_to_go = False
            if self.show_status.running:
                self.show_status.stop()
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
                    assert name == name.lower()
                    meta = Metadata(claim['metadata'], process_now=False)
                    self.metadata[name] = meta
                except AssertionError:
                    log.info("Bad metadata for lbry://%s", name)
            else:
                log.debug("Missing metadata for lbry://%s", name)
                if 'claim_id' in claim:
                    self._handle_bad_new_claim(name, claim['claim_id'])

    def _update_winning_name(self, name, claim):
        claim_id = claim['claimId']
        self.metadata[name] = self._claims[claim['txid']]['metadata']
        self.claimtrie[name] = self._claims[claim['txid']]
        if claim_id in self.non_complying_claims:
            self.non_complying_claims.remove(claim_id)
        log.info("Updated lbry://%s", name)

    def _handle_bad_new_claim(self, name, claim_id):
        if claim_id not in self.non_complying_claims:
            self.non_complying_claims.append(claim_id)
            log.warning("Missing metadata for lbry://%s", name)
            self._delayed_check_claim(name)

    def refresh_winning_name(self):
        """
        if there are no claims left to check, retry in 30 seconds
        if there are claims to check, update the next one with no delay
        """

        if not self._claims_to_check:
            reactor.callLater(30, self.refresh_winning_name)
        else:
            name = self._claims_to_check.pop()
            log.info("Checking winning claim for lbry://%s", name)
            claims = self.api.get_claims_for_name({'name': name})
            if claims['claims']:
                current = claims['claims'][0]
                try:
                    self._update_winning_name(name, current)
                except KeyError:
                    self._handle_bad_new_claim(name, current['claimId'])
            reactor.callLater(0, self.refresh_winning_name)

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
        log.info("claim for lbry://%s does not conform to any specification", name)
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

    def _on_block_height(self, height, fn, *args):
        if height >= self._chain_height:
            reactor.callLater(0, fn, *args)
        else:
            reactor.callLater(30, self._on_block_height, height, fn, *args)

    def _delayed_check_claim(self, name, delay=30):
        def _do_claim_check(_name):
            log.debug("Checking lbry://%s", _name)
            self._claims_to_check.append(_name)
        reactor.callLater(delay, _do_claim_check, name)

    def _check_name_at_height(self, height, name):
        def _check_name(_name):
            if _name not in self._claims_to_check:
                self._claims_to_check.append(_name)

        self._on_block_height(height, _check_name, name)

    def _start(self):
        self.update_descriptors()
        self._initialize_metadata()
        log.info("*********************")
        log.info("Loaded %i names", len(self.metadata))
        log.info("Loaded %i claims", len(self._claims))
        log.info("Started!")
        log.info("*********************")
        self.refresh_winning_name()
        self.exchange_rate_manager.start()
        self.cost_updater.start(60)
        self.name_checker.start(1800, now=False)

    def start(self):
        d = self._open_db()
        d.addCallback(lambda _: self.load_claims())
        d.addCallback(lambda _: log.info("Catching up with the blockchain"))
        d.addCallback(lambda _: self.catchup())

    def stop(self):
        log.info("*********************")
        log.info("Stopping updater")

        def stop_if_running(looping_call):
            if looping_call.running:
                looping_call.stop()

        for lcall in (self.cost_updater, self.show_status, self.name_checker):
            stop_if_running(lcall)

        self.exchange_rate_manager.stop()
