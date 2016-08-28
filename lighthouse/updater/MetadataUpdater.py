import json
import os
import time
import logging.handlers

from twisted.internet import defer, reactor, threads
from twisted.internet.task import LoopingCall
from jsonrpc.proxy import JSONRPCProxy
from lbrynet.conf import API_CONNECTION_STRING, MIN_BLOB_DATA_PAYMENT_RATE
from lbrynet.metadata.LBRYMetadata import Metadata, verify_name_characters
from lbrynet.lbrynet_daemon.LBRYExchangeRateManager import ExchangeRateManager
from lighthouse.conf import MAX_SD_TRIES

log = logging.getLogger()


class MetadataUpdater(object):
    def __init__(self):
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        self.api = JSONRPCProxy.from_url(API_CONNECTION_STRING)
        self.cache_file = os.path.join(os.path.expanduser("~/"), "lighthouse_cache")
        self.claimtrie_updater = LoopingCall(self._update_claimtrie)
        self.sd_updater = LoopingCall(self._update_descriptors)
        self.cost_updater = LoopingCall(self._update_costs)
        self.exchange_rate_manager = ExchangeRateManager()

        if os.path.isfile(self.cache_file):
            log.info("Loading cache")
            f = open(self.cache_file, "r")
            r = json.loads(f.read())
            f.close()
            self.claimtrie = r.get('claimtrie', [])
            self.metadata = r.get('metadata', {})
            self.sd_cache = r.get('sd_cache', {})
            self.sd_attempts = r.get('sd_attempts', {})
            self.bad_uris = r.get('bad_uris', [])
            self.cost_and_availability = r.get('cost_and_availability', {})
        else:
            log.info("Rebuilding metadata cache")
            self.claimtrie = []
            self.metadata = {}
            self.sd_cache = {}
            self.sd_attempts = {}
            self.bad_uris = []
            self.cost_and_availability = {n: {'cost': 0.0, 'available': False} for n in self.metadata}
        self.descriptors_to_download = []
        for name in self.metadata:
            sd_hash = self.metadata[name]['sources']['lbry_sd_hash']
            if not self.sd_cache.get(sd_hash, False) and self.sd_attempts.get(sd_hash, 0) < MAX_SD_TRIES:
                self.descriptors_to_download.append(sd_hash)

    def _filter_claimtrie(self):
        claims = self.api.get_nametrie()
        r = []
        for claim in claims:
            if claim['txid'] not in self.bad_uris:
                try:
                    verify_name_characters(claim['name'])
                    r.append(claim)
                except AssertionError:
                    self.bad_uris.append(claim['txid'])
                    log.info("Bad name for claim %s" % claim['txid'])
        return r

    def _update_claimtrie(self):
        claimtrie = self._filter_claimtrie()
        if claimtrie != self.claimtrie:
            for claim in claimtrie:
                if claim['name'] not in self.metadata:
                    self._update_metadata(claim)
                elif claim['txid'] != self.metadata[claim['name']]['txid']:
                    self._update_metadata(claim)

    def _save_stream_descriptor(self, sd_hash):
        if self.sd_cache.get(sd_hash, False):
            return
        self.sd_cache[sd_hash] = self.api.download_descriptor({'sd_hash': sd_hash})
        if not self.sd_cache[sd_hash]:
            self.sd_attempts[sd_hash] = self.sd_attempts.get(sd_hash, 0) + 1
            if self.sd_attempts[sd_hash] < MAX_SD_TRIES:
                self.descriptors_to_download.append(sd_hash)

    def _save_metadata(self, claim, metadata):
        try:
            m = Metadata(metadata)
        except:
            return self._notify_bad_metadata(claim)
        ver = m.get('ver', '0.0.1')
        log.info("lbry://%s conforms to metadata version %s" % (claim['name'], ver))
        self.metadata[claim['name']] = m
        self.metadata[claim['name']]['txid'] = claim['txid']
        if claim not in self.claimtrie:
            self.claimtrie.append(claim)
        sd_hash = m['sources']['lbry_sd_hash']
        if not self.sd_cache.get(sd_hash, False) and self.sd_attempts.get(sd_hash, 0) < MAX_SD_TRIES and sd_hash not in self.descriptors_to_download:
            self.descriptors_to_download.append(sd_hash)

        return self._cache_metadata()

    def _notify_bad_metadata(self, claim):
        log.info("lbry://%s does not conform to any specification" % str(claim['name']))
        if claim['txid'] not in self.bad_uris:
            self.bad_uris.append(claim['txid'])
        return self._cache_metadata()

    def _update_metadata(self, claim):
        d = defer.succeed(None)
        d.addCallback(lambda _: self.api.get_claims_for_tx({'txid': claim['txid']}))
        d.addCallback(lambda claims: json.loads(claims[0]['value']))
        d.addCallbacks(lambda metadata: self._save_metadata(claim, metadata),
                       lambda _: self._notify_bad_metadata(claim))
        return d

    def _update_descriptors(self):
        sds_to_get = []
        while self.descriptors_to_download:
            sds_to_get.append(self.descriptors_to_download.pop())
        d = defer.DeferredList([threads.deferToThread(self._save_stream_descriptor, sd_hash) for sd_hash in sds_to_get])
        d.addCallback(lambda _: self._cache_metadata())

    def _update_costs(self):
        d = defer.DeferredList([threads.deferToThread(self._get_cost, n) for n in self.metadata])
        d.addCallback(lambda _: self._cache_metadata())

    def _cache_metadata(self):
        r = {
                'metadata': self.metadata,
                'claimtrie': self.claimtrie,
                'bad_uris': self.bad_uris,
                'sd_cache': self.sd_cache,
                'sd_attempts': self.sd_attempts,
                'cost_and_availability': self.cost_and_availability
        }
        f = open(self.cache_file, "w")
        f.write(json.dumps(r))
        f.close()
        return defer.succeed(None)

    def _get_cost(self, name):
        sd = self.sd_cache.get(self.metadata[name]['sources']['lbry_sd_hash'], None)

        if self.metadata[name].get('fee', False):
            fee = self.exchange_rate_manager.to_lbc(self.metadata[name]['fee']).amount
        else:
            fee = 0.0

        if sd:
            if isinstance(MIN_BLOB_DATA_PAYMENT_RATE, float):
                min_data_rate = {'LBC': {'amount': MIN_BLOB_DATA_PAYMENT_RATE, 'address': ''}}
            else:
                min_data_rate = MIN_BLOB_DATA_PAYMENT_RATE
            stream_size = sum([blob['length'] for blob in sd['blobs']]) / 1000000.0
            data_cost = self.exchange_rate_manager.to_lbc(min_data_rate).amount * stream_size
            available = True
        else:
            data_cost = 0.0
            available = False
        self.cost_and_availability[name] = {'cost': data_cost + fee, 'available': available, 'ts': time.time()}

    def start(self):
        log.info("Starting exchange rate manager")
        self.exchange_rate_manager.start()
        log.info("Starting claim updater")
        self.claimtrie_updater.start(30)
        log.info("Starting sd updater")
        self.sd_updater.start(30)
        log.info("Starting cost updater")
        self.cost_updater.start(60)
        log.info("started!")

    def stop(self):
        log.info("Stopping updater")
        if self.claimtrie_updater.running:
            self.claimtrie_updater.stop()
        if self.sd_updater.running:
            self.sd_updater.stop()
        if self.cost_updater.running:
            self.cost_updater.stop()
        self.exchange_rate_manager.stop()
