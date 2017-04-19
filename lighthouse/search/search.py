import logging.handlers
from twisted.internet import defer, threads
from fuzzywuzzy import process
from fuzzywuzzy.fuzz import UQRatio
import time
from lighthouse.util import add_or_move_to_front
from lighthouse.conf import CACHE_SIZE, MAX_RETURNED_RESULTS, DEFAULT_WEIGHTS
from lighthouse.conf import DEFAULT_SETTINGS, FILTERED, MAX_RESULTS_CACHED

log = logging.getLogger(__name__)


class FuzzyIndex(object):
    def __init__(self, get_index):
        self.get_index = get_index
        self.max_cache = CACHE_SIZE
        self.results_cache = {}
        self.search_cache = []
        self.cnt = 0

    def search(self, search, channel_claims=None):
        to_search = self.get_index()
        if channel_claims is not None:
            to_search = {k: v for k, v in to_search.iteritems() if k in channel_claims}
        results = process.extract(search, to_search,
                                  limit=MAX_RESULTS_CACHED, scorer=UQRatio)
        results_for_return = {}
        for _, score, claim_id in results:
            results_for_return[claim_id] = score
        return results_for_return


class LighthouseSearch(object):
    def __init__(self, claim_manager):
        self.claim_manager = claim_manager
        self.get_claim = claim_manager.claim_cache.get_cached_claim
        self.indexes = {
            "title": FuzzyIndex(self.claim_manager.claim_cache.get_title),
            "author": FuzzyIndex(self.claim_manager.claim_cache.get_author),
            "description": FuzzyIndex(self.claim_manager.claim_cache.get_description),
        }

    def search(self, search, search_keys=None, channel_claims=None):
        if not search:
            return None

        if search_keys is None:
            search_keys = ['title']
        results = {}
        for search_by in search_keys:
            if search_by not in self.indexes:
                return {'error': 'invalid search key: \"%s\"' % search_by}
            search_result = self.indexes[search_by].search(search, channel_claims)
            for claim_id, score in search_result.iteritems():
                if claim_id in FILTERED:
                    continue
                if claim_id not in results:
                    results[claim_id] = score
                elif score > results[claim_id]:
                    results[claim_id] = score
        return results

    @defer.inlineCallbacks
    def format_response(self, search_results):
        response = []
        if not search_results:
            defer.returnValue(None)
        for claim_id, score in sorted(search_results.iteritems(), key=lambda x: x[1], reverse=True):
            claim = yield self.get_claim(claim_id)
            if claim and 'metadata' in claim:
                del claim['metadata']
            if claim:
                response.append(claim)
            if len(response) >= 25:
                break
        defer.returnValue(response)
