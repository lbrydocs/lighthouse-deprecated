import logging.handlers
from twisted.internet import defer, threads
from fuzzywuzzy import process
from lighthouse.util import add_or_move_to_front
from lighthouse.conf import CACHE_SIZE, MAX_RETURNED_RESULTS, DEFAULT_WEIGHTS
from lighthouse.conf import METADATA_INDEXES, DEFAULT_SETTINGS, FILTERED, MAX_RESULTS_CACHED

log = logging.getLogger(__name__)


class FuzzyIndex(object):
    def __init__(self, index, updater):
        self.index = index
        self.updater = updater
        self.max_cache = CACHE_SIZE
        self.results_cache = {}
        self.search_cache = []

    def search(self, value, max_results=MAX_RESULTS_CACHED, settings=DEFAULT_SETTINGS):
        d = self.get_search_results(value, max_results, settings)
        return d

    def get_search_results(self, value, max_results, settings):
        if settings:
            force = settings.get('force', False)
        else:
            force = False

        if not force:
            if value in self.results_cache:
                return defer.succeed(self.results_cache[value])
        return self.process_search(value, max_results, settings)

    def process_search(self, search, max_results, settings):
        return self._process_search(search, max_results, settings)

    def _update_cache(self, k, r):
        if len(self.search_cache) > self.max_cache or k in self.results_cache:
            del self.results_cache[self.search_cache.pop()]
        self.search_cache.reverse()
        self.search_cache.append(k)
        self.search_cache.reverse()
        self.results_cache.update({k: r})
        return r

    def _process_search(self, search, max_results, settings):
        return defer.fail(NotImplementedError())


class FuzzyNameIndex(FuzzyIndex):
    def __init__(self, updater):
        FuzzyIndex.__init__(self, 'name', updater)

    def _process_search(self, search, max_results=MAX_RESULTS_CACHED, settings=DEFAULT_SETTINGS):
        d = threads.deferToThread(
            process.extract,
            search,
            {n: n for n in self.updater.names},
            limit=max_results
        )
        d.addCallback(lambda r: self._update_cache(search, r))
        return d


class FuzzyMetadataIndex(FuzzyIndex):
    def _process_search(self, search, max_results=MAX_RESULTS_CACHED, settings=DEFAULT_SETTINGS):
        _metadata = self.updater.metadata
        d = threads.deferToThread(
            process.extract,
            search,
            {n: _metadata[n][self.index] for n in self.updater.names},
            limit=max_results,
        )
        d.addCallback(lambda r: self._update_cache(search, r))
        return d


class LighthouseSearch(object):
    def __init__(self, updater):
        self.updater = updater
        self.indexes = {key: FuzzyMetadataIndex(key, self.updater) for key in METADATA_INDEXES}
        self.indexes.update({'name': FuzzyNameIndex(self.updater)})

    def _get_dict_for_return(self, name):
        r = {
            'name': name,
            'value': self.updater.metadata[name],
            'availability': self.updater.availability[name],
            'stream_size': self.updater.stream_sizes.get(name, False)
        }
        return r

    def search(self, search, settings=DEFAULT_SETTINGS):
        def search_by(search, settings):
            search_keys = settings.get('search_by')
            dl = []
            for search_by in search_keys:
                d = self.indexes[search_by].search(search)
                dl.append(d)
            d = defer.DeferredList(dl)
            d.addCallback(lambda r: {search_keys[i]: r[i][1] for i in range(len(search_keys))})
            return d

        def _apply_weights(results):
            r = {}
            for k in results:
                applied_weights = []
                for v in results[k]:
                    weight = DEFAULT_WEIGHTS[k]
                    # penalty for nearly empty fields that can otherwise score high
                    if len(v[0]) < 2:
                        weight = 0.25
                    applied_weights.append((v[2], v[1] * weight))
                r.update({k: applied_weights})
            return r

        def _sort(results):
            return sorted(results, key=lambda x: x[1], reverse=True)

        def _combine(results):
            raw_results = []
            for r in results:
                raw_results += results[r]
            results_without_duplicates = []
            # keep highest scoring unique search results
            for name, score in raw_results:
                if name in [r[0] for r in raw_results]:
                    existing_val = next((r for r in raw_results if r[0] == name and score != score), False)
                else:
                    existing_val = False

                if name not in FILTERED:
                    if not existing_val:
                        results_without_duplicates.append((name, score))
                    elif score > existing_val[1]:
                        results_without_duplicates.remove(existing_val)
                        results_without_duplicates.append((name, score))
            return results_without_duplicates

        def _format(results):
            shortened = results[:MAX_RETURNED_RESULTS]
            final = []
            for r in shortened:
                final.append(self._get_dict_for_return(r[0]))
            return final

        def _direct_match(search, results):
            results_for_return = results
            if search in self.updater.metadata:
                # if the name isn't in the results, add it as the top result
                # if it is in the results, make sure it's the top result
                results_for_return = add_or_move_to_front(search, results_for_return)
            return results_for_return

        # lcase search as to by default weigh lower-case names higher than names with odd capitalizations
        search = search.lower()
        d = search_by(search, settings)
        d.addCallback(_apply_weights)
        d.addCallback(_combine)
        d.addCallback(_sort)
        d.addCallback(lambda r: _direct_match(search, r))
        d.addCallback(_format)
        return d
