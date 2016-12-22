import os
import logging.handlers

from twisted.enterprise import adbapi
from twisted.internet.task import LoopingCall
from twisted.internet import defer
from twisted.internet import reactor

from lighthouse.conf import CACHE_DIR
from lighthouse.updater.Availability import StreamAvailabilityManager
from lighthouse.updater.Claimtrie import ClaimManager

log = logging.getLogger(__name__)


class DatabaseAlreadyOpenError(Exception):
    pass


class AlreadyRunningError(Exception):
    pass


class NotRunningError(Exception):
    pass


def start_if_not_running(looping_call, t=30):
    if not looping_call.running:
        looping_call.start(t)


def stop_if_running(looping_call):
    if looping_call.running:
        looping_call.stop()


class DBUpdater(object):
    def __init__(self, blockchain_manager):
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        self.db = None
        self.cache_dir = CACHE_DIR
        self.db_path = os.path.join(self.cache_dir, "lighthouse.sqlite")

        self.blockchain_manager = blockchain_manager
        self.availability_manager = None
        self.claimtrie_manager = None

        self._metadata_cache = {}
        self._availability_cache = {}
        self._size_cache = {}
        self._sd_hashes = []

        self.cache_updater = LoopingCall(self.update_caches)

    @staticmethod
    def _create_tables(transaction):
        transaction.execute("create table if not exists claims (" +
                            "    claim_id text primary key, " +
                            "    uri text not null, " +
                            "    claim_data blob, " +
                            "    txid text not null, " +
                            "    nout integer not null, " +
                            "    height integer not null, " +
                            "    amount integer not null, " +
                            "    in_claimtrie integer)")
        transaction.execute("create table if not exists supports (" +
                            "    txid text primary key, " +
                            "    nout integer not null, " +
                            "    claim_id text not null, " +
                            "    height integer not null, " +
                            "    amount integer not null, " +
                            "    in_support_map integer, " +
                            "    foreign key(claim_id) references claims(claim_id))")
        transaction.execute("create table if not exists metadata (" +
                            "    claim_id text primary key, " +
                            "    title text, " +
                            "    author text, " +
                            "    description text, " +
                            "    language text, " +
                            "    license text, " +
                            "    nsfw boolean, " +
                            "    thumbnail text, " +
                            "    preview text, " +
                            "    content_type text, " +
                            "    sd_hash text, " +
                            "    license_url text, " +
                            "    ver text, " +
                            "    foreign key(claim_id) references claims(claim_id))")
        transaction.execute("create table if not exists fee (" +
                            "    claim_id text primary key, " +
                            "    currency text not null, " +
                            "    address text not null, " +
                            "    amount integer not null, " +
                            "    foreign key(claim_id) references metadata(claim_id))")
        transaction.execute("create table if not exists claimtrie (" +
                            "    uri text primary key, " +
                            "    claim_id text not null, " +
                            "    txid text not null, " +
                            "    nout integer not null, " +
                            "    foreign key(claim_id) references claims(claim_id))")
        transaction.execute("create table if not exists stream_size (" +
                            "    claim_id text primary key, " +
                            "    sd_hash text not null, " +
                            "    total_bytes integer, " +
                            "    foreign key(claim_id) references claims(claim_id))")
        transaction.execute("create table if not exists skipped_claims (" +
                            "    txid text primary key, " +
                            "    nout integer not null)")
        transaction.execute("create table if not exists stream_availability (" +
                            "    claim_id text primary key, " +
                            "    sd_hash text not null, " +
                            "    peers number not null, " +
                            "    last_checked number not null,"
                            "    foreign key(claim_id) references claims(claim_id))")

    def _setup(self):
        if self.db is not None:
            raise DatabaseAlreadyOpenError
        self.db = adbapi.ConnectionPool('sqlite3', self.db_path, check_same_thread=False)
        self.availability_manager = StreamAvailabilityManager(self.db)
        self.claimtrie_manager = ClaimManager(self.db, self.blockchain_manager, self.availability_manager)
        d = self.db.runInteraction(self._create_tables)
        return d

    def _start_looping_calls(self):
        start_if_not_running(self.cache_updater, 180)

    def _stop_looping_calls(self):
        stop_if_running(self.cache_updater)

    def start(self):
        d = self._setup()
        d.addCallback(lambda _: self.availability_manager.start())
        d.addCallback(lambda _: self.blockchain_manager.start())
        d.addCallback(lambda _: self._start_looping_calls())
        d.addErrback(log.exception)
        return d

    def stop(self):
        log.info("Shutting down")
        d = safe_stop(self.availability_manager)
        d.addCallback(lambda _: safe_stop(self.blockchain_manager))
        d.addCallback(lambda _: self._stop_looping_calls())

    def _update_metadata_cache(self, metadata, name):
        self._metadata_cache[name] = metadata
        return

    def _update_availability_cache(self, peers, name):
        self._availability_cache[name] = peers
        return

    def _update_size_cache(self, size, name):
        if size:
            self._size_cache[name] = size
        return

    def update_sd_hashes(self):
        self._sd_hashes = [self.metadata[name]['sources']['lbry_sd_hash'] for name in self.names]
        return

    def _update_name(self, name):
        def _do_update(_metadata):
            d = defer.succeed(self._update_metadata_cache(_metadata, name))
            d.addCallback(lambda _: self.availability_manager.get_availability_for_name(name))
            d.addCallback(self._update_availability_cache, name)
            d.addCallback(lambda _: self.availability_manager.get_size_for_name(name))
            d.addCallback(self._update_size_cache, name)
            return d

        d = self.claimtrie_manager.metadata_manager.get_winning_metadata(name)
        d.addCallback(lambda metadata: False if not metadata else _do_update(metadata))
        d.addErrback(log.exception)
        return d

    def update_names(self, names):
        return defer.DeferredList([self._update_name(name) for name in names])

    def update_caches(self):
        d = self.claimtrie_manager.update()
        d.addCallback(lambda _: self.availability_manager.update())
        d.addCallback(lambda _: self.claimtrie_manager.get_claimed_names())
        d.addCallback(lambda names: self.update_names(names))
        d.addCallback(lambda _: self.update_sd_hashes())
        d.addErrback(log.exception)

    @property
    def metadata(self):
        return self._metadata_cache

    @property
    def availability(self):
        return self._availability_cache

    @property
    def names(self):
        return self._metadata_cache.keys()

    @property
    def sd_hashes(self):
        return self._sd_hashes

    @property
    def stream_sizes(self):
        return self._size_cache

    def get_stream_info(self, name):
        r = dict(name=name, value=self.metadata[name], peer_count=self.availability[name],
                 stream_size=self.stream_sizes.get(name, False))
        return r


def safe_stop(manager):
    if manager:
        return manager.stop()
    else:
        return defer.succeed(True)
