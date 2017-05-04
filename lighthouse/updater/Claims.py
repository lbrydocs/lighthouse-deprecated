import logging
import subprocess
import socket
import os
import time

import sqlite3
from twisted.enterprise import adbapi

from lbryschema.decode import smart_decode
from lbryschema import uri
from lbryschema.error import DecodeError, URIParseError
from lbryschema.claim import ClaimDict
from lbryschema.fee import Fee
from lbryumserver import deserialize
from decimal import Decimal
from appdirs import user_data_dir
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from twisted.internet import threads, defer, reactor, task
from twisted.internet.task import LoopingCall
from lighthouse import conf

import sys
log = logging.getLogger(__name__)


USE_TXINDEX = True
VERBOSE_LBRYCRDD = False
COIN = Decimal(10**8)
common_words = [
    'the',
    'of',
    'to',
    'and',
    'a',
    'in',
    'is',
    'it',
    'you',
    'that',
    'he',
    'was',
    'for',
    'on',
    'are',
    'with',
    'as',
    'i',
    'his',
    'they',
    'be',
    'at',
    'one',
    'have',
    'this',
    'from',
    'or',
    'had',
    'by',
    'hot',
    'word'
]


def _catch_connection_error(f):
    def w(*args):
        try:
            return f(*args)
        except socket.error:
            raise ValueError(
                "Unable to connect to an lbrycrd server. Make sure an lbrycrd server "
                "is running and that this application can connect to it.")
    return w


def rerun_if_locked(f):
    def rerun(err, *args, **kwargs):
        if err.check(sqlite3.OperationalError) and err.value.message == "database is locked":
            log.warning("database was locked. rerunning %s with args %s, kwargs %s",
                        str(f), str(args), str(kwargs))
            return task.deferLater(reactor, 0, wrapper, *args, **kwargs)
        return err

    def wrapper(*args, **kwargs):
        d = f(*args, **kwargs)
        d.addErrback(rerun, *args, **kwargs)
        return d

    return wrapper


def get_lbrycrdd_connection_string(wallet_conf):
    settings = {"username": "rpcuser",
                "password": "rpcpassword",
                "rpc_port": 9245}
    if wallet_conf and os.path.exists(wallet_conf):
        with open(wallet_conf, "r") as conf:
            conf_lines = conf.readlines()
        for l in conf_lines:
            if l.startswith("rpcuser="):
                settings["username"] = l[8:].rstrip('\n')
            if l.startswith("rpcpassword="):
                settings["password"] = l[12:].rstrip('\n')
            if l.startswith("rpcport="):
                settings["rpc_port"] = int(l[8:].rstrip('\n'))

    rpc_user = settings["username"]
    rpc_pass = settings["password"]
    rpc_port = settings["rpc_port"]
    rpc_url = "127.0.0.1"
    return "http://%s:%s@%s:%i" % (rpc_user, rpc_pass, rpc_url, rpc_port)


def get_lbrycrdd_path(data_dir):
    path_file = os.path.join(data_dir, "lbrycrdd_path")
    if os.path.isfile(path_file):
        with open(path_file, "r") as f:
            return f.read().strip()
    log.warning("No lbrycrdd path set in %s, trying to find it in the cwd (%s)", path_file, os.getcwd())
    return "lbrycrdd"


class SQLiteStorage(object):
    def __init__(self):
        self.db_path = os.path.join(conf.CACHE_DIR, "lighthouse.sqlite")
        self.db = adbapi.ConnectionPool('sqlite3', self.db_path, check_same_thread=False)

    @defer.inlineCallbacks
    def start(self):
        reactor.addSystemEventTrigger("before", "shutdown", self.stop)
        yield self.db.runOperation(
             "CREATE TABLE IF NOT EXISTS claims ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "claim_id CHAR(40),"
             "claim_name TEXT,"
             "raw_value TEXT,"
             "txid TEXT,"
             "nout INTEGER,"
             "address TEXT,"
             "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")

        yield self.db.runOperation(
            "CREATE TABLE IF NOT EXISTS channels ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "channel_claim_id INTEGER NOT NULL,"
            "claim_id INTEGER NOT NULL,"
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "FOREIGN KEY(channel_claim_id) REFERENCES claims(id) ON UPDATE RESTRICT ON DELETE CASCADE,"
            "FOREIGN KEY(claim_id) REFERENCES claims(id) ON UPDATE RESTRICT ON DELETE CASCADE)")

        yield self.db.runOperation(
            "CREATE TABLE IF NOT EXISTS metadata ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "claim_id INTEGER UNIQUE NOT NULL, "
            "title TEXT,"
            "author TEXT,"
            "description TEXT,"
            "language TEXT,"
            "license TEXT,"
            "license_url TEXT,"
            "thumbnail TEXT,"
            "preview TEXT,"
            "content_type TEXT,"
            "nsfw BOOL,"
            "fee TEXT,"
            "sd_hash TEXT,"
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "FOREIGN KEY(claim_id) REFERENCES claims(id) ON UPDATE RESTRICT ON DELETE CASCADE)")

        yield self.db.runOperation(
            "CREATE TABLE IF NOT EXISTS stream_sizes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "claim_id INTEGER UNIQUE NOT NULL, "
            "stream_size INTEGER,"
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            "FOREIGN KEY(claim_id) REFERENCES claims(id) ON DELETE CASCADE)")

        yield self.db.runOperation(
            "CREATE TABLE IF NOT EXISTS claimtrie ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "claim_name TEXT UNIQUE,"
            "claim_id INTEGER,"
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")

    @rerun_if_locked
    def _run_operation(self, query, args=None):
        if args is None:
            return self.db.runOperation(query)
        return self.db.runOperation(query, args)

    @rerun_if_locked
    def _run_query(self, query, args=None):
        if args is None:
            return self.db.runQuery(query)
        return self.db.runQuery(query, args)

    @defer.inlineCallbacks
    def stop(self):
        yield self.db.close()

    @defer.inlineCallbacks
    def _add_claim(self, claim_id, name, serialized, txid, nout, address):
        yield self._run_operation("INSERT INTO "
                                   "claims(claim_id, claim_name, raw_value, txid, nout, address) "
                                   "VALUES (?, ?, ?, ?, ?, ?)",
                                   (claim_id, name, serialized, txid, nout, address))
        row = yield self._run_query("SELECT id FROM claims WHERE claim_id=?", (claim_id, ))
        defer.returnValue(row[0][0])

    @defer.inlineCallbacks
    def _add_stripped_claim_metadata(self, row_id, title, author, description, language, license,
                            license_url, thumbnail, preview, content_type, nsfw, fee, sd_hash):
        title = " ".join([ngram for ngram in title.lower().split(" ") if ngram not in common_words])
        description = " ".join([ngram for ngram in description.lower().split(" ") if ngram not in common_words])

        yield self._run_operation("INSERT INTO "
                                   "metadata(claim_id, title, author, description, language, "
                                   "license, license_url, thumbnail, preview, content_type, "
                                   "nsfw, fee, sd_hash) "
                                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row_id, title, author, description, language, license,
                                    license_url, thumbnail, preview, content_type, nsfw, fee,
                                    sd_hash))

    @defer.inlineCallbacks
    def add_claim(self, claim_id, name, claim, txid, nout, address):
        claim_row = yield self._add_claim(claim_id, name, claim.serialized.encode('hex'), txid,
                                          nout, address)
        claim_pb = claim.protobuf
        fee = None
        if claim_pb.stream.metadata.HasField("fee"):
            fee = claim_pb.stream.metadata.fee.SerializeToString().encode('hex')
        sd_hash = claim.source_hash
        meta_pb = claim_pb.stream.metadata
        source_pb = claim_pb.stream.source
        yield self._add_stripped_claim_metadata(claim_row, meta_pb.title, meta_pb.author,
                                       meta_pb.description, meta_pb.language, meta_pb.license,
                                       meta_pb.licenseUrl,
                                       meta_pb.thumbnail, meta_pb.preview, source_pb.contentType,
                                       meta_pb.nsfw, fee, sd_hash)
        yield self._run_operation("INSERT INTO stream_sizes(claim_id) VALUES (?)", (claim_row,))

    @defer.inlineCallbacks
    def get_metadata(self, claim_id):
        query_str = "SELECT * FROM metadata WHERE id=(SELECT id FROM claims WHERE claim_id=?)"
        results = yield self._run_query(query_str, (claim_id, ))
        if results:
            defer.returnValue(results[0])
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def register_claim_to_channel(self, claim_id, channel_claim_id):
        yield self._run_operation("INSERT INTO channels(channel_claim_id, claim_id) "
                                   "VALUES ((SELECT id FROM claims WHERE claim_id=?),"
                                   "       (SELECT id FROM claims WHERE claim_id=?))",
                                   (channel_claim_id, claim_id))

    @defer.inlineCallbacks
    def claim_outpoint_is_known(self, claim_id, txid, nout):
        results = yield self._run_query("SELECT txid, nout FROM claims WHERE claim_id=?",
                                         (claim_id,))
        response = False
        if results:
            t, n = results[0]
            if txid == t and nout == n:
                response = True
        defer.returnValue(response)

    @defer.inlineCallbacks
    def _get_channel_infos(self):
        channel_info = yield self._run_query("SELECT claim_id, channel_claim_id FROM channels")
        channel_map = {}
        for claim, channel in channel_info:
            r = yield self.db.runQuery("SELECT claim_name, claim_id FROM claims WHERE id=?",
                                       (channel,))
            channel_map[claim] = r[0]
        defer.returnValue(channel_map)

    @defer.inlineCallbacks
    def get_channel_claim_ids(self, channel_id):
        r = yield self._run_query("SELECT claims.claim_id FROM channels INNER JOIN claims ON claims.id=channels.claim_id WHERE channels.channel_claim_id=(SELECT id FROM claims WHERE claims.claim_id=?)", (channel_id, ))
        results = [x[0] for x in r]
        defer.returnValue(results)

    @defer.inlineCallbacks
    def get_claims(self):
        claim_dump = yield self._run_query("SELECT claims.claim_id, claim_name, raw_value, txid, nout, "
                                                    "address, metadata.* "
                                            "FROM claims "
                                            "INNER JOIN metadata "
                                            "ON metadata.claim_id=claims.id")

        channel_map = yield self._get_channel_infos()
        results = []

        for args in claim_dump:
            claim_id, name, raw, txid, nout, addr = args[0], args[1], args[2], args[3], args[4], args[5]
            metadata = args[6:]
            claim = {
                'claim_id': claim_id,
                'name': name,
                'claim': ClaimDict.deserialize(raw.decode('hex')).claim_dict,
                'txid': txid,
                'nout': nout,
                'address': addr,
                'metadata': metadata
            }
            if metadata[1] in channel_map:
                claim['channel_name'], claim['channel_id'] = channel_map[metadata[1]]
            results.append(claim)
        defer.returnValue(results)

    @defer.inlineCallbacks
    def get_claim(self, claim_id):
        result = yield self._run_query("SELECT claim_id, claim_name, raw_value, txid, nout, address"
                                        " FROM claims WHERE claim_id=?", (claim_id, ))
        if result:
            claim_id, name, raw, txid, nout, address = result[0]
            r = {
                'claim_id': claim_id,
                'name': name,
                'claim': ClaimDict.deserialize(raw.decode('hex')),
                'txid': txid,
                'nout': nout,
                'address': address
            }
            defer.returnValue(r)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_stream_size(self, sd_hash):
        results = yield self._run_query("SELECT stream_size FROM stream_sizes INNER JOIN metadata ON stream_sizes.claim_id=metadata.claim_id WHERE metadata.sd_hash=?", (sd_hash, ))
        response = None
        if results:
            response = results[0][0]
        defer.returnValue(response)

    @defer.inlineCallbacks
    def get_stream_size_for_claim(self, claim_id):
        results = yield self._run_query("SELECT stream_size FROM stream_sizes INNER JOIN claims ON claims.id=stream_sizes.claim_id WHERE claims.claim_id=?", (claim_id, ))
        response = None
        if results:
            response = results[0][0]
        defer.returnValue(response)

    @defer.inlineCallbacks
    def get_stream_size_for_name(self, name):
        results = yield self._run_query("SELECT stream_size FROM stream_sizes INNER JOIN claimtrie ON claimtrie.claim_id=stream_sizes.claim_id WHERE claimtrie.claim_name=?", (name, ))
        response = None
        if results:
            response = results[0][0]
        defer.returnValue(response)

    @defer.inlineCallbacks
    def update_claimtrie(self, claimtrie):
        known_winning = yield self._run_query("SELECT claimtrie.claim_name, claims.txid, claims.nout FROM claimtrie INNER JOIN claims ON claimtrie.claim_id=claims.id")
        known_winning_dict = {name: (txid, nout) for (name, txid, nout) in known_winning}
        log.info("%i names are known", len(known_winning_dict))
        update_cnt = 0
        current_claimtrie = {}
        for node in claimtrie:
            if node and 'txid' in node:
                current_claimtrie[node['name']] = (node['txid'], node['n'])

        drop_names = []
        for name in known_winning_dict:
            if name not in current_claimtrie:
                drop_names.append(name)

        for to_drop in drop_names:
            log.info("Deleting %s", to_drop)
            yield self._run_operation("DELETE FROM claimtrie WHERE name=?", (to_drop,))

        changes = {}
        for name, winning in current_claimtrie.iteritems():
            winning_txid, winning_nout = winning
            if not name:
                continue
            if name not in known_winning_dict:
                changes[name] = (winning_txid, winning_nout)
            else:
                known_txid, known_nout = known_winning_dict[name]
                if known_txid != winning_txid or known_nout != winning_nout:
                    changes[name] = (winning_txid, winning_nout)

        log.info("%i pending changes", len(changes))

        for name, (txid, nout) in changes.iteritems():
            if name not in known_winning_dict:
                yield self._run_operation("INSERT OR REPLACE INTO claimtrie(claim_name, claim_id) SELECT claim_name, id FROM claims WHERE txid=? AND nout=?",
                    (txid, nout))
                update_cnt += 1

            elif known_winning_dict[name] != changes[name]:
                log.info("update %s %s:%i --> %s:%i ", name, known_winning_dict[name][0], known_winning_dict[name][1], node['txid'], int(node['n']))
                yield self._run_operation("UPDATE claimtrie SET claim_id=(SELECT claim_id FROM claims WHERE txid=? AND nout=?) WHERE claim_name=?",
                    (txid, nout, name,))
                update_cnt += 1
            else:
                log.info("%s is up to date ", name)

        if update_cnt:
            log.info("%i updated winning claims", update_cnt)
        log.info("Finished updating claims in trie")

    @defer.inlineCallbacks
    def save_stream_size(self, sd_hash, stream_size):
        log.info("Save size (%i bytes) for stream %s", stream_size, sd_hash)
        yield self._run_operation("UPDATE stream_sizes SET stream_size=? WHERE claim_id=(SELECT claim_id FROM metadata WHERE sd_hash=?)", (stream_size, sd_hash))


class ClaimCache(object):
    def __init__(self, storage):
        self.storage = storage
        self._title = {}
        self._author = {}
        self._description = {}
        self._language = {}
        self._license = {}
        self._license_url = {}
        self._thumbnail = {}
        self._preview = {}
        self._content_type = {}
        self._nsfw = {}
        self._fee = {}
        self._sd_hash = {}
        self._claim_infos = {}

    @defer.inlineCallbacks
    def sync_cache(self):
        claim_dump = yield self.storage.get_claims()
        log.info("Sync %i claims in cache", len(claim_dump))
        for claim_info in claim_dump:
            claim_id = claim_info['claim_id']
            self._claim_infos[claim_id] = claim_info
            metadata_tuple = claim_info['metadata']
            if metadata_tuple:
                serialized_fee = metadata_tuple[12]
                fee = None if not serialized_fee else Fee.deserialize(serialized_fee.decode('hex'))
                if not metadata_tuple[2] or len(metadata_tuple[2]) <= 4:
                    continue
                if not metadata_tuple[3] or len(metadata_tuple[3]) <= 4:
                    continue
                if not metadata_tuple[4] or len(metadata_tuple[4]) <= 4:
                    continue
                self._title[claim_id]        = metadata_tuple[2]
                self._author[claim_id]       = metadata_tuple[3]
                self._description[claim_id]  = metadata_tuple[4]
                self._language[claim_id]     = metadata_tuple[5]
                self._license[claim_id]      = metadata_tuple[6]
                self._license_url[claim_id]  = metadata_tuple[7]
                self._thumbnail[claim_id]    = metadata_tuple[8]
                self._preview[claim_id]      = metadata_tuple[9]
                self._content_type[claim_id] = metadata_tuple[10]
                self._nsfw[claim_id]         = bool(metadata_tuple[11])
                self._fee[claim_id]          = fee
                self._sd_hash[claim_id]      = metadata_tuple[13]
            else:
                if claim_id in self._title:
                    del self._title[claim_id]
                if claim_id in self._author:
                    del self._author[claim_id]
                if claim_id in self._description:
                    del self._description[claim_id]
                if claim_id in self._language:
                    del self._language[claim_id]
                if claim_id in self._license:
                    del self._license[claim_id]
                if claim_id in self._license_url:
                    del self._license_url[claim_id]
                if claim_id in self._thumbnail:
                    del self._thumbnail[claim_id]
                if claim_id in self._preview:
                    del self._preview[claim_id]
                if claim_id in self._content_type:
                    del self._content_type[claim_id]
                if claim_id in self._nsfw:
                    del self._nsfw[claim_id]
                if claim_id in self._fee:
                    del self._fee[claim_id]
                if claim_id in self._sd_hash:
                    del self._sd_hash[claim_id]

    def get_title(self):
        return self._title

    def get_author(self):
        return self._author

    def get_description(self):
        return self._description

    def get_language(self):
        return self._language

    def get_license(self):
        return self._license

    def get_license_url(self):
        return self._license_url

    def get_thumbnail(self):
        return self._thumbnail

    def get_preview(self):
        return self._preview

    def get_content_type(self):
        return self._content_type

    def get_nsfw(self):
        return self._nsfw

    def get_fee(self):
        return self._fee

    def get_sd_hash(self):
        return self._sd_hash

    def get_cached_claim(self, claim_id):
        return self._claim_infos.get(claim_id, None)


class LBRYcrdManager(object):
    def __init__(self, lbrycrdd_data_dir=None, lbrycrdd_path=None, conf_path=None):
        reactor.addSystemEventTrigger("before", "shutdown", self.stop)
        self.lbrycrdd_process = None
        self.started_lbrycrdd = False
        self.lbrycrdd_data_dir = None
        self.lbrycrdd_path = None
        self.lbrycrd_conf = None
        self.rpc_conn_string = None
        self.set_lbrycrd_attributes(lbrycrdd_data_dir, lbrycrdd_path, conf_path)
        self.storage = SQLiteStorage()
        self.claim_cache = ClaimCache(self.storage)
        self.claimtrie_updater = LoopingCall(self.sync_claimtrie)

    def set_lbrycrd_attributes(self, user_lbrycrdd_data_dir=None, user_lbrycrdd_path=None,
                               user_lbrycrd_conf=None):
        if sys.platform == "darwin":
            _lbrycrdd_data_dir = user_data_dir("lbrycrd")
        else:
            _lbrycrdd_data_dir = os.path.join(os.path.expanduser("~"), ".lbrycrd")
        _lbrycrdd_path = get_lbrycrdd_path(_lbrycrdd_data_dir)
        self.lbrycrdd_data_dir = user_lbrycrdd_data_dir or _lbrycrdd_data_dir
        self.lbrycrdd_path = user_lbrycrdd_path or _lbrycrdd_path
        self.lbrycrd_conf = user_lbrycrd_conf or os.path.join(self.lbrycrdd_data_dir, "lbrycrd.conf")
        self.rpc_conn_string = get_lbrycrdd_connection_string(self.lbrycrd_conf)

    def rpc_conn(self):
        return AuthServiceProxy(self.rpc_conn_string)

    @defer.inlineCallbacks
    def start(self):
        yield self.storage.start()
        yield threads.deferToThread(self._make_connection)
        yield self.sync_winning_claims()
        yield self.claim_cache.sync_cache()
        self.claimtrie_updater.start(600)

    def stop(self):
        if self.claimtrie_updater.running:
            self.claimtrie_updater.stop()
        if self.lbrycrdd_path is not None:
            return self._stop_daemon()

    def _make_connection(self):
        log.info("Connecting to lbrycrdd...")
        if self.lbrycrdd_path is not None:
            self._start_daemon()
        self._get_info_rpc()
        log.info("Connected to lbrycrdd!")

    def _start_lbrycrdd(self):
        start_lbrycrdd_command = [
            self.lbrycrdd_path,
            "-datadir=%s" % self.lbrycrdd_data_dir,
            "-conf=%s" % self.lbrycrd_conf
        ]

        if USE_TXINDEX:
            start_lbrycrdd_command.append("-txindex")

        if VERBOSE_LBRYCRDD:
            start_lbrycrdd_command.append("-printtoconsole")

        self.lbrycrdd_process = subprocess.Popen(start_lbrycrdd_command)

        self.started_lbrycrdd = True

    def _start_daemon(self):
        tries = 0
        try:
            try:
                self.rpc_conn().getinfo()
            except ValueError:
                log.exception(
                    'Failed to get rpc info. Rethrowing with a hopefully more useful error message')
                raise Exception('Failed to get rpc info from lbrycrdd.  Try restarting lbrycrdd')
            log.info("lbrycrdd was already running when LBRYcrdManager was started.")
            return
        except (socket.error, JSONRPCException):
            tries += 1
            log.info(
                "lbrcyrdd was not running when LBRYcrdManager was started. Attempting to start it.")
        try:
            self._start_lbrycrdd()
        except OSError:
            import traceback
            log.error(
                "Couldn't launch lbrycrdd at path %s: %s",
                self.lbrycrdd_path, traceback.format_exc())
            raise ValueError("Couldn't launch lbrycrdd. Tried %s" % self.lbrycrdd_path)

        while tries < 6:
            try:
                self.rpc_conn().getinfo()
                break
            except (socket.error, JSONRPCException):
                tries += 1
                if tries > 2:
                    log.warning("Failed to connect to lbrycrdd.")

                if tries < 6:
                    time.sleep(2 ** tries)
                    if tries > 2:
                        log.warning("Trying again in %d seconds", 2 ** tries)
                else:
                    log.warning("Giving up.")
        else:
            self.lbrycrdd_process.terminate()
            raise ValueError("Couldn't open lbrycrdd")

    def _stop_daemon(self):
        if self.lbrycrdd_process is not None and self.started_lbrycrdd is True:
            log.info("Stopping lbrycrdd...")
            d = threads.deferToThread(self._stop_rpc)
            d.addCallback(lambda _: log.info("Stopped lbrycrdd."))
            return d
        return defer.succeed(True)

    def get_blockchain_height(self):
        d = defer.succeed(self._get_info_rpc())
        d.addCallback(lambda r: r.get('blocks'))
        return d

    def get_claims_for_name(self, name):
        return threads.deferToThread(self._get_claims_for_name_rpc, name)

    def get_value_for_name(self, name):
        return threads.deferToThread(self._get_value_for_name_rpc, name)

    def get_claims_in_trie(self):
        # all claims in trie
        return threads.deferToThread(self._get_claims_in_trie_rpc)

    @defer.inlineCallbacks
    def sync_winning_claims(self):
        claimtrie = yield threads.deferToThread(self._get_claimtrie_rpc)
        yield self.storage.update_claimtrie(claimtrie)

    def get_transaction(self, txid):
        return defer.succeed(self.rpc_conn().getrawtransaction(txid, 1))

    @defer.inlineCallbacks
    def get_claim_outpoint_address(self, txid, nout):
        tx = yield self.get_transaction(txid)
        script_pubkey = None

        for output in tx['vout']:
            if output['n'] == nout:
                script_pubkey = output['scriptPubKey']['hex']
        if not script_pubkey:
            defer.returnValue(None)
        address = deserialize.get_address_from_output_script(script_pubkey.decode('hex'))
        defer.returnValue(address)

    @defer.inlineCallbacks
    def get_all_claims(self):
        results = yield self.get_claims_in_trie()
        claimtrie = {}
        for item in results:
            claimtrie[item['name']] = item['claims']
        defer.returnValue(claimtrie)

    @defer.inlineCallbacks
    def validate_and_register_claim_to_channel(self, claim, claim_id, txid, nout, name):
        claim_address = yield self.get_claim_outpoint_address(txid, nout)
        certificate_info = yield self.storage.get_claim(claim.certificate_id)
        certificate = certificate_info['claim']
        try:
            validated = yield claim.validate_signature(claim_address, certificate)
            if validated:
                yield self.storage.register_claim_to_channel(claim_id, certificate_info['claim_id'])
                log.info("validated signature for lbry://%s#%s/%s", certificate_info['name'],
                         certificate_info['claim_id'], name)
        except:
            pass

    @defer.inlineCallbacks
    def sync_claimtrie(self):
        claimtrie = yield self.get_all_claims()
        alledgedly_signed = []
        total_claims = sum([len(v) for k, v in claimtrie.iteritems()])
        imported_names = 0
        log.info("checking %i claims", total_claims)
        for name, claims_for_name in claimtrie.iteritems():
            try:
                uri.parse_lbry_uri(name)
                for claim in claims_for_name:
                    try:
                        val = "".join(chr(x) for x in [ord(c) for c in claim['value']])
                        txid, nout, claim_id = claim['txid'], claim['n'], claim['claimId']
                        decoded = smart_decode(val)
                        claim_is_known = yield self.storage.claim_outpoint_is_known(claim_id,
                                                                                    txid, nout)
                        if claim_is_known:
                            pass
                        else:
                            claim_address = yield self.get_claim_outpoint_address(txid, nout)
                            yield self.storage.add_claim(claim_id, name, decoded, txid, nout,
                                                         claim_address)
                            imported_names += 1
                            if decoded.has_signature:
                                alledgedly_signed.append((decoded, claim_id, txid, nout, name))
                    except DecodeError:
                        pass
            except URIParseError:
                pass

        log.info("%i total claims known, finished importing %i, need to check %i signatures",
                 total_claims, imported_names, len(alledgedly_signed))
        for decoded, claim_id, txid, nout, name in alledgedly_signed:
            yield self.validate_and_register_claim_to_channel(decoded, claim_id, txid, nout, name)
        yield self.sync_winning_claims()
        yield self.claim_cache.sync_cache()

    @_catch_connection_error
    def _get_claimtrie_rpc(self):
        return self.rpc_conn().getclaimtrie()

    @_catch_connection_error
    def _get_claims_in_trie_rpc(self):
        return self.rpc_conn().getclaimsintrie()

    @_catch_connection_error
    def _get_claims_for_name_rpc(self, name):
        return self.rpc_conn().getclaimsforname(name)

    @_catch_connection_error
    def _get_value_for_name_rpc(self, name):
        return self.rpc_conn().getvalueforname(name)

    @_catch_connection_error
    def _get_info_rpc(self):
        return self.rpc_conn().getinfo()

    @_catch_connection_error
    def _get_balance(self):
        return self.rpc_conn().getbalance()

    @_catch_connection_error
    def _stop_rpc(self):
        # check if our lbrycrdd is actually running, or if we connected to one that was already
        # running and ours failed to start
        if self.lbrycrdd_process.poll() is None:
            self.rpc_conn().stop()
            self.lbrycrdd_process.wait()
