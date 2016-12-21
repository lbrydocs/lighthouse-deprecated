import sys
import logging
import subprocess
import socket
import time
import os
from appdirs import user_data_dir
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from twisted.internet import threads, defer
from lighthouse.conf import USE_TXINDEX, VERBOSE_LBRYCRDD

log = logging.getLogger(__name__)


def _catch_connection_error(f):
    def w(*args):
        try:
            return f(*args)
        except socket.error:
            raise ValueError(
                "Unable to connect to an lbrycrd server. Make sure an lbrycrd server "
                "is running and that this application can connect to it.")
    return w


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


class LBRYcrdManager(object):
    def __init__(self, lbrycrdd_data_dir=None, lbrycrdd_path=None, conf_path=None):
        self.lbrycrdd_process = None
        self.started_lbrycrdd = False
        self.lbrycrdd_data_dir = None
        self.lbrycrdd_path = None
        self.lbrycrd_conf = None
        self.rpc_conn_string = None
        self.set_lbrycrd_attributes(lbrycrdd_data_dir, lbrycrdd_path, conf_path)

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

    def start(self):
        return threads.deferToThread(self._make_connection)

    def stop(self):
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

    def get_most_recent_blocktime(self):
        def get_block_time(block):
            if 'time' in block:
                return block['time']
            else:
                raise ValueError("Could not get a block time")

        d = threads.deferToThread(self._get_best_blockhash_rpc)
        d.addCallback(lambda blockhash: threads.deferToThread(self._get_block_rpc, blockhash))
        d.addCallback(get_block_time)
        return d

    def get_blockchain_height(self):
        d = threads.deferToThread(self._get_info_rpc)
        d.addCallback(lambda r: r.get('blocks'))
        return d

    def get_block(self, height):
        d = threads.deferToThread(self._get_blockhash_rpc, height)
        d.addCallback(self._get_block_rpc)
        return d

    def get_best_blockhash(self):
        d = threads.deferToThread(self._get_blockchain_info_rpc)
        d.addCallback(lambda blockchain_info: blockchain_info['bestblockhash'])
        return d

    def get_nametrie(self):
        d = threads.deferToThread(self._get_nametrie_rpc)
        d.addCallback(lambda claimtrie: {claim.get("name"): (claim.get('txid'), claim.get('n'))
                                         for claim in claimtrie if claim.get("txid", False)})
        return d

    def get_transaction(self, txid):
        d = threads.deferToThread(self._get_raw_tx_rpc, txid)
        d.addCallback(self._get_decoded_tx_rpc)
        return d

    def get_claims_for_name(self, name):
        return threads.deferToThread(self._get_claims_for_name_rpc, name)

    def get_claims_from_tx(self, txid):
        return threads.deferToThread(self._get_claims_from_tx_rpc, txid)

    def get_value_for_name(self, name):
        return threads.deferToThread(self._get_value_for_name_rpc, name)

    @_catch_connection_error
    def _get_info_rpc(self):
        return self.rpc_conn().getinfo()

    @_catch_connection_error
    def _get_raw_tx_rpc(self, txid):
        return self.rpc_conn().getrawtransaction(txid)

    @_catch_connection_error
    def _get_decoded_tx_rpc(self, raw):
        return self.rpc_conn().decoderawtransaction(raw)

    @_catch_connection_error
    def _get_blockchain_info_rpc(self):
        return self.rpc_conn().getblockchaininfo()

    @_catch_connection_error
    def _get_block_rpc(self, blockhash):
        return self.rpc_conn().getblock(blockhash)

    @_catch_connection_error
    def _get_blockhash_rpc(self, height):
        return self.rpc_conn().getblockhash(height)

    @_catch_connection_error
    def _get_claims_from_tx_rpc(self, txid):
        return self.rpc_conn().getclaimsfortx(txid)

    @_catch_connection_error
    def _get_claims_for_name_rpc(self, name):
        return self.rpc_conn().getclaimsforname(name)

    @_catch_connection_error
    def _get_nametrie_rpc(self):
        return self.rpc_conn().getclaimtrie()

    @_catch_connection_error
    def _get_value_for_name_rpc(self, name):
        return self.rpc_conn().getvalueforname(name)

    @_catch_connection_error
    def _get_best_blockhash_rpc(self):
        return self.rpc_conn().getbestblockhash()

    @_catch_connection_error
    def _stop_rpc(self):
        # check if our lbrycrdd is actually running, or if we connected to one that was already
        # running and ours failed to start
        if self.lbrycrdd_process.poll() is None:
            self.rpc_conn().stop()
            self.lbrycrdd_process.wait()
