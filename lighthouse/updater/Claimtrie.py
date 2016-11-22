import json
import logging.handlers
from decimal import Decimal
from jsonschema.exceptions import ValidationError
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from lbrynet.metadata.Metadata import Metadata, verify_name_characters
from lighthouse.updater.Metadata import MetadataManager

log = logging.getLogger(__name__)
COIN = Decimal(10 ** 8)


class UnknownClaimTypeError(Exception):
    pass


class Support(object):
    def __init__(self, txid, nout, amount, uri, claim_id, height, in_support_map):
        self.txid = txid
        self.nout = nout
        self.amount = amount
        self.uri = uri
        self.claim_id = claim_id
        self.height = height
        self.in_support_map = in_support_map


class Claim(object):
    def __init__(self, txid, nout, amount, uri, data, claim_id, height, in_claim_trie, is_controlling, sd_hash):
        self.txid = txid
        self.nout = nout
        self.amount = amount
        self.uri = uri
        self.claim_data = data
        self.claim_id = claim_id
        self.height = height
        self.in_claimtrie = in_claim_trie
        self.is_controlling = is_controlling
        self.sd_hash = sd_hash


def convert_amount_to_deweys(amount):
    return int(Decimal(amount) * COIN)


def convert_deweys_to_lbc(deweys):
    return Decimal(deweys) / COIN


def get_amount_from_raw_tx(transaction, nout):
    for tx in transaction['vout']:
        if tx['n'] == nout:
            return convert_amount_to_deweys(tx['value'])


def parse_claim_tx(claims, txid, current_height, raw_tx):
    for claim in claims:
        nout = claim['nOut']
        uri = claim['name']
        height = current_height - claim['depth']
        amount = get_amount_from_raw_tx(raw_tx, nout)
        if "in support map" in claim:
            claim_id = claim['supported claimId']
            in_support_map = claim['in support map']
            yield Support(txid, nout, amount, uri, claim_id, height, in_support_map)
        elif "in claim trie" in claim:
            claim_id = claim['claimId']
            claim_data = claim['value']
            in_claim_trie = claim['in claim trie']
            is_controlling = claim['is controlling']

            try:
                m = json.loads(claim_data)
                meta = Metadata(m)
                sd_hash = meta['sources']['lbry_sd_hash']
            except Exception as err:
                sd_hash = None

            yield Claim(txid, nout, amount, uri, claim_data, claim_id, height, in_claim_trie, is_controlling, sd_hash)
        else:
            log.error("Unknown claim type %s", str(claim))
            continue


class ClaimManager(object):
    def __init__(self, db, blockchain_manager, availability_manager):
        self.db = db
        self.blockchain_manager = blockchain_manager
        self.availability_manager = availability_manager
        self.metadata_manager = MetadataManager(db)
        self._last_checked_height = None

    def _update_claim_db(self, claim_id, uri, claim_data, txid, nout, height, amount, in_claim_trie):
        d = self.db.runQuery("insert or replace into claims values (?, ?, ?, ?, ?, ?, ?, ?)",
                                (claim_id, uri, claim_data, txid, nout, height, amount, in_claim_trie))
        return d

    def _update_support_db(self, txid, nout, claim_id, height, amount, in_support_map):
        d = self.db.runQuery("insert or replace into supports values (?, ?, ?, ?, ?, ?)",
                                                 (txid, nout, claim_id, height, amount, in_support_map))
        return d

    def _update_claimtrie_db(self, uri, claim_id, txid, nout):
        d = self.db.runQuery("insert or replace into claimtrie values (?, ?, ?, ?)", (uri, claim_id, txid, nout))
        return d

    def _update_stream_size_db(self, claim_id, sd_hash, total_bytes):
        d = self.db.runQuery("insert or replace into stream_size values (?, ?, ?)", (claim_id, sd_hash, total_bytes))
        return d

    def _add_claim_to_skipped(self, txid, nout):
        d = self.db.runQuery("insert or replace into skipped_claims values (?, ?)", (txid, nout))
        return d

    def _unskip_claim(self, txid, nout):
        d = self.db.runQuery("delete from skipped_claims where txid=? and nout=?", (txid, nout))
        return d

    def _claim_is_skipped(self, txid, nout):
        d = self.db.runQuery("select * from skipped_claims where txid=? and nout=?", (txid, nout))
        d.addCallback(lambda r: False if not r else True)
        return d

    def _claim_id_for_tx(self, txid, nout):
        d = self.db.runQuery("select claim_id from claims where txid=? and nout=? and uri=?", (txid, nout))
        return d

    def _get_winning_tx_for_name(self, name):
        d = self.db.runQuery("select txid, nout from claimtrie where uri=?", (name,))
        d.addCallback(lambda r: False if not r else r[0])
        return d

    def _update_claimtrie(self, name, claim_id, txid, nout):
        d = self._update_claimtrie_db(name, claim_id, txid, nout)
        return d

    def _update_claim(self, claim):
        d = self._update_claim_db(claim.claim_id, claim.uri, claim.claim_data, claim.txid,
                                  claim.nout, claim.height, claim.amount, claim.in_claimtrie)
        d.addCallback(lambda _: self._claim_is_skipped(claim.txid, claim.nout))
        d.addCallback(lambda skipped: None if skipped else self._update_metadata(claim))
        return d

    def _update_metadata(self, claim):
        try:
            metadata = json.loads(claim.claim_data)
            if not isinstance(metadata, dict):
                raise ValueError(type(metadata))
            d = self.metadata_manager.update_metadata(claim.claim_id, metadata)
            d.addCallback(lambda _: self.availability_manager.update_availability(claim.claim_id, claim.claim_id))
            log.debug("Update winning metadata for %s", claim.claim_id)
        except (ValueError, ValidationError):
            log.debug("Skipping non-conforming %s... (nout %i) claim for lbry://%s", claim.txid[:8], claim.nout,
                        claim.uri)
            d = self._add_claim_to_skipped(claim.txid, claim.nout)
        return d

    def _update_support(self, support):
        d = self._update_support_db(support.txid, support.nout, support.claim_id, support.height,
                                    support.amount, support.in_support_map)
        return d

    def _save_claims_and_supports(self, claims):
        dl = []
        for claim in claims:
            if isinstance(claim, Claim):
                log.debug("Update winning claim for lbry://%s", claim.uri)
                d = self._update_claim(claim)
                if claim.is_controlling:
                    d.addCallback(lambda _: self._update_claimtrie(claim.uri, claim.claim_id, claim.txid, claim.nout))
            elif isinstance(claim, Support):
                log.debug("Add support for lbry://%s", claim.uri)
                d = self._update_support(claim)
            else:
                log.warning("Unknown claim type")
                raise UnknownClaimTypeError(type(claim))
            dl.append(d)
        return defer.DeferredList(dl)

    def _should_update_claims(self, claims, txid, current_height):
        if claims:
            d = self.blockchain_manager.get_transaction(txid)
            d.addCallback(lambda raw_transaction: list(parse_claim_tx(claims, txid, current_height, raw_transaction)))
            d.addCallback(lambda claim_objs: self._save_claims_and_supports(claim_objs))
            return d
        return False

    def _update_claims_from_tx(self, txid, current_height):
        d = self.blockchain_manager.get_claims_from_tx(txid)
        d.addCallback(self._should_update_claims, txid, current_height)
        return d

    def update_claims_from_txid(self, txid):
        d = self.blockchain_manager.get_blockchain_height()
        d.addCallback(lambda h: self._update_claims_from_tx(txid, h))
        return d

    def _handle_claimtrie_response(self, claimtrie):
        def _update_if_needed(is_needed, txid):
            if is_needed:
                return self.update_claims_from_txid(txid)

        def _check_tx(old_tx, new_txid, new_nout):
            if not old_tx:
                return True
            if old_tx[0] != new_txid or old_tx[1] != new_nout:
                return True
            return False

        for name in claimtrie:
            txid, nout = claimtrie[name]
            try:
                verify_name_characters(name)
            except Exception:
                d = self._add_claim_to_skipped(txid, nout)
            else:
                d = self._get_winning_tx_for_name(name)
                d.addCallback(_check_tx, txid, nout)
                d.addCallback(_update_if_needed, txid)

    def update(self):
        d = self.blockchain_manager.get_nametrie()
        d.addCallback(lambda claimtrie: self._handle_claimtrie_response(claimtrie))
        return d

    def get_claimed_names(self):
        def _clean_results(results):
            results_for_return = []
            for (name, ) in results:
                try:
                    verify_name_characters(name)
                    results_for_return.append(name)
                except AssertionError:
                    pass
            return results_for_return

        d = self.db.runQuery("select uri from claimtrie")
        d.addCallback(_clean_results)
        return d