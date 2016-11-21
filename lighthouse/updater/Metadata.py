import logging.handlers

from twisted.internet import defer
from twisted.python.failure import Failure

from lbrynet.metadata.Metadata import Metadata
from lbrynet.metadata.Fee import FeeValidator

log = logging.getLogger(__name__)


class MetadataManager(object):
    def __init__(self, db):
        self._db = db
        self.cache = {}

    def _update_fee_db(self, claim_id, currency, address, amount):
        d = self._db.runQuery("insert or replace into fee values (?, ?, ?, ?)",
                             (claim_id, currency, address, amount))
        d.addErrback(log.info)

        return d

    def _update_metadata_db(self, claim_id, title, author, description, language, license, nsfw, thumbnail, preview,
                            content_type, sd_hash, license_url, ver):
        d = self._db.runQuery("insert or replace into metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                             (claim_id, title, author, description, language, license, nsfw,
                              thumbnail, preview, content_type, sd_hash, license_url, ver))
        d.addErrback(lambda err: log.warning("update metadata db failed"))
        return d

    def update_metadata(self, claim_id, metadata):
        try:
            meta = Metadata(metadata, migrate=True)
        except AssertionError:
            return defer.succeed(False)
        ver = meta.get('ver', '0.0.1')
        title = meta.get('title', None)
        author = meta.get('author', None)
        description = meta.get('description', None)
        language = meta.get('language', None)
        license = meta.get('license', None)
        license_url = meta.get('license_url', None)
        nsfw = meta.get('nsfw', None)
        thumbnail = meta.get('thumbnail', None)
        preview = meta.get('preview', None)
        content_type = meta.get('content_type', None)
        fee = meta.get('fee', None)
        sources = meta.get('sources', None)
        sd_hash = None
        if sources:
            sd_hash = sources.get('lbry_sd_hash')
        if fee:
            try:
                validated_fee = FeeValidator(fee)
                d = self._update_fee_db(claim_id, validated_fee.currency_symbol,
                                        validated_fee.address, validated_fee.amount)
            except Exception:
                d = defer.succeed(None)
        else:
            d = defer.succeed(None)
        d.addCallback(lambda _: self._update_metadata_db(claim_id, title, author,
                                                         description, language, license,
                                                         nsfw, thumbnail, preview,
                                                         content_type, sd_hash, license_url, ver))
        return d

    def _get_validated_fee(self, currency_symbol, address, amount):
        r = {
            currency_symbol: {
                "address": address,
                "amount": amount
            }
        }
        return FeeValidator(r)

    def _add_fee_and_get_final_metadata(self, claim_id, title, author, description, language, license, nsfw,
                                        thumbnail, preview, content_type, sd_hash, license_url, ver):
        d = self._db.runQuery("select currency, address, amount from fee where claim_id=?", (claim_id,))
        d.addCallback(lambda fee: None if not fee else self._get_validated_fee(*fee[0]))
        d.addCallback(lambda fee: self._get_validated_metadata(fee, claim_id, title, author, description, language,
                                                               license, nsfw, thumbnail, preview, content_type, sd_hash,
                                                               license_url, ver))
        return d

    def _get_validated_metadata(self, fee, claim_id, title, author, description, language, license, nsfw,
                                thumbnail, preview, content_type, sd_hash, license_url, ver):
        r = {}
        if ver is not None:
            r.update({"ver": ver})
        if title is not None:
            r.update({"title": title})
        if author is not None:
            r.update({"author": author})
        if description is not None:
            r.update({"description": description})
        if language is not None:
            r.update({"language": language})
        if license is not None:
            r.update({"license": license})
        if license_url is not None:
            r.update({"license_url": license_url})
        if nsfw is not None:
            r.update({"nsfw": True if nsfw else False})
        if thumbnail is not None:
            r.update({"thumbnail": thumbnail})
        if preview is not None:
            r.update({"preview": preview})
        if content_type is not None:
            r.update({"content_type": content_type})
        r.update({"sources": {"lbry_sd_hash": sd_hash}})
        if fee is not None and not isinstance(fee, Failure):
            r.update({'fee': fee})
        m = Metadata(r)
        return m

    def _get_metadata_for_claim(self, claim_id):
        d = self._db.runQuery("select * from metadata where claim_id=?", (claim_id, ))
        d.addCallbacks(lambda r: False if not r else self._add_fee_and_get_final_metadata(*r[0]))
        return d

    def get_winning_metadata(self, name):
        d = self._db.runQuery("select claim_id from claimtrie where uri=?", (name, ))
        d.addCallback(lambda (r, ): False if not r else r[0])
        d.addCallback(lambda claim_id: False if not claim_id else self._get_metadata_for_claim(claim_id))
        return d
