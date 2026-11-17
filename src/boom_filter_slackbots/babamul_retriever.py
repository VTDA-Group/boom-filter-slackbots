"""Candidates for the antares-filter-slackbots ranker, read from the local Babamul cache.

The cache is filled daily by babamul_ingest.py (villrv/anomaly-pipelines), which reads
BOOM's public LSST Kafka stream and keeps objects passing the old Orcus "Superphot lsst"
filter cut. This retriever replaces only the candidate-retrieval step of
LSSTOrcusRetriever (Orcus candidates + ANTARES light curves); host matching, the
Superphot+ fit, ranking and Slack posting are Kaylee's code, unchanged.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.time import Time

from antares_filter_slackbots.retrievers import LSSTRetriever

DEFAULT_CACHE = Path("/n/lab_storage/avillar_lab/Lab/Orcus/babamul_cache")
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
ARCSEC = 1.0 / 3600.0


def _nearest(cm: dict, catalog: str) -> dict | None:
    rows = cm.get(catalog) or []
    if isinstance(rows, dict):
        rows = [rows]
    rows = [r for r in rows if r.get("distance_arcsec") is not None]
    return min(rows, key=lambda r: r["distance_arcsec"]) if rows else None


def star_catalog_check(cm: dict) -> bool:
    """LSSTRetriever.star_catalog_check on Babamul cross-matches. False = reject.

    Kept: Gaia DR3 source with a measured parallax (ANTARES matches within ~1"), or any
    Gaia source within 0.2". Not portable: the bright-guide-star and SDSS-galaxy
    rules (no such catalogs in Babamul) and allwise_cut (Babamul's AllWISE rows lack
    cc_flags/ext_flg/var_flg). The ingest's filter-6 cut has already removed stars
    (Gaia, LSPSC, 2MASS) and QSOs (Milliquas, WISE colour), so this is a second pass.
    """
    gaia = _nearest(cm, "Gaia_DR3")
    if gaia is not None and gaia["distance_arcsec"] <= 1.0:
        plx = gaia.get("parallax")
        if plx is not None and not (isinstance(plx, float) and math.isnan(plx)):
            return False
        if gaia["distance_arcsec"] < 0.2:
            return False
    return True


def bogus_check(lc: pd.DataFrame) -> str | None:
    """LSSTRetriever.filter_bogus_alerts on the cached detections. None = pass.

    The per-detection flag count is not portable (the cache keeps flags only for the
    newest alert, which the ingest already required to be clean).
    """
    pos = lc.dropna(subset=["ra", "dec"])
    if len(pos) > 1 and (pos.ra.std() > 0.5 * ARCSEC or pos.dec.std() > 0.5 * ARCSEC):
        return "bad localization"
    if len(lc) < 5:
        return "too few real"
    if lc.mag.max() - lc.mag.min() < 0.5:
        return "real mag ptp cut"
    if lc.mjd.max() - lc.mjd.min() < 3:
        return "real mjd cut"
    if lc.mjd.round().nunique() < 3:
        return "fewer than 3 nights"
    return None


class BabamulCacheRetriever(LSSTRetriever):
    """Retriever for RankingFilter/ANTARESRanker backed by the Babamul cache."""

    def __init__(self, lookback_days: float = 7.0, cache_dir: Path | str = DEFAULT_CACHE,
                 max_age_days: float = 200.0):
        # Skip LSSTRetriever.__init__ (it pings NOIRLab); ANTARESRetriever's sets up the
        # Alerce/TNS helpers the shared code expects.
        super(LSSTRetriever, self).__init__(lookback_days)
        self.cache = Path(cache_dir)
        self.max_age_days = max_age_days       # sp_orcus pre-filter: first seen < 200 d ago
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        # Host-match cache written by ANTARESRanker.add_host_galaxy_info; keep it here,
        # not in Kaylee's checkout.
        self._prost_path = str(DATA_DIR / "PROST_BABAMUL.csv")
        self.counts: dict[str, int] = {}

    def quality_check(self, ts):  # same as LSSTOrcusRetriever: done upstream + bogus_check
        return True

    def _crossmatches(self, ids: set[str]) -> dict[str, dict]:
        out = {}
        with open(self.cache / "cross_matches.jsonl") as fh:
            for line in fh:
                d = json.loads(line)
                if d["objectId"] in ids:
                    out[d["objectId"]] = d.get("cm") or {}
        return out

    def _lightcurve(self, oid: str) -> pd.DataFrame | None:
        path = self.cache / "lightcurves" / f"{oid}.csv"
        if not path.exists():
            return None
        lc = pd.read_csv(path)
        lc = lc[(lc["kind"] == "alert") & lc["mag"].notna() & lc["magerr"].notna()]
        for col in ("ra", "dec"):          # caches written before per-detection positions
            if col not in lc.columns:
                lc[col] = np.nan
        return lc.sort_values("mjd")

    def candidates(self) -> pd.DataFrame:
        passed = pd.read_csv(self.cache / "passed.csv", dtype={"objectId": str, "ztf_id": str},
                             low_memory=False)
        passed["passed_at"] = pd.to_datetime(passed["passed_at"], utc=True)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=self._lookback)
        passed = passed[passed["passed_at"] >= cutoff]
        return passed.sort_values("jd").drop_duplicates("objectId", keep="last")

    def retrieve_candidates(self, filt, max_num):
        """Same contract as LSSTOrcusRetriever.retrieve_candidates: (locus_df, ts_dict)."""
        print(f"Running Babamul retriever for filter {filt.name} (cache {self.cache})")
        filt.setup()
        self.check_watch()
        passed = self.candidates()
        xm = self._crossmatches(set(passed.objectId))
        counts = {"candidates": len(passed), "no_lightcurve": 0, "too_old": 0,
                  "too_few_or_many": 0, "star": 0, "bogus": 0}
        ts_dict, rows = {}, []
        for r in passed.itertuples():
            lc = self._lightcurve(r.objectId)
            if lc is None or lc.empty:
                counts["no_lightcurve"] += 1
                continue
            if self._mjd - lc.mjd.min() > self.max_age_days:
                counts["too_old"] += 1
                continue
            if not 5 <= len(lc) <= 2000:                  # sp_orcus num_mag_values
                counts["too_few_or_many"] += 1
                continue
            cm = xm.get(r.objectId, {})
            if not star_catalog_check(cm):
                counts["star"] += 1
                continue
            if lc["ra"].isna().all():                   # backfill: object position only
                lc["ra"], lc["dec"] = r.ra, r.dec
            if bogus_check(lc) is not None:
                counts["bogus"] += 1
                continue

            tns = _nearest(cm, "TNS")
            if tns is not None and tns["distance_arcsec"] <= 2.0:
                tns_name = f"{tns.get('name_prefix') or ''}{tns.get('name')}".strip() or "---"
                tns_cls = tns.get("type") or "---"
                tns_z = tns.get("redshift")
                tns_z = float(tns_z) if tns_z not in (None, "") else np.nan
            else:
                tns_name, tns_cls, tns_z = "---", "---", np.nan
            brightest = lc.loc[lc.mag.idxmin()]
            ztf_id = r.ztf_id if isinstance(r.ztf_id, str) and r.ztf_id not in ("", "nan") else None
            rows.append({
                "name": r.objectId, "ra": r.ra, "dec": r.dec,
                "nuclear": None,
                "tns_name": tns_name, "tns_class": tns_cls, "tns_redshift": tns_z,
                "brightest_alert_magnitude": float(brightest.mag),
                "peak_phase": self._mjd - float(brightest.mjd),
                "ztf_object_id": ztf_id,
            })
            ts_dict[r.objectId] = pd.DataFrame({
                "ant_mjd": lc.mjd.to_numpy(), "ant_mag": lc.mag.to_numpy(),
                "ant_magerr": lc.magerr.to_numpy(), "ant_passband": lc.band.to_numpy(),
                "ant_ra": lc.ra.to_numpy(), "ant_dec": lc.dec.to_numpy(),
            })
        counts["kept"] = len(rows)
        self.counts = counts
        print("Babamul candidates:", ", ".join(f"{k} {v}" for k, v in counts.items()))
        if not rows:
            return None, None
        return pd.DataFrame.from_records(rows), ts_dict
