"""Daily Superphot+ on Babamul (BOOM) LSST candidates.

Same filter settings as sp_orcus in antares-filter-slackbots/run.py, with candidates
from the local Babamul cache instead of Orcus + ANTARES. Host matching, the fit,
ranking, Slack posting and the vote web page are the shared antares-filter-slackbots code.

    python -m boom_filter_slackbots.run --dry-run      # rank and save a CSV; no Slack
    python -m boom_filter_slackbots.run                # post to --channel
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from astropy.time import Time

from antares_filter_slackbots.antares_ranker import ANTARESRanker, RankingFilter
from antares_filter_slackbots.filters import SuperphotPlusLSST
from antares_filter_slackbots.slack_formatters import SlackPoster
from antares_filter_slackbots.webpage import create_html_tab

from boom_filter_slackbots.babamul_retriever import DATA_DIR, DEFAULT_CACHE, BabamulCacheRetriever

FILTER_NAME = "babamul_superphot"


def superphot_filter(retriever, channel: str) -> RankingFilter:
    current_time = Time.now().mjd
    filt = RankingFilter(
        FILTER_NAME,
        retriever,
        SuperphotPlusLSST(),
        channel,
        "superphot_plus_prob",
        pre_filter_properties={       # applied in BabamulCacheRetriever (age, num points)
            "oldest_alert_observation_time": (current_time - 200., 99_999_999,),
            "num_mag_values": (5, 2000),
        },
        save_properties=[
            "superphot_plus_class_without_redshift", "superphot_plus_prob_without_redshift",
            "superphot_plus_classifier", "superphot_plus_sampler", "superphot_non_Ia_prob",
        ],
        post_filter_tags=["superphot_plus_classified", ],
        post_filter_properties={
            "superphot_plus_valid": (1, 1), "superphot_non_Ia_prob": (0.8, 1.0)
        },
        groupby_properties={'superphot_plus_class': ('SN Ia', 'SN II', 'SN IIn', 'SN Ibc', 'SLSN-I',)},
    )
    # RankingFilter puts outputs/votes in the antares-filter-slackbots checkout; keep ours here.
    default_prefix = filt.save_prefix
    filt.save_prefix = str(DATA_DIR / FILTER_NAME)
    os.makedirs(filt.save_prefix, exist_ok=True)
    if Path(default_prefix).is_dir() and not any(Path(default_prefix).iterdir()):
        shutil.rmtree(default_prefix)
    return filt


def run(filt: RankingFilter, max_num: int, *, dry_run: bool) -> None:
    """ANTARESRanker.run, with a dry-run stop before anything is posted."""
    ranker = ANTARESRanker()

    def empty(stage: str) -> None:
        print(f"No candidates left after {stage}.")
        if not dry_run:
            SlackPoster(None, {}, filt.save_prefix).post_empty(filt.channel)

    df, ts_dict = filt.retriever.retrieve_candidates(filt, max_num)
    if df is None:
        return empty("Babamul retrieval")
    host_df = ranker.add_host_galaxy_info(df, ts_dict, filt.retriever)
    if host_df is None:
        return empty("host matching")
    final_df = ranker.apply_filter_to_df(host_df, ts_dict, filt)
    if final_df is None:
        return empty("Superphot+")

    df_pruned = ranker.rank(final_df, filt, max_num)
    df_pruned["posted_before"] = False
    filt_meta = {
        'max_num': max_num,
        'ranking_property': filt.ranked_property,
        'groupby': list(filt._group_props.keys())[0] if len(filt._group_props.keys()) > 0 else None,
    }
    for (k, vals) in filt._group_props.items():
        for v in vals:
            filt_meta[f'overflow_{v}'] = (len(final_df[final_df[k] == v]) < len(df_pruned[df_pruned[k] == v]))

    if dry_run:
        out = Path(filt.save_prefix) / "dry_run_ranked.csv"
        df_pruned.to_csv(out)
        print(f"[dry-run] {len(final_df)} classified, {len(df_pruned)} would be posted to "
              f"{filt.channel}; saved to {out}")
        return
    ranker.save_objects(df_pruned, filt.save_prefix)
    poster = SlackPoster(df_pruned, filt_meta, filt.save_prefix)
    poster.post(filt.channel)
    create_html_tab(df_pruned, poster)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--channel", default="#orcus-superphot-plus")
    p.add_argument("--lookback-days", type=float, default=7.0)
    p.add_argument("--cache-dir", default=str(DEFAULT_CACHE))
    p.add_argument("--max-num", type=int, default=1000)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    retriever = BabamulCacheRetriever(args.lookback_days, args.cache_dir)
    run(superphot_filter(retriever, args.channel), args.max_num, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
