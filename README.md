# boom-filter-slackbots

Slack bots for transient filters that run on [BOOM](https://github.com/boom-astro/boom)'s
public LSST stream (Babamul) instead of Orcus or ANTARES.

## Superphot+ (`boom_filter_slackbots.run`)

The Babamul version of the `sp_orcus` filter in
[antares-filter-slackbots](https://github.com/VTDA-Group/antares-filter-slackbots).
Only candidate retrieval is new; everything after it is that repo's code, unchanged.

1. **Candidates**: `BabamulCacheRetriever` reads the local Babamul cache
   (`/n/lab_storage/avillar_lab/Lab/Orcus/babamul_cache`). The cache is filled daily by
   `babamul_ingest.py` in villrv/anomaly-pipelines, which applies the old Orcus
   "Superphot lsst" filter cut to the stream. Objects that passed in the last 7 days are then
   checked with ports of `LSSTRetriever`'s checks:
   - **star/catalog**: Gaia DR3 parallax, or a Gaia source within 0.2", using Babamul's cross-matches;
   - **bogus**: position scatter ≤ 0.5", ≥ 5 detections, ≥ 0.5 mag range, ≥ 3 days and ≥ 3 nights;
   - **pre-filter**: first detection < 200 days ago.

   TNS name, class and redshift come from Babamul's TNS cross-match.
2. **Host galaxy, Superphot+ fit, ranking, Slack post, vote page**: `ANTARESRanker` and
   `SuperphotPlusLSST` from antares-filter-slackbots, with the same settings as `sp_orcus`
   (posts objects with P(non-Ia) ≥ 0.8, grouped by class).

**Not ported**, because Babamul doesn't carry the inputs:
- the bright-guide-star and SDSS-galaxy rules;
- `allwise_cut`;
- the per-detection flag count in `filter_bogus_alerts`.

The ingest's filter-6 cut already rejects stars and QSOs.

Outputs (`loci.csv`, vote history, the host-match cache `PROST_BABAMUL.csv`) go to `data/` in this
repo, not to the antares-filter-slackbots checkout.

### Running on Cannon

```bash
BOOM_SP_DRY_RUN=1 deploy/run_daily.sh    # rank and save data/babamul_superphot/dry_run_ranked.csv
deploy/run_daily.sh                      # post to #orcus-superphot-plus
```

It uses the antares-filter-slackbots environment (`conda_envs/antares_env_backup`) with that repo
installed, which supplies the Slack/YSE-PZ/TNS credentials (`auth.py`).
