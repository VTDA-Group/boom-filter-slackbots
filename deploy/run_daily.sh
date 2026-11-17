#!/bin/bash
# Daily Superphot+ on Babamul (BOOM) LSST candidates -> Slack.
#
# Candidates come from the Babamul cache that the Orcus LAISS cron
# (villrv/anomaly-pipelines, orcus_laiss_ad_cron.sh, 08:00) refreshes each morning,
# so schedule this after it. Uses antares-filter-slackbots' environment and code
# for host matching, the Superphot+ fit, ranking and posting.
#
#   BOOM_SP_CHANNEL   Slack channel (default #orcus-superphot-plus)
#   BOOM_SP_DRY_RUN=1 rank and save a CSV, post nothing

set -euo pipefail

REPO="/n/lab_storage/avillar_lab/Lab/boom-filter-slackbots"
ENV="/n/holylabs/LABS/avillar_lab/Lab/conda_envs/antares_env_backup"   # Kaylee's SP+ env
LOG_DIR="/n/netscratch/avillar_lab/Lab/boom-filter-slackbots/logs"     # disposable
STAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "$LOG_DIR"
exec > >(tee -a "${LOG_DIR}/superphot_${STAMP}.log") 2>&1
echo "=== Babamul Superphot+ daily ==="
echo "Started: $(date)  Host: $(hostname)"

module load gcc            # the env's ICU needs a newer libstdc++ than the system one
export PYTHONNOUSERSITE=1  # ~/.local typing_extensions breaks pydantic in this env
export PYTHONPATH="${REPO}/src"
cd "$REPO"
git pull --quiet || echo "git pull skipped/failed (continuing with local copy)"

ARGS=(--channel "${BOOM_SP_CHANNEL:-#orcus-superphot-plus}")
[[ "${BOOM_SP_DRY_RUN:-0}" == "1" ]] && ARGS+=(--dry-run)
"${ENV}/bin/python" -u -m boom_filter_slackbots.run "${ARGS[@]}"

echo "Finished: $(date)"
