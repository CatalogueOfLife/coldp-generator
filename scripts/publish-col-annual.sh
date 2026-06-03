#!/usr/bin/env bash
#
# publish-col-annual.sh
# ---------------------------------------------------------------------------
# Build every historical CoL Annual Checklist (colac, 2000 + 2002-2019; 2001 was
# never released) into a ColDP archive and publish the zips to the public
# annual-checklist download folder.
#
# Stages:
#   1. build the fat JAR (mvn package)                          [skip: --no-build]
#   2. run the colac generator for each year -> <SRC>/<year>/colac.zip
#   3. stage each colac.zip          -> <STAGE>/<year>_coldp.zip
#   4. scp the zips to the server and move them into place as the `col` user
#                                                               [skip: --no-upload]
#
# The colac generator reads the per-year MariaDB databases col<year>ac on the
# LOCAL host (see CLAUDE.md). Make sure MariaDB is running before stage 2.
#
# Usage:
#   scripts/publish-col-annual.sh [--no-build] [--no-upload] [--years "2005 2010"]
#
# Server settings (ssh host/user, remote dir) are intentionally NOT hardcoded in
# this public repo. Provide them in an untracked  scripts/publish-col-annual.local
# (copy publish-col-annual.local.example) or via the matching environment vars.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# ---- build / generate settings (override via env) -------------------------
YEARS="${YEARS:-2000 $(seq 2002 2019)}"   # 2000 + 2002-2019; 2001 was never released
SRC="${SRC:-/tmp/colac-archives}"          # <SRC>/<year>/colac.zip is produced here
STAGE="${STAGE:-/tmp/col-annual-upload}"   # renamed <year>_coldp.zip staged here
MVN_ARGS="${MVN_ARGS:--q -DskipTests}"
JAVA_OPTS="${JAVA_OPTS:-}"

# colac MariaDB connection (defaults match the generator's own defaults)
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-root}"
DB_PASS="${DB_PASS:-root}"

# ---- server settings (override locally; see header) -----------------------
SSH_USER="${SSH_USER:-}"
SSH_HOST="${SSH_HOST:-}"
REMOTE_DIR="${REMOTE_DIR:-}"               # public annual folder on the server
REMOTE_STAGE="${REMOTE_STAGE:-/tmp/col-annual-stage}"
RUN_AS="${RUN_AS:-col}"                     # write into the NFS dir as this user;
                                            # root would be squashed to nobody
# untracked local overrides keep infra specifics out of the repo
[ -f "$SCRIPT_DIR/publish-col-annual.local" ] && source "$SCRIPT_DIR/publish-col-annual.local"

# ---- flags ----------------------------------------------------------------
DO_BUILD=1; DO_UPLOAD=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)  DO_BUILD=0; shift ;;
    --no-upload) DO_UPLOAD=0; shift ;;
    --years)     YEARS="$2"; shift 2 ;;
    -h|--help)   awk 'NR>1{ if (/^set -euo/) exit; print }' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

shopt -s nullglob

# ---- 1 + 2. build the fat JAR and generate every year ---------------------
if [[ $DO_BUILD -eq 1 ]]; then
  echo "==> building fat JAR"
  mvn $MVN_ARGS -f "$REPO_DIR/pom.xml" package
  JAR=$(ls "$REPO_DIR"/target/coldp-generator-*.jar 2>/dev/null | grep -v original | head -1)
  [[ -n "$JAR" ]] || { echo "fat JAR not found in $REPO_DIR/target" >&2; exit 1; }
  echo "==> using $JAR"
  for y in $YEARS; do
    echo "==> generating colac $y -> $SRC/$y"
    rm -rf "$SRC/$y/colac" "$SRC/$y/colac.zip"
    java $JAVA_OPTS -jar "$JAR" -s colac --year "$y" -r "$SRC/$y" \
      --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS"
  done
fi

# ---- 3. stage: <year>/colac.zip -> <STAGE>/<year>_coldp.zip ---------------
# Only the selected $YEARS are staged/uploaded (so `--years 2005` publishes just
# 2005, even when older archives are still present in $SRC).
rm -rf "$STAGE"; mkdir -p "$STAGE"
for y in $YEARS; do
  f="$SRC/$y/colac.zip"
  if [[ -f "$f" ]]; then
    echo "stage: $f -> $STAGE/${y}_coldp.zip"
    cp -f "$f" "$STAGE/${y}_coldp.zip"
  else
    echo "skip: no archive for $y at $f"
  fi
done

files=("$STAGE"/[0-9][0-9][0-9][0-9]_coldp.zip)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "No <year>_coldp.zip in $STAGE — nothing to publish."
  exit 0
fi
printf 'prepared %d archive(s):\n' "${#files[@]}"
printf '  %s\n' "${files[@]##*/}"

if [[ $DO_UPLOAD -eq 0 ]]; then
  echo "==> --no-upload: archives staged in $STAGE, not uploaded."
  exit 0
fi

# ---- 4. upload to a world-readable staging dir, then move into place -------
: "${SSH_USER:?set SSH_USER (see scripts/publish-col-annual.local.example)}"
: "${SSH_HOST:?set SSH_HOST (see scripts/publish-col-annual.local.example)}"
: "${REMOTE_DIR:?set REMOTE_DIR (see scripts/publish-col-annual.local.example)}"

echo "==> uploading ${#files[@]} file(s) to ${SSH_USER}@${SSH_HOST}:${REMOTE_STAGE}"
ssh "${SSH_USER}@${SSH_HOST}" "rm -rf '${REMOTE_STAGE}' && mkdir -p '${REMOTE_STAGE}'"
scp "${files[@]}" "${SSH_USER}@${SSH_HOST}:${REMOTE_STAGE}/"

# copy into place AS col, replacing older copies. Writing as col (not root)
# avoids NFS root_squash and yields col:col ownership.
ssh -t "${SSH_USER}@${SSH_HOST}" "
    set -e
    chmod 755 '${REMOTE_STAGE}'
    chmod 644 ${REMOTE_STAGE}/*_coldp.zip
    sudo -u ${RUN_AS} cp -f ${REMOTE_STAGE}/*_coldp.zip ${REMOTE_DIR}/
    rm -rf '${REMOTE_STAGE}'
    echo '--- now in ${REMOTE_DIR}: ---'
    ls -l ${REMOTE_DIR}/*_coldp.zip
"
echo "Done."
