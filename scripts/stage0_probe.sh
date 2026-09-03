#!/usr/bin/env bash
# Stage 0 live probe: create a managed (sbx microVM) session whose workspace
# is the mount sentinel, proving that
#   (a) POST /v1/sessions accepts host_type=managed + the sentinel workspace
#       (201, not a 422 from parse_repo_workspace), and
#   (b) the managed launch reaches the instrumented SbxLauncher.start_host,
#       which records the marker to /tmp/sbx-stage0-probe.log and fails loud.
#
# This is the seed of the eventual "create swarm-agent session" helper the
# coordinator will call. It talks ONLY to the local Omnigent HTTP API — no
# Omnigent source is touched.
#
# Prereqs: the omni-sbx server is running with the instrumented launcher
# installed, on OMNI_SERVER. A local single-user server needs no auth; set
# OMNI_TOKEN for an authenticated server.
#
# Usage:
#   scripts/stage0_probe.sh --list                 # list agents, pick an id
#   AGENT_ID=ag_xxx scripts/stage0_probe.sh        # fire the probe
#
# Env:
#   OMNI_SERVER   default http://localhost:6767
#   OMNI_TOKEN    optional bearer token (authenticated servers)
#   AGENT_ID      agent to bind (required unless --list)
#   WORKTREE      sentinel path (default /srv/worktrees/probe) — need not exist
#                 for Stage 0 (the probe stops before mounting)
#   MODE          rw | ro (default rw)
set -euo pipefail

OMNI_SERVER="${OMNI_SERVER:-http://localhost:6767}"
WORKTREE="${WORKTREE:-/srv/worktrees/probe}"
MODE="${MODE:-rw}"

auth=()
if [[ -n "${OMNI_TOKEN:-}" ]]; then
  auth=(-H "Authorization: Bearer ${OMNI_TOKEN}")
fi

if [[ "${1:-}" == "--list" ]]; then
  echo "Agents on ${OMNI_SERVER}:"
  if ! agents_json=$(curl -fsS "${auth[@]+"${auth[@]}"}" "${OMNI_SERVER}/v1/agents"); then
    echo "  (failed — check OMNI_SERVER / OMNI_TOKEN)" >&2
    exit 1
  fi
  # Prefer a compact id/name/description table via jq; the payload may be a
  # bare array or wrapped in .data/.agents, and the id field is .id or
  # .agent_id — cope with all. Fall back to raw JSON when jq is absent.
  if command -v jq >/dev/null 2>&1; then
    printf '%s\n' "${agents_json}" \
      | jq -r '(.data // .agents // .) as $a
               | (if ($a|type)=="array" then $a else [$a] end)[]
               | [(.agent_id // .id), .name, (.description // "")] | @tsv' \
      | column -t -s "$(printf '\t')"
  else
    echo "  (jq not found — showing raw JSON; install jq for a table)"
    printf '%s\n' "${agents_json}"
  fi
  echo
  echo "Pick an agent_id above, then: AGENT_ID=<id> $0"
  exit 0
fi

if [[ -z "${AGENT_ID:-}" ]]; then
  echo "AGENT_ID is required. Run '$0 --list' to find one." >&2
  exit 2
fi

WORKSPACE="git@sbxmount:${WORKTREE}#${MODE}"
echo "Creating managed session:"
echo "  server    ${OMNI_SERVER}"
echo "  agent_id  ${AGENT_ID}"
echo "  workspace ${WORKSPACE}"
echo

# No initial message: the managed provision starts on create regardless, so
# the launch reaches start_host without needing a first turn.
body=$(printf '{"agent_id":"%s","host_type":"managed","workspace":"%s"}' \
  "${AGENT_ID}" "${WORKSPACE}")

http_code=$(curl -sS -o /tmp/stage0_create_resp.json -w '%{http_code}' \
  "${auth[@]+"${auth[@]}"}" -H 'Content-Type: application/json' \
  -X POST "${OMNI_SERVER}/v1/sessions" -d "${body}")

echo "HTTP ${http_code}"
echo "Response:"; cat /tmp/stage0_create_resp.json; echo
echo
case "${http_code}" in
  2*) echo "OK (a): managed create accepted the sentinel workspace."
      echo "Now check (b) — the launcher was reached:"
      echo "    cat /tmp/sbx-stage0-probe.log"
      echo "Expect a line: STAGE0 PROBE: ... path='${WORKTREE}' mode='${MODE}' ..." ;;
  422) echo "422: parse/validation REJECTED the sentinel — inspect the response above." ;;
  *)   echo "Unexpected ${http_code} — inspect the response above." ;;
esac
