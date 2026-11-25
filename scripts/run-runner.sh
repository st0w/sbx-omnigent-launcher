#!/bin/sh
# Run an Omnigent pipeline with the sbx runner. Works for any pipeline and
# from any directory:
#
#   run-runner.sh --run-id <campaign>-<attempt> [--resume]
#
# This file carries no absolute paths, no usernames, no secret-store names
# and nothing specific to one pipeline. Everything arrives through the
# environment, and every required variable fails loud if unset rather than
# defaulting to something that only works on one laptop or for one pipeline.
#
# Per pipeline:
#
#   OMNI_PIPELINE      The pipeline definition (its pipeline.live.yaml).
#   OMNI_PUBLISH_REPO  https://github.com/ URL of the repo the run publishes
#                      its branches to.
#
# Per machine (PUBLISH_TOKEN_CMD per pipeline instead, if the publish repos
# need different tokens):
#
#   OMNI_LAUNCHER_PY   Python interpreter of the sbx-omnigent-launcher venv.
#   OMNI_CANONICAL_ROOT
#                      Root of the canonical mirrors.
#   OMNI_WORKTREE_ROOT Root of the run worktrees. Must equal
#                      sandbox.sbx.worktree_root in the server's config.yaml.
#   PUBLISH_TOKEN_CMD  Command printing ONLY a GitHub token with push access
#                      to the publish repo, on stdout. It is read by the
#                      runner on the HOST and passed to git/gh through the
#                      environment — never into argv, never into a VM.
#
# OMNI_PIPELINE, OMNI_LAUNCHER_PY and the two roots must be ABSOLUTE paths,
# so nothing depends on the directory the script is started from.
#
# Example (adapt to whatever secret store you use — the runner only cares
# that the command prints a token):
#
#   export OMNI_PIPELINE="/path/to/repos/<repo>/omnigent/pipeline.live.yaml"
#   export OMNI_PUBLISH_REPO="https://github.com/<org>/<repo>"
#   export OMNI_LAUNCHER_PY="/path/to/sbx-omnigent-launcher/.venv/bin/python"
#   export OMNI_CANONICAL_ROOT="$HOME/sbx-canonical"
#   export OMNI_WORKTREE_ROOT="$HOME/sbx-worktrees"
#   export PUBLISH_TOKEN_CMD='pass show <store>/publish-token'
#   export PUBLISH_TOKEN_CMD='security find-generic-password -w -s <item>'
#
# The runner splits PUBLISH_TOKEN_CMD with shlex and runs it as ARGV — never
# through a shell — so nothing in it is expanded, globbed or piped at read
# time. Let YOUR shell expand $HOME at assignment (double quotes, as above) or
# write an absolute path; a single-quoted '$HOME/...' reaches `cat` verbatim
# and fails.
#
# This command must READ THE STORE, not a snapshot of it. It is re-run at
# PUSH time, hours after the run began, precisely so a secret rotated
# mid-run is picked up — and re-run once more if the push is rejected as
# unauthenticated. Point it at a file you copied the token into and you
# defeat all of that: the copy goes stale the moment you rotate, and the
# retry re-reads the same stale value and correctly reports that the store
# has not changed. Keep one source of truth (TASKS.md #43).
#
# It must ALSO answer without a GUI prompt, or an unattended publish blocks
# on a dialog nobody is there to click. Those two requirements are not in
# tension — make the store itself readable without a prompt. On macOS that
# means trusting /usr/bin/security on THAT ONE keychain item (click "Always
# Allow" once, or set its partition list); every other item still prompts,
# and the value stays encrypted at rest.
#
# Each pipeline's `repo:` is a NEUTRAL local path (for example
# /path/to/repos/<repo>), so the pipeline definition carries no
# personal filesystem layout. Create it once per machine and repo:
#
#   sudo mkdir -p /path/to/repos
#   sudo ln -s /absolute/path/to/your/<repo>-checkout /path/to/repos/<repo>
#
# A local path (rather than the GitHub URL) keeps the canonical mirror a fast
# local clone that needs NO credential at all. Pointing `repo:` at
# https://github.com/... would make every run authenticate with whatever the
# host's git credential helper offers — a broad personal token — for an
# operation that currently needs none.
#
# NETWORK. The sandboxes reach the internet through sbx's policy allowlist.
# Each pipeline should list the hosts its VMs fetch from under `egress:` in
# its pipeline definition. The runner checks each one against what the VMs
# may reach before any turn, so a missing rule fails the run at the start
# rather than hours in. Allow one it refuses with (machine-wide: every
# sandbox on the machine gets it):
#
#   sbx policy allow network '<host>:443'
#
# Confirm it took, from inside a sandbox rather than from your shell — a
# host curl proves nothing about the guest's path:
#
#   sbx exec <any-sandbox> -- sh -c \
#     'curl -sS -o /dev/null -w "%{http_code}" https://<host>/'
#
# If you also run a host-level outbound firewall (Little Snitch and the like),
# its rules apply on top and are per-PROCESS: the guest's traffic egresses via
# the sbx proxy, not your shell, so allow that process and make the rule
# PERMANENT. A one-hour grant expires mid-run, and an unattended writer that
# loses its package registry will spend its whole turn budget politely
# retrying.
#
# Set OMNI_SBX_DISK_METRICS=1 to record what each module costs on disk
# (every node worktree + the sbx store, sampled at each stage boundary, into
# <canonical-root>/_metrics/<run>.jsonl).

set -eu

: "${OMNI_PIPELINE:?set OMNI_PIPELINE to the pipeline definition}"
: "${OMNI_PUBLISH_REPO:?set OMNI_PUBLISH_REPO to the https://github.com/ URL of the publish repo}"
: "${OMNI_LAUNCHER_PY:?set OMNI_LAUNCHER_PY to the launcher venv python}"
: "${OMNI_CANONICAL_ROOT:?set OMNI_CANONICAL_ROOT to the canonical mirror root}"
: "${OMNI_WORKTREE_ROOT:?set OMNI_WORKTREE_ROOT to the worktree root}"
: "${PUBLISH_TOKEN_CMD:?set PUBLISH_TOKEN_CMD to a command printing a push token}"

die() {
  echo "run-runner.sh: $*" >&2
  exit 2
}

require_absolute() {
  case $2 in
    /*) ;;
    *) die "$1 must be an absolute path, not '$2'" ;;
  esac
}
require_absolute OMNI_PIPELINE "$OMNI_PIPELINE"
require_absolute OMNI_LAUNCHER_PY "$OMNI_LAUNCHER_PY"
require_absolute OMNI_CANONICAL_ROOT "$OMNI_CANONICAL_ROOT"
require_absolute OMNI_WORKTREE_ROOT "$OMNI_WORKTREE_ROOT"

# The push authenticates with PUBLISH_TOKEN_CMD's GitHub token, which only
# applies over https. An ssh URL would push with whatever key the host
# offers instead, the same broad-credential problem described above.
case $OMNI_PUBLISH_REPO in
  https://github.com/*) ;;
  *) die "OMNI_PUBLISH_REPO must be an https://github.com/ URL, not '$OMNI_PUBLISH_REPO'" ;;
esac

# The campaign id is REQUIRED, as the runner's own --run-id flag, and this
# file carries no default: a default went stale with every module, and
# keeping it current cost a pull request per campaign.
#
#   run-runner.sh --run-id <campaign>-1             clean start
#   run-runner.sh --run-id <campaign>-1 --resume    continue that one
#
# The id names the worktree directory every node works in --
# $OMNI_WORKTREE_ROOT/<run-id>/nodes/<chunk>-<agent> -- and it is what
# --resume keys on, so a wrong one either resumes a finished campaign or
# files this one's work under a previous campaign's name. It also names
# every branch the run publishes (pipeline/<run-id>-<module>), so reusing a
# finished campaign's id recreates branch names already merged and deleted
# on the remote. Pipelines run against the same server share its worktree
# root, so an id must be unique across all of them, not just within one.
#
# The convention is <campaign>-<attempt>. The attempt number rises only when
# a campaign is restarted from scratch rather than resumed. A campaign of
# several modules runs as ONE campaign, not one per module: campaign mode
# threads each module's winning branch into the next within a single run.
#
# Because the id becomes a directory name, it must be letters, digits,
# '.', '_' and '-', and must not begin with '.' or '-'.
run_id=
take_next=false
for argument in "$@"; do
  if [ "$take_next" = true ]; then
    run_id=$argument
    take_next=false
  fi
  case $argument in
    --run-id) take_next=true ;;
    --run-id=*) run_id=${argument#--run-id=} ;;
  esac
done
case $run_id in
  '' | [.-]* | *[!A-Za-z0-9._-]*)
    die 'pass a campaign id, e.g. --run-id <campaign>-<attempt>'
    ;;
esac

exec "$OMNI_LAUNCHER_PY" -m sbx_omnigent.runner \
  -c "$OMNI_PIPELINE" \
  --canonical-root "$OMNI_CANONICAL_ROOT" \
  --worktree-root "$OMNI_WORKTREE_ROOT" \
  --publish-repo "$OMNI_PUBLISH_REPO" \
  --publish-token-command "$PUBLISH_TOKEN_CMD" \
  "$@"
