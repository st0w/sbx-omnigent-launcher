"""Host-side git worktree lifecycle for collaborative swarms.

The *trusted plane* plumbing a per-swarm coordinator drives (via
``sys_os_shell``) to feed the sandbox mount path built in
:mod:`sbx_omnigent.launcher`. Nothing here runs inside a microVM.

Topology (see ``docs/COLLABORATIVE_SWARM_DESIGN.md``):

- a **canonical bare mirror** of the GitHub repo lives under
  ``canonical_root`` and is only ever refreshed *from* GitHub
  (``clone --mirror`` once, ``fetch --prune`` after) — never pushed to,
  so it can't prune a task branch that hasn't reached GitHub yet;
- each swarm gets a **standalone clone** under ``worktree_root`` on
  branch ``task/<swarm_id>``, cheap and hardlinked from the mirror;
- **publish** pushes ``task/<swarm_id>`` straight from the worktree to
  GitHub (WIP stays local until then) and opens a *draft* PR — the
  human reviews and merges.

``worktree_root`` MUST match the server's ``sandbox.sbx.worktree_root``
so the launcher can bind-mount the worktrees this module creates.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
import time
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from collections.abc import Callable

#: Branch prefix for a swarm's work. Publish only ever pushes a ref
#: under this prefix — never the base/default branch.
_TASK_BRANCH_PREFIX = 'task/'

#: Safe charset for a swarm id and a derived repo name: the id becomes a
#: filesystem directory AND a branch component, so keep it conservative.
_SAFE_NAME_RE = re.compile(r'^[A-Za-z0-9._-]+$')


def _run_default(
    cmd: list[str],
    *,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """
    Run a command, returning stdout and failing loud on error.

    :param cmd: Full argv, e.g. ``["git", "clone", url, dest]``.
    :param cwd: Working directory, or ``None`` for the current one.
    :param env: Full environment for the child, or ``None`` to inherit
        this process's. Used to scope a publish token to just the
        commands that need it (see
        :meth:`WorktreeManager._run_publish`).
    :returns: Captured stdout.
    :raises click.ClickException: If the command exits non-zero.
    """
    proc = subprocess.run(
        cmd, cwd=cwd, env=env, capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise click.ClickException(
            f'command failed (rc={proc.returncode}): '
            f'{" ".join(cmd)}\n{proc.stderr.strip()}'
        )
    return proc.stdout


def _validate_name(value: str, kind: str) -> str:
    """
    Validate a swarm id / repo name against the safe charset.

    :param value: The candidate string.
    :param kind: Human label for errors, e.g. ``"swarm id"``.
    :returns: *value* unchanged when valid.
    :raises click.ClickException: If it contains anything but letters,
        digits, ``.``, ``_``, ``-``, or is ``.``/``..``.
    """
    if (
        not isinstance(value, str)
        or value in ('.', '..')
        or not _SAFE_NAME_RE.fullmatch(value)
    ):
        raise click.ClickException(
            f'invalid {kind}: {value!r} (allowed: letters, digits, and . _ -)'
        )
    return value


def _repo_name(repo_url: str) -> str:
    """
    Derive the canonical mirror's base name from a repo URL.

    :param repo_url: e.g. ``"https://github.com/org/repo.git"`` or
        ``"git@github.com:org/repo"``.
    :returns: The last path segment with ``.git`` stripped, e.g.
        ``"repo"``.
    :raises click.ClickException: If no safe name can be derived.
    """
    last = repo_url.rstrip('/').split('/')[-1]
    if ':' in last:
        last = last.rsplit(':', 1)[-1]
    name = last[:-4] if last.endswith('.git') else last
    return _validate_name(name, 'repo name')


def _redact(text: str, secret: str | None) -> str:
    """
    Blank *secret* out of *text*.

    git and gh can echo a credential back in their error output, and
    that output is interpolated into the exception a failed command
    raises. Scrubbing it keeps a token out of terminals and logs.

    :param text: The text to scrub.
    :param secret: The value to remove, or ``None`` for a no-op.
    :returns: *text* with every occurrence of *secret* replaced.
    """
    if not secret:
        return text
    return text.replace(secret, '***')


#: Substrings that mark a failed publish as a CREDENTIAL failure
#: rather than a repository one, lower-cased for matching. Only these
#: earn the one re-read-and-retry in
#: :meth:`WorktreeManager._run_publish` — a missing branch or a
#: protected base must fail immediately, not get tried twice with a
#: different token.
#:
#: The first entry is verbatim from the live failure this exists for:
#: ``remote: Invalid username or token. Password authentication is not
#: supported for Git operations.`` / ``fatal: Authentication failed``.
_AUTH_FAILURE_MARKERS = (
    'invalid username or token',
    'authentication failed',
    'bad credentials',
    'could not read username',
    'requires authentication',
    'http 401',
    '401 unauthorized',
)


def looks_like_auth_failure(text: str) -> bool:
    """
    Whether a failed publish looks like a REJECTED CREDENTIAL.

    Deliberately narrow. Re-reading the token and retrying is only ever
    right when the token itself was the problem; retrying a "branch
    already exists" or a protected-base rejection just runs a failing
    command twice and doubles the time to the error.

    :param text: The failure text (git/gh stderr, already redacted).
    :returns: ``True`` when the failure names a credential problem.
    """
    lowered = text.lower()
    return any(marker in lowered for marker in _AUTH_FAILURE_MARKERS)


def github_slug(repo_url: str) -> str:
    """
    Extract ``owner/repo`` from a GitHub URL for ``gh -R``.

    Also the validator for "can this target open a PR at all?" — the
    runner calls it at startup so ``publish: mode: pr`` against a
    non-GitHub target (a local path, say) fails in seconds instead of
    at the end of the first build.

    :param repo_url: An ``https://`` / ``ssh://`` / ``git@`` GitHub URL.
    :returns: ``"owner/repo"``.
    :raises click.ClickException: If owner/repo cannot be parsed.
    """
    url = repo_url
    if url.startswith('git@'):
        _host, _sep, path = url.partition(':')
    elif '://' in url:
        after = url.split('://', 1)[1]
        path = after.split('/', 1)[1] if '/' in after else ''
    else:
        raise click.ClickException(
            f'cannot parse a GitHub slug from {repo_url!r}'
        )
    path = path.strip('/')
    if path.endswith('.git'):
        path = path[:-4]
    parts = [p for p in path.split('/') if p]
    if len(parts) < 2:
        raise click.ClickException(
            f'cannot parse owner/repo from {repo_url!r}'
        )
    return f'{parts[-2]}/{parts[-1]}'


def _pr_create_command(
    slug: str,
    *,
    head: str,
    base: str,
    title: str,
    body: str,
    draft: bool,
) -> list[str]:
    """
    Build the ``gh pr create`` argv (a draft PR by default).

    :param slug: ``owner/repo`` for ``-R``.
    :param head: The task branch to open the PR from.
    :param base: The base branch to merge into.
    :param title: PR title.
    :param body: PR body.
    :param draft: Open as a draft (the human marks it ready).
    :returns: The ``gh`` argv.
    """
    cmd = [
        'gh',
        'pr',
        'create',
        '-R',
        slug,
        '--base',
        base,
        '--head',
        head,
        '--title',
        title,
        '--body',
        body,
    ]
    if draft:
        cmd.append('--draft')
    return cmd


class WorktreeManager:
    """
    Canonical mirror + per-swarm worktree lifecycle.

    :param canonical_root: Host dir holding ``<name>.git`` bare mirrors.
    :param worktree_root: Host dir holding per-swarm worktrees; MUST
        equal the server's ``sandbox.sbx.worktree_root`` so the launcher
        can bind-mount them.
    :param default_branch: Base branch cut from and merged into
        (e.g. ``"main"``).
    :param run: Command runner (injectable for tests); defaults to
        :func:`_run_default`.
    :param publish_token: Token for the identity that pushes and opens
        PRs (a dedicated pipeline account, say, rather than the human
        running this), OR a zero-arg callable returning one. ``None`` =
        use whatever git/gh credentials the host already has.
        **Prefer the callable**: publish happens at the END of a run
        that may have taken hours, and a token captured at startup can
        be rotated or expired by then — which cost a finished module
        its pull request (TASKS.md #43). A callable is read lazily and
        re-read once if the credential is rejected. See
        :meth:`_run_publish`.
    :param build_cache: Directory names, relative to a node worktree
        root, carried between nodes as a warm build cache — e.g.
        ``('target',)`` for cargo. Empty (the default) disables it.
        See :meth:`seed_build_cache`.
    :param build_cache_key: Names the cache directory under
        ``<canonical_root>/_buildcache/``, so two repositories never
        share one. ``None`` also disables the cache.
    """

    def __init__(
        self,
        *,
        canonical_root: str,
        worktree_root: str,
        default_branch: str = 'main',
        run: Callable[..., str] = _run_default,
        publish_token: str | Callable[[], str | None] | None = None,
        build_cache: tuple[str, ...] = (),
        build_cache_key: str | None = None,
    ) -> None:
        self._build_cache = build_cache
        self._build_cache_key = build_cache_key
        self._canonical_root = canonical_root
        self._worktree_root = worktree_root
        self._default_branch = default_branch
        self._run = run
        # Normalise both forms to a provider so there is one code path.
        # A plain string stays supported (tests and direct callers).
        self._publish_token_provider: Callable[[], str | None] = (
            publish_token
            if callable(publish_token)
            else (lambda: publish_token)
        )
        self._publish_token: str | None = None
        self._publish_token_read = False

    def publish_token(self, *, refresh: bool = False) -> str | None:
        """
        The publish credential, read lazily and cached.

        LAZY on purpose. Publish is the LAST thing a run does, and a
        run can take hours; a token read at startup and held in memory
        is a token that may have been rotated or expired by the time it
        is used. That is not hypothetical — it cost a finished module
        its pull request, after every stage had passed (TASKS.md #43).

        :param refresh: Re-read even when a value is already cached.
            Used once, after a credential is rejected.
        :returns: The token, or ``None`` when the run publishes on the
            host's own git/gh credentials.
        """
        if refresh or not self._publish_token_read:
            self._publish_token = self._publish_token_provider()
            self._publish_token_read = True
        return self._publish_token

    def create_issue(
        self,
        repo_url: str,
        *,
        title: str,
        body: str,
        label: str | None = None,
    ) -> str | None:
        """
        File one issue on the publish repository.

        Goes through :meth:`_run_publish`, so it inherits the publish
        identity, the rotated-credential retry and the redaction that
        the pull-request path already has.

        Best-effort by contract: a tracker that is down, a repository
        with issues disabled, or a label that does not exist must never
        fail a publish that otherwise succeeded. The finding is still in
        the reviewer report committed to the branch.

        :param repo_url: The publish repository URL.
        :param title: Issue title.
        :param body: Issue body (markdown).
        :param label: Label to apply, or ``None``. A label the repo does
            not have makes ``gh`` refuse the whole call, so this retries
            once WITHOUT it rather than losing the finding.
        :returns: The new issue's URL, or ``None`` if it could not be
            filed.
        """
        slug = github_slug(repo_url)
        base = ['gh', 'issue', 'create', '-R', slug,
                '--title', title, '--body', body]
        try:
            return self._run_publish(
                base + (['--label', label] if label else [])
            ).strip() or None
        except click.ClickException:
            if not label:
                return None
        try:
            return self._run_publish(base).strip() or None
        except click.ClickException:
            return None

    def issue_bodies_text(
        self, repo_url: str, *, limit: int = 500
    ) -> str | None:
        """
        Every issue body on the publish repo, OPEN and CLOSED, as one
        blob to search.

        Closed ones matter most: an issue a human closed is a finding
        they have dealt with, and re-filing it next run is the one
        behaviour that would make the tracker worthless.

        Returned as ONE string rather than a list because bodies are
        multi-line and the caller only ever asks "does this marker
        appear anywhere" — splitting would invent boundaries that do not
        matter and could cut a marker in half.

        :param repo_url: The publish repository URL.
        :param limit: Max issues to read.
        :returns: The concatenated bodies, or ``None`` when they could
            not be read at all. ``None`` is NOT the same as empty: empty
            means file everything, unreadable means file NOTHING, since
            filing blind is how a tracker fills with duplicates.
        """
        slug = github_slug(repo_url)
        try:
            return self._run_publish([
                'gh', 'issue', 'list', '-R', slug,
                '--state', 'all', '--limit', str(limit),
                '--json', 'body', '--jq', '.[].body',
            ])
        except click.ClickException:
            return None

    def _run_publish(self, cmd: list[str]) -> str:
        """
        Run a publish command as the publish identity.

        The token is injected into THIS child's environment only —
        never into the manager's own process (where every later
        subprocess would inherit it) and never into argv (where ``ps``
        and the failure message would expose it). With no token
        configured the command runs exactly as before, on the host's
        own credentials.

        A REJECTED credential — and only that, see
        :func:`looks_like_auth_failure` — gets the token re-read once
        and the command retried. A rotation mid-run is ordinary
        (expiry, a re-issued PAT), and the alternative is throwing away
        a whole finished module's publish over a value that is sitting
        correct in the credential store.

        :param cmd: The ``git push`` / ``gh`` argv.
        :returns: Captured stdout.
        :raises click.ClickException: On failure, with the token
            scrubbed from the message.
        """
        token = self.publish_token()
        if token is None:
            return self._run(cmd)
        try:
            return self._run(cmd, env={**os.environ, 'GH_TOKEN': token})
        except click.ClickException as exc:
            # `from None` throughout: the original's traceback and args
            # may carry the token.
            detail = _redact(str(exc), token)
            if not looks_like_auth_failure(detail):
                raise click.ClickException(detail) from None
        try:
            fresh = self.publish_token(refresh=True)
        except Exception as exc:
            # The re-read failed. Report the ORIGINAL rejection, which
            # is the failure that matters, and name this as a footnote.
            raise click.ClickException(
                f'{detail}\n\n(the publish token was rejected, and '
                f're-reading it also failed: {exc})'
            ) from None
        if fresh is None or fresh == token:
            raise click.ClickException(
                f'{detail}\n\n(the publish token was rejected, and '
                f'the credential store still holds the same value — so '
                f'the token itself is expired or lacks access to this '
                f'repository, rather than being stale in this process.)'
            ) from None
        click.echo(
            '[publish] the token was rejected and the credential store '
            'now holds a different one (rotated mid-run?) — retrying '
            'once with the current value.'
        )
        try:
            return self._run(cmd, env={**os.environ, 'GH_TOKEN': fresh})
        except click.ClickException as exc:
            raise click.ClickException(
                _redact(str(exc), fresh)
            ) from None

    def branch_for(self, swarm_id: str) -> str:
        """:returns: The task branch name, e.g. ``"task/swarm-a"``."""
        return f'{_TASK_BRANCH_PREFIX}{_validate_name(swarm_id, "swarm id")}'

    def canonical_path(self, repo_url: str) -> str:
        """:returns: The bare mirror path for *repo_url*."""
        return os.path.join(
            self._canonical_root, f'{_repo_name(repo_url)}.git'
        )

    def worktree_path(self, swarm_id: str) -> str:
        """:returns: The worktree path for *swarm_id* (validated)."""
        return os.path.join(
            self._worktree_root, _validate_name(swarm_id, 'swarm id')
        )

    def ensure_canonical(self, repo_url: str) -> str:
        """
        Create-or-refresh the canonical bare mirror of *repo_url*.

        First call clones ``--mirror``; later calls ``fetch --prune`` so
        a swarm is cut from current upstream. The mirror is fetch-only
        (never pushed to), so refreshing can't drop a task branch.

        :param repo_url: The GitHub repo URL.
        :returns: The canonical mirror path.
        :raises click.ClickException: If a git command fails.
        """
        path = self.canonical_path(repo_url)
        if os.path.isdir(path):
            self._run(['git', '-C', path, 'fetch', '--prune', 'origin'])
        else:
            os.makedirs(self._canonical_root, exist_ok=True)
            self._run(['git', 'clone', '--mirror', repo_url, path])
        return path

    def create_swarm_worktree(
        self,
        swarm_id: str,
        repo_url: str,
        base_branch: str | None = None,
    ) -> str:
        """
        Cut a fresh worktree for *swarm_id* on ``task/<swarm_id>``.

        Refreshes the mirror, then clones it locally (hardlinked, no
        checkout) and creates the task branch off the base branch.

        :param swarm_id: Swarm identifier (also the directory + branch
            component).
        :param repo_url: The GitHub repo URL.
        :param base_branch: Branch to cut from; ``None`` uses the
            manager's default branch.
        :returns: The absolute worktree path.
        :raises click.ClickException: If it already exists or a git
            command fails.
        """
        swarm_id = _validate_name(swarm_id, 'swarm id')
        canonical = self.ensure_canonical(repo_url)
        worktree = self.worktree_path(swarm_id)
        if os.path.exists(worktree):
            raise click.ClickException(f'worktree already exists: {worktree}')
        base = base_branch or self._default_branch
        os.makedirs(self._worktree_root, exist_ok=True)
        # --no-checkout: create the task branch off origin/<base>
        # ourselves, so we never depend on the mirror's HEAD.
        self._run(['git', 'clone', '--no-checkout', canonical, worktree])
        self._run(
            [
                'git',
                '-C',
                worktree,
                'checkout',
                '-b',
                self.branch_for(swarm_id),
                f'origin/{base}',
            ]
        )
        return worktree

    def commit_worktree(
        self,
        swarm_id: str,
        *,
        message: str,
        author: str | None = None,
        committer_name: str = 'swarm-coordinator',
        committer_email: str = 'swarm@localhost',
    ) -> bool:
        """
        Stage and commit the worktree's current state (trusted plane).

        The coder only edits files in its ``rw`` mount; the trusted
        plane (this, on the host) turns the approved working tree into a
        commit. That keeps ALL git in the trusted plane — the untrusted
        coder VM needs no git write capability — and it is reliable code
        rather than an agent instruction. Authorship can still be
        attributed to the coder via *author* while the committer stays
        the coordinator identity.

        :param swarm_id: Swarm identifier.
        :param message: Commit message.
        :param author: Optional ``"Name <email>"`` recorded as the
            commit AUTHOR (e.g. the coder), or ``None`` to use the
            committer identity for both.
        :param committer_name: Committer name recorded on the commit.
        :param committer_email: Committer email recorded on the commit.
        :returns: ``True`` if a commit was created, ``False`` if the
            worktree had nothing to commit (clean tree).
        :raises click.ClickException: On a missing worktree or a failed
            git command.
        """
        swarm_id = _validate_name(swarm_id, 'swarm id')
        worktree = self.worktree_path(swarm_id)
        if not os.path.isdir(worktree):
            raise click.ClickException(
                f'no worktree for swarm {swarm_id!r}: {worktree}'
            )
        # Nothing staged AND nothing unstaged/untracked → clean tree.
        status = self._run(['git', '-C', worktree, 'status', '--porcelain'])
        if not status.strip():
            return False
        self._run(['git', '-C', worktree, 'add', '-A'])
        cmd = [
            'git',
            '-C',
            worktree,
            '-c',
            f'user.name={committer_name}',
            '-c',
            f'user.email={committer_email}',
            'commit',
            '-m',
            message,
        ]
        if author is not None:
            cmd += ['--author', author]
        self._run(cmd)
        return True

    def publish_swarm(
        self,
        swarm_id: str,
        repo_url: str,
        *,
        title: str,
        body: str,
        base_branch: str | None = None,
        draft: bool = True,
        open_pr: bool = True,
    ) -> str:
        """
        Push the swarm's approved task branch to *repo_url*.

        Both modes push ``task/<swarm>`` straight from the worktree with
        an explicit ``src:dst`` refspec (only that branch moves — never
        the base). Then, depending on *open_pr*:

        - **GitHub mode** (``open_pr=True``): ``gh pr create`` opens a
          draft PR against *repo_url*'s GitHub slug for the human to
          review and merge. *repo_url* must be a GitHub URL.
        - **Local mode** (``open_pr=False``): stop after the push — no
          ``gh``, no network beyond the push itself. *repo_url* is any
          git remote (typically a local path), and the human merges
          ``task/<swarm>`` there when ready. Use this to keep everything
          local / off GitHub.

        :param swarm_id: Swarm identifier.
        :param repo_url: Push target — a GitHub URL (``open_pr=True``),
            or any git remote / local path (``open_pr=False``).
        :param title: PR title (GitHub mode).
        :param body: PR body (GitHub mode).
        :param base_branch: PR base; ``None`` uses the default branch.
        :param draft: Open the PR as a draft (GitHub mode; default
            ``True``).
        :param open_pr: Open a GitHub draft PR after the push. ``False``
            = local mode (push only).
        :returns: The PR URL (GitHub mode), or a human-readable summary
            of the pushed branch (local mode).
        :raises click.ClickException: On a missing worktree, a base ==
            task collision, or a failed command.
        """
        swarm_id = _validate_name(swarm_id, 'swarm id')
        worktree = self.worktree_path(swarm_id)
        if not os.path.isdir(worktree):
            raise click.ClickException(
                f'no worktree for swarm {swarm_id!r}: {worktree}'
            )
        branch = self.branch_for(swarm_id)
        base = base_branch or self._default_branch
        # Defense in depth: never push onto the base branch.
        if branch == base or not branch.startswith(_TASK_BRANCH_PREFIX):
            raise click.ClickException(
                f'refusing to publish branch {branch!r} onto base {base!r}'
            )
        self._run_publish(
            [
                'git',
                '-C',
                worktree,
                'push',
                repo_url,
                f'{branch}:{branch}',
            ]
        )
        if not open_pr:
            # Local mode: the reviewed branch now lives in repo_url; the
            # human merges it there. No GitHub, no gh.
            return (
                f'Pushed {branch} to {repo_url}. Review and merge it '
                f'into {base} when ready (e.g. `git merge {branch}`).'
            )
        cmd = _pr_create_command(
            github_slug(repo_url),
            head=branch,
            base=base,
            title=title,
            body=body,
            draft=draft,
        )
        return self._run_publish(cmd).strip()

    def dispose_swarm(self, swarm_id: str) -> None:
        """
        Remove a swarm's worktree (its branch lives on the remote).

        Only paths that resolve strictly under ``worktree_root`` are
        removed, so a crafted id can never delete elsewhere.

        :param swarm_id: Swarm identifier.
        :raises click.ClickException: If the resolved path escapes the
            root.
        """
        worktree = self.worktree_path(swarm_id)
        root = os.path.realpath(self._worktree_root)
        real = os.path.realpath(worktree)
        if real != worktree and not real.startswith(root + os.sep):
            # A symlinked worktree entry resolving outside the root.
            raise click.ClickException(
                f'refusing to remove {worktree!r}: resolves outside {root!r}'
            )
        if os.path.isdir(worktree):
            shutil.rmtree(worktree)

    # ── Pipeline: branch-as-artifact, isolated per-writer ─────────
    #
    # A pipeline RUN keeps ONE shared "hub" clone of the canonical
    # mirror (``<run>/repo``) that aggregates every node's branch. Each
    # node gets its OWN standalone clone of that hub under
    # ``<run>/nodes/<node>`` — a full ``.git`` DIRECTORY, not a linked
    # ``git worktree`` — so git works normally inside the node's
    # microVM mount (a linked worktree's ``.git`` file points at the
    # hub's path, which is NOT mounted into the VM, leaving git broken
    # and confusing agents that orient via ``git status``/``git diff``).
    # A node's branch is seeded from an upstream node's branch
    # (inheritance) or the base, then pushed back to the hub so a
    # downstream writer can inherit it, a judge can compare competing
    # candidates, and publish/merge can find it. All git stays in the
    # trusted plane; agent VMs only edit files in their mount.

    def run_dir(self, run_id: str) -> str:
        """:returns: The run's directory under ``worktree_root``."""
        return os.path.join(
            self._worktree_root, _validate_name(run_id, 'run id')
        )

    def run_state_path(self, run_id: str) -> str:
        """
        :returns: The run's state file, beside its hub clone.

        Lives in the RUN dir, not the repo: it is orchestrator
        bookkeeping, never something a node commits or an agent sees.
        """
        return os.path.join(self.run_dir(run_id), 'state.json')

    def write_run_state(self, run_id: str, payload: dict) -> bool:
        """
        Persist a run's node bookkeeping, atomically and best-effort.

        Committed work already survives a crash — every writer's tree is
        on a hub branch. What does NOT survive is the bookkeeping: which
        nodes ran, and the outputs that exist only in memory (an
        approved plan of record, a judge's selection). Losing those is
        what forces a whole run to be repeated, so they are written out
        as the run proceeds.

        Written via a temp file + rename so a crash mid-write can never
        leave a half-parsed state behind. NEVER raises: bookkeeping must
        not be able to fail a run that is otherwise succeeding.

        :param run_id: Pipeline run id.
        :param payload: JSON-serializable run state.
        :returns: Whether the state was written.
        """
        path = self.run_state_path(run_id)
        tmp = f'{path}.tmp'
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
            os.replace(tmp, path)
            return True
        except (OSError, TypeError, ValueError):
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            return False

    def write_run_artifact(
        self, run_id: str, relpath: str, content: str
    ) -> bool:
        """
        Write a durable text artifact into the run directory.

        For records that must outlive the thing that produced them. A
        reviewer's report is the case this exists for: its microVM is
        disposed the moment it votes, and deleting the session deletes
        the transcript, so the report has to be written down BEFORE the
        vote is acted on. The run directory now survives a failed run,
        which makes it the right home — a blocked run keeps the reports
        that explain why it blocked.

        Best-effort and never raises, like :meth:`write_run_state`:
        recording something must not be able to fail a run that is
        otherwise succeeding. Written via temp + rename so a crash
        cannot leave a half-written record.

        :param run_id: Pipeline run id.
        :param relpath: Destination path under the run dir (may be
            nested). Rejected if absolute or escaping the run dir.
        :param content: File contents.
        :returns: Whether the artifact was written.
        """
        if not relpath or os.path.isabs(relpath):
            return False
        try:
            root = os.path.realpath(self.run_dir(run_id))
            full = os.path.realpath(os.path.join(root, relpath))
            if full == root or os.path.commonpath([full, root]) != root:
                return False
            os.makedirs(os.path.dirname(full), exist_ok=True)
            tmp = f'{full}.tmp'
            with open(tmp, 'w', encoding='utf-8') as handle:
                handle.write(content)
            os.replace(tmp, full)
            return True
        except (OSError, ValueError, click.ClickException):
            return False

    def read_run_state(self, run_id: str) -> dict | None:
        """
        Read a run's persisted state, or ``None`` when unusable.

        :param run_id: Pipeline run id.
        :returns: The state mapping, or ``None`` when absent,
            unreadable, or not a JSON object.
        """
        try:
            with open(self.run_state_path(run_id), encoding='utf-8') as fh:
                state = json.load(fh)
        except (OSError, ValueError):
            return None
        return state if isinstance(state, dict) else None

    def _run_repo(self, run_id: str) -> str:
        """:returns: The run's shared hub clone path."""
        return os.path.join(self.run_dir(run_id), 'repo')

    def _nodes_dir(self, run_id: str) -> str:
        """:returns: The dir holding per-node clones for a run."""
        return os.path.join(self.run_dir(run_id), 'nodes')

    def node_branch(self, run_id: str, node_id: str) -> str:
        """:returns: A node's branch, e.g. ``pl/<run>/<node>``."""
        return (
            f'pl/{_validate_name(run_id, "run id")}/'
            f'{_validate_name(node_id, "node id")}'
        )

    def node_worktree_path(self, run_id: str, node_id: str) -> str:
        """:returns: A node's isolated clone path."""
        return os.path.join(
            self._nodes_dir(run_id), _validate_name(node_id, 'node id')
        )

    def create_run(
        self,
        run_id: str,
        repo_url: str,
        *,
        base_branch: str | None = None,
        reuse: bool = False,
    ) -> str:
        """
        Create a run's shared hub clone from the canonical mirror.

        The hub aggregates every node branch: each node is a standalone
        clone of THIS hub and pushes its branch back here, so branch
        inheritance, judge comparison, and publish all resolve against
        one repo — while every node still gets its own isolated,
        self-contained working tree.

        :param run_id: Pipeline run id (dir + branch component).
        :param repo_url: Repo URL/path to mirror + clone.
        :param base_branch: Unused today; reserved so callers can record
            a non-default base. Nodes pass their own base at cut time.
        :param reuse: Resume into an EXISTING run's hub instead of
            refusing. The hub is the durable record of what the earlier
            attempt achieved, so it is reused, never re-cloned.
        :returns: The shared hub clone path.
        :raises click.ClickException: If the run exists (and *reuse* is
            unset), if resuming a run with no hub, or if git fails.
        """
        run_id = _validate_name(run_id, 'run id')
        rdir = self.run_dir(run_id)
        if reuse:
            # Resuming: the hub already aggregates every branch the
            # earlier attempt committed, and re-cloning would throw
            # exactly the work we are resuming onto.
            repo = self._run_repo(run_id)
            if not os.path.isdir(repo):
                raise click.ClickException(
                    f'cannot resume run {run_id!r}: no hub clone at {repo}'
                )
            return repo
        canonical = self.ensure_canonical(repo_url)
        if os.path.exists(rdir):
            raise click.ClickException(f'run already exists: {rdir}')
        os.makedirs(self._nodes_dir(run_id), exist_ok=True)
        repo = self._run_repo(run_id)
        # --no-checkout: the hub needs no working tree of its own; it
        # only holds refs. Node clones + branch pushes flow through it.
        self._run(['git', 'clone', '--no-checkout', canonical, repo])
        return repo

    def hub_branch_tip(self, run_id: str, node_id: str) -> str | None:
        """
        The commit a node's branch points at ON THE HUB.

        The hub is the authority: downstream nodes seed from it, the
        judge clones it, the no-op guard diffs it, and publish pushes
        from it. A node's own clone can be AHEAD of it (see
        :meth:`commit_node`), so "what did the agent do" and "what will
        anyone else see" are different questions — this answers the
        second.

        :param run_id: Pipeline run id.
        :param node_id: The node whose branch to resolve.
        :returns: The commit sha, or ``None`` when the hub has no such
            branch yet.
        """
        try:
            out = self._run(
                [
                    'git', '-C', self._run_repo(run_id), 'rev-parse',
                    '--verify', '--quiet',
                    self.node_branch(run_id, node_id),
                ]
            )
        except click.ClickException:
            return None
        return out.strip() or None

    def node_branch_exists(self, run_id: str, node_id: str) -> bool:
        """
        Whether a node's branch is already on the run's hub.

        Lets a resume tell "this node never started" from "this node
        started and left work behind", so a re-run can be seeded from
        that work rather than discarding it.

        :param run_id: Pipeline run id.
        :param node_id: The node to look for.
        :returns: Whether ``pl/<run>/<node>`` exists on the hub.
        """
        repo = self._run_repo(run_id)
        if not os.path.isdir(repo):
            return False
        branch = self.node_branch(run_id, node_id)
        try:
            out = self._run(
                ['git', '-C', repo, 'branch', '--list', branch]
            )
        except click.ClickException:
            return False
        return bool(out.strip())

    def node_diff_files(
        self, run_id: str, node_id: str, *, against: str
    ) -> list[str]:
        """
        Repo-relative paths a node's branch changed vs another ref.

        Lets the orchestrator judge whether a writer ACTUALLY did the
        work, by inspection alone — no agent-authored code is executed,
        so this stays safely in the trusted plane.

        *against* is resolved on the hub, falling back to
        ``origin/<against>`` so a plain base-branch name works whether
        or not the hub carries it locally.

        :param run_id: Pipeline run id.
        :param node_id: The node whose branch to inspect.
        :param against: Ref the branch was cut from.
        :returns: Changed paths (``[]`` when the trees are identical).
        :raises click.ClickException: If neither ref resolves.
        """
        repo = self._run_repo(run_id)
        branch = self.node_branch(run_id, node_id)
        try:
            out = self._run(
                ['git', '-C', repo, 'diff', '--name-only', against, branch]
            )
        except click.ClickException:
            out = self._run(
                [
                    'git', '-C', repo, 'diff', '--name-only',
                    f'origin/{against}', branch,
                ]
            )
        return [line.strip() for line in out.splitlines() if line.strip()]

    def node_added_lines(
        self, run_id: str, node_id: str, *, against: str
    ) -> list[str]:
        """
        Lines a node's branch ADDED versus another ref.

        Content, where :meth:`node_diff_files` gives only names — so a
        gate can ask what the writer actually wrote, not merely where.
        Added lines only: what the base tree already contained is
        somebody else's decision and not this writer's to answer for.

        :param run_id: Pipeline run id.
        :param node_id: The node whose branch to inspect.
        :param against: Ref the branch was cut from.
        :returns: The added lines, without their ``+`` marker.
        :raises click.ClickException: If neither ref resolves.
        """
        repo = self._run_repo(run_id)
        branch = self.node_branch(run_id, node_id)
        argv = ['git', '-C', repo, 'diff', '--unified=0']
        try:
            out = self._run([*argv, against, branch])
        except click.ClickException:
            out = self._run([*argv, f'origin/{against}', branch])
        return [
            line[1:]
            for line in out.splitlines()
            if line.startswith('+') and not line.startswith('+++')
        ]

    def create_node_worktree(
        self,
        run_id: str,
        node_id: str,
        *,
        from_node: str | None = None,
        base_branch: str | None = None,
        replace: bool = False,
    ) -> str:
        """
        Create an isolated, self-contained clone + branch for one node.

        Each node is a standalone ``git clone`` of the run's hub repo (a
        full ``.git`` DIRECTORY, hardlinked objects), so git works
        normally inside the node's microVM mount — unlike a linked
        ``git worktree``, whose ``.git`` file points at the hub's path
        that is not mounted into the VM. The branch is seeded from
        *from_node*'s branch (inheritance) or ``origin/<base>``, then
        pushed back to the hub so downstream nodes can inherit it and
        publish/judge can find it even before any commit.

        :param run_id: Pipeline run id.
        :param node_id: This node's id (clone dir + branch tail).
        :param from_node: Upstream node whose branch seeds this one, or
            ``None`` to cut from the base branch. Its branch must be on
            the hub already (it was created + committed).
        :param base_branch: Base (used when *from_node* is ``None``);
            ``None`` uses the manager default.
        :param replace: Discard a stale clone of this node from an
            earlier attempt and re-cut it (resume). When that attempt
            left a branch behind, the new clone starts from THAT branch,
            so partial work is inherited rather than thrown away.
        :returns: The node's clone path.
        :raises click.ClickException: On a missing run or git error.
        """
        repo = self._run_repo(run_id)
        if not os.path.isdir(repo):
            raise click.ClickException(
                f'no run {run_id!r}; call create_run first'
            )
        path = self.node_worktree_path(run_id, node_id)
        if os.path.exists(path):
            if not replace:
                raise click.ClickException(f'node worktree exists: {path}')
            self._remove_under_root(path)
        branch = self.node_branch(run_id, node_id)
        if replace and self.node_branch_exists(run_id, node_id):
            # Resuming a node that already ran: its branch may carry a
            # partial commit from the attempt that failed. Cut from that
            # branch so the agent picks up its own prior work instead of
            # starting the stage over from the seed.
            start = f'origin/{branch}'
        elif from_node is not None:
            start = f'origin/{self.node_branch(run_id, from_node)}'
        else:
            start = f'origin/{base_branch or self._default_branch}'
        os.makedirs(self._nodes_dir(run_id), exist_ok=True)
        # A standalone clone (local, hardlinked objects) gives the node
        # its own real .git so git works inside the mounted VM.
        self._run(['git', 'clone', '--no-checkout', repo, path])
        self._run(['git', '-C', path, 'checkout', '-B', branch, start])
        # Register the branch on the hub so downstream inheritance,
        # judge comparison, and publish can see it before any commit.
        # A replaced node force-pushes: it is re-cutting a branch only
        # it owns, and the discarded tip is the failed attempt's.
        push = ['git', '-C', path, 'push', 'origin', branch]
        self._run([*push, '--force'] if replace else push)
        # AFTER the push, so a cache-seeding failure can never cost the
        # branch registration downstream nodes inherit from. The cache
        # entries are git-ignored build output by definition, so this
        # cannot change what the node commits.
        self.seed_build_cache(path)
        return path

    def _build_cache_dir(self) -> str | None:
        """
        Where the warm build cache lives, or ``None`` when disabled.

        Sits beside ``_metrics`` and ``_retained`` under the canonical
        root: derived state that outlives any one run, keyed by
        repository so two projects never share a cache.

        :returns: The cache directory path, or ``None``.
        """
        if not self._build_cache or not self._build_cache_key:
            return None
        return os.path.join(
            self._canonical_root, '_buildcache', self._build_cache_key
        )

    def seed_build_cache(self, worktree_path: str) -> list[str]:
        """
        Clone the warm build cache into a freshly cut node worktree.

        Every node starts from an empty tree, so every node paid a
        from-clean compile — measured at roughly two thirds of a build
        increment's wall-clock time, and identical whether the
        increment changed seven lines or seven hundred. Seeding the
        previous node's build directory turns that into an incremental
        one.

        On APFS ``cp -Rc`` is a copy-on-write CLONE: measured at 200 MB
        in under 10 ms with no space consumed until a block diverges,
        so a multi-gigabyte cache costs neither time nor disk to hand
        out. ``-c`` falls back to a real copy on a filesystem without
        clonefile, which is slower but still correct.

        STALENESS IS THE BUILD TOOL'S PROBLEM, deliberately. Cargo
        fingerprints every artifact by source hash, feature flags and
        compiler version, so a cache from another branch — or another
        toolchain — is revalidated and rebuilt where it disagrees. The
        worst case is the from-clean build we do today; there is no
        case where it silently uses the wrong artifact.

        Best-effort throughout: a cache that cannot be read or copied
        leaves the worktree exactly as it was. Seeding is an
        optimization, and an optimization that can fail a run is worse
        than no optimization.

        :param worktree_path: The node's clone directory.
        :returns: The cache entries actually seeded (for logging).
        """
        cache = self._build_cache_dir()
        if cache is None:
            return []
        seeded = []
        for name in self._build_cache:
            src = os.path.join(cache, name)
            dst = os.path.join(worktree_path, name)
            # Never overwrite: a checked-in directory of the same name
            # is the repository's, not ours to replace.
            if os.path.exists(dst) or not os.path.isdir(src):
                continue
            try:
                if not os.listdir(src):
                    continue  # empty cache entry buys nothing
                self._run(['cp', '-Rc', src, dst])
                seeded.append(name)
            except (click.ClickException, OSError):
                # A partial copy is worse than none: a half-written
                # build directory is exactly what a build tool cannot
                # revalidate its way out of.
                shutil.rmtree(dst, ignore_errors=True)
        return seeded

    def refresh_build_cache(self, worktree_path: str) -> list[str]:
        """
        Replace the warm build cache from a node that just finished.

        Called only after a stage COMPLETES, so the cache is never
        refreshed from a node that failed mid-build and could leave a
        torn build directory behind.

        Written to a temporary name and renamed into place, so a crash
        or a concurrent refresh can never leave a half-written cache
        for the next node to seed from. Two nodes finishing at once
        both refresh and the last one wins, which is harmless: either
        is a valid build of the same tree.

        Best-effort, like :meth:`seed_build_cache`.

        :param worktree_path: The completed node's clone directory.
        :returns: The cache entries actually refreshed (for logging).
        """
        cache = self._build_cache_dir()
        if cache is None:
            return []
        refreshed = []
        for name in self._build_cache:
            src = os.path.join(worktree_path, name)
            if not os.path.isdir(src):
                continue
            dst = os.path.join(cache, name)
            staged = f'{dst}.incoming.{os.getpid()}'
            try:
                os.makedirs(cache, exist_ok=True)
                shutil.rmtree(staged, ignore_errors=True)
                self._run(['cp', '-Rc', src, staged])
                previous = f'{dst}.replaced.{os.getpid()}'
                if os.path.exists(dst):
                    os.rename(dst, previous)
                os.rename(staged, dst)
                shutil.rmtree(previous, ignore_errors=True)
                refreshed.append(name)
            except (click.ClickException, OSError):
                shutil.rmtree(staged, ignore_errors=True)
        return refreshed

    def write_ignored_file(
        self, worktree_path: str, name: str, content: str
    ) -> None:
        """
        Write ``name`` into a node worktree and git-ignore it there.

        Used to hand an agent a large turn as a file it reads instead
        of an over-long paste (see runner ``_agy_deliverable``). The
        file is added to the clone's ``.git/info/exclude`` so it is
        (a) never staged by :meth:`commit_node`'s ``git add -A`` and
        (b) never shown by the settle-wait's ``git status --porcelain``
        — an un-ignored helper file would falsely satisfy the settle
        before the agent has written anything. The exclude entry is
        written BEFORE the file, so it is ignored the instant it exists.

        :param worktree_path: The node's clone directory (the agent's
            mounted working directory).
        :param name: Filename to write at the worktree root; a bare
            filename with no path separators.
        :param content: File contents.
        :raises click.ClickException: If *name* is not a bare filename.
        """
        if '/' in name or name in ('', '.', '..'):
            raise click.ClickException(
                f'ignored-file name must be a bare filename: {name!r}'
            )
        exclude = os.path.join(worktree_path, '.git', 'info', 'exclude')
        os.makedirs(os.path.dirname(exclude), exist_ok=True)
        existing = ''
        if os.path.exists(exclude):
            with open(exclude, encoding='utf-8') as fh:
                existing = fh.read()
        if name not in existing.split():
            with open(exclude, 'a', encoding='utf-8') as fh:
                if existing and not existing.endswith('\n'):
                    fh.write('\n')
                fh.write(f'{name}\n')
        with open(
            os.path.join(worktree_path, name), 'w', encoding='utf-8'
        ) as fh:
            fh.write(content)

    def write_tracked_file(
        self, worktree_path: str, relpath: str, content: str
    ) -> None:
        """
        Write a committable file at a nested path in a node worktree.

        Unlike :meth:`write_ignored_file`, the file is TRACKED — a later
        :meth:`commit_node` stages and commits it. Used to drop the plan
        of record (``docs/plans/<pipeline>.md``) onto the branch about
        to be published. Parent directories are created. The path must
        stay INSIDE the worktree: an absolute path or one escaping via
        ``..`` is rejected, so a hostile pipeline value cannot write
        elsewhere on the host.

        :param worktree_path: The node's clone directory.
        :param relpath: Repo-relative destination path (may be nested).
        :param content: File contents.
        :raises click.ClickException: If *relpath* is empty, absolute,
            or escapes the worktree.
        """
        if not relpath or os.path.isabs(relpath):
            raise click.ClickException(
                f'tracked-file path must be relative and non-empty: '
                f'{relpath!r}'
            )
        root = os.path.realpath(worktree_path)
        full = os.path.realpath(os.path.join(root, relpath))
        if full != root and os.path.commonpath([full, root]) != root:
            raise click.ClickException(
                f'tracked-file path escapes the worktree: {relpath!r}'
            )
        if full == root:
            raise click.ClickException(
                f'tracked-file path must name a file: {relpath!r}'
            )
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, 'w', encoding='utf-8') as fh:
            fh.write(content)

    def read_tracked_file(
        self, worktree_path: str, relpath: str
    ) -> str | None:
        """
        Read a committable file back out of a node worktree.

        The counterpart to :meth:`write_tracked_file`, and it exists for
        one reason: the findings ledger is APPEND-ONLY. It is a
        human-edited artifact — a person annotates status and reasoning
        on it — so the runner must read what is already on the
        branch and add to it, never regenerate it. A writer that
        rebuilt the document each run would clobber the very
        annotations it exists to hold (TASKS.md #10).

        Same path guards as the write side: an absolute path, or one
        escaping the worktree via ``..``, is refused rather than
        read, so a hostile value cannot read arbitrary host files.

        :param worktree_path: The node's clone directory.
        :param relpath: Repo-relative path to read.
        :returns: The file's contents, or ``None`` when it does
            not exist — a first run has no ledger yet, and that is
            not an error.
        :raises click.ClickException: If *relpath* is empty,
            absolute, or escapes the worktree.
        """
        if not relpath or os.path.isabs(relpath):
            raise click.ClickException(
                f'tracked-file path must be relative and non-empty: '
                f'{relpath!r}'
            )
        root = os.path.realpath(worktree_path)
        full = os.path.realpath(os.path.join(root, relpath))
        if full != root and os.path.commonpath([full, root]) != root:
            raise click.ClickException(
                f'tracked-file path escapes the worktree: {relpath!r}'
            )
        try:
            with open(full, encoding='utf-8') as fh:
                return fh.read()
        except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
            return None

    def reseed_node_worktree(
        self, run_id: str, node_id: str, from_node: str
    ) -> str:
        """
        Move a pre-warmed node's branch + tree onto an upstream's tip.

        Pre-warming boots a downstream writer's VM during planning, when
        its ``from`` upstream is still at base — so the node's clone
        is seeded from the upstream's BASE. Once the upstream node
        actually commits, this fetches that commit and ``reset --hard``s
        the node's branch (and its host-mounted working tree) onto the
        upstream tip, so the already-booted VM sees the upstream's files
        before it is driven. The node keeps its own branch name and
        commits fast-forward on the upstream (exactly one ahead), just
        like a node freshly cut from the committed upstream.

        :param run_id: Pipeline run id.
        :param node_id: The pre-warmed node to reseed.
        :param from_node: Upstream writer node whose committed branch
            tip the node is reseeded onto.
        :returns: The node's clone path.
        :raises click.ClickException: On a missing clone or git error.
        """
        path = self.node_worktree_path(run_id, node_id)
        if not os.path.isdir(path):
            raise click.ClickException(
                f'no worktree for node {node_id!r}: {path}'
            )
        upstream = self.node_branch(run_id, from_node)
        self._run(['git', '-C', path, 'fetch', 'origin', upstream])
        self._run(
            ['git', '-C', path, 'reset', '--hard', f'origin/{upstream}']
        )
        return path

    def alias_node_branch(
        self, run_id: str, alias_id: str, target_id: str
    ) -> str:
        """
        Point hub branch ``pl/<run>/<alias_id>`` at ``.../<target_id>``.

        A judge records a winner but publishes no branch of its own, so
        a downstream stage cannot seed ``from`` it. Creating the judge
        node's own branch as an alias of the selected winner on the hub
        makes the judge behave like any writer for inheritance: a later
        writer can cut its worktree from ``pl/<run>/<alias_id>`` and a
        review can mount it. Force-updates so it is idempotent.

        :param run_id: Pipeline run id.
        :param alias_id: Node id whose branch becomes the alias (the
            judge).
        :param target_id: Node id whose committed branch the alias
            points at (the selected winner).
        :returns: The alias branch name.
        :raises click.ClickException: On a missing run or git error.
        """
        repo = self._run_repo(run_id)
        if not os.path.isdir(repo):
            raise click.ClickException(
                f'no run {run_id!r}; call create_run first'
            )
        alias = self.node_branch(run_id, alias_id)
        target = self.node_branch(run_id, target_id)
        self._run(['git', '-C', repo, 'branch', '-f', alias, target])
        return alias

    def _porcelain(self, path: str) -> str:
        """A worktree's ``git status --porcelain`` (``''`` = clean)."""
        return self._run(
            ['git', '-C', path, 'status', '--porcelain']
        ).strip()

    def node_is_dirty(self, run_id: str, node_id: str) -> bool:
        """
        Whether a node's worktree holds work that is not on its branch.

        Includes untracked files: a writer that creates a new source
        file leaves it untracked until it is staged, and that file is
        every bit as much the work as an edit to an existing one.

        :param run_id: Pipeline run id.
        :param node_id: The writer node.
        :returns: ``True`` when the worktree has uncommitted changes.
        """
        path = self.node_worktree_path(run_id, node_id)
        if not os.path.isdir(path):
            return False
        return bool(self._porcelain(path))

    def wait_for_node_settle(
        self,
        run_id: str,
        node_id: str,
        *,
        timeout: float = 300.0,
        stable_window: float = 4.0,
        poll: float = 1.0,
    ) -> None:
        """
        Block until a writer node's worktree has stopped changing.

        A native-TUI writer (agy) reports its turn ``idle`` while its
        file writes are still landing — so committing the instant the
        turn driver returns would capture an empty or partial tree and
        lose the agent's work. The node's clone is a host-mounted path,
        so the orchestrator can watch it directly: poll ``git status``
        and return only once the tree is NON-empty and has been
        unchanged for *stable_window*. An empty tree never satisfies
        that, so a writer still mid-write keeps the wait open up to
        *timeout*; a writer that legitimately changed nothing simply
        waits out *timeout* and the caller then commits a clean (no-op)
        tree. Best-effort — bounded by *timeout*, so it never hangs.

        :param run_id: Pipeline run id.
        :param node_id: The writer node whose worktree to watch.
        :param timeout: Hard cap on the total wait.
        :param stable_window: Seconds the (non-empty) tree must stay
            unchanged before it is considered settled.
        :param poll: Seconds between ``git status`` checks.
        """
        path = self.node_worktree_path(run_id, node_id)
        if not os.path.isdir(path):
            return
        deadline = time.monotonic() + timeout
        last = self._porcelain(path)
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            time.sleep(poll)
            cur = self._porcelain(path)
            if cur != last:
                last, last_change = cur, time.monotonic()
                continue
            if cur and time.monotonic() - last_change >= stable_window:
                return

    def commit_node(
        self,
        run_id: str,
        node_id: str,
        *,
        message: str,
        author: str | None = None,
        committer_name: str = 'pipeline-orchestrator',
        committer_email: str = 'pipeline@localhost',
    ) -> bool:
        """
        Commit a node's clone onto its branch, then push to the hub.

        Mirrors :meth:`commit_worktree` for a pipeline node's isolated
        clone: the untrusted writer VM only edits files; the host turns
        the approved tree into a commit on ``pl/<run>/<node>`` and
        pushes it to the shared hub so downstream nodes inherit it and
        publish/judge see the committed branch.

        :param run_id: Pipeline run id.
        :param node_id: The writer node.
        :param message: Commit message.
        :param author: Optional ``"Name <email>"`` commit author (the
            writer agent), or ``None``.
        :param committer_name: Committer name.
        :param committer_email: Committer email.
        :returns: ``True`` if the HUB's branch advanced — whether the
            commit came from here or from the agent itself.
        :raises click.ClickException: On missing clone or git error.
        """
        path = self.node_worktree_path(run_id, node_id)
        if not os.path.isdir(path):
            raise click.ClickException(
                f'no worktree for node {node_id!r}: {path}'
            )
        branch = self.node_branch(run_id, node_id)
        before = self.hub_branch_tip(run_id, node_id)
        status = self._run(['git', '-C', path, 'status', '--porcelain'])
        if status.strip():
            self._run(['git', '-C', path, 'add', '-A'])
            cmd = [
                'git',
                '-C',
                path,
                '-c',
                f'user.name={committer_name}',
                '-c',
                f'user.email={committer_email}',
                'commit',
                '-m',
                message,
            ]
            if author is not None:
                cmd += ['--author', author]
            self._run(cmd)
        # Push UNCONDITIONALLY — not only when this method committed.
        # Agents run `git commit` inside their own VM, and when one has
        # already committed everything the tree is clean here, so the
        # early return skipped the push: the work stayed in the node's
        # clone and never reached the hub. Everything downstream reads
        # the HUB — the no-op guard diffs it, the judge clones it,
        # publish pushes from it — so the branch silently looked
        # untouched. Observed live: a refactor agent committed a real
        # 248/281-line change plus a build fix, and the guard, diffing
        # the hub, reported "changed 0 file(s)" three times and failed
        # the run over work sitting on disk the whole time. A push with
        # nothing to send is a cheap no-op.
        self._run(['git', '-C', path, 'push', 'origin', branch])
        return self.hub_branch_tip(run_id, node_id) != before

    def create_judge_worktree(
        self,
        run_id: str,
        judge_id: str,
        candidate_nodes: list[str],
        *,
        replace: bool = False,
    ) -> str:
        """
        Build a read-only comparison tree for a judge node.

        Creates ``<run>/nodes/<judge_id>/`` with one self-contained
        clone per candidate, in a subdir named for the candidate node,
        so a judge mounts the parent ``:ro`` and reads ``./<cand>`` for
        every competing implementation. Each subdir is a standalone
        clone (own ``.git`` DIRECTORY) so git works inside the judge's
        VM — a linked worktree's ``.git`` pointer would be broken there.

        :param run_id: Pipeline run id.
        :param judge_id: The judge node's id (the parent dir).
        :param candidate_nodes: Competing writer node ids to compare
            (each already committed + pushed to the hub).
        :returns: The judge's parent directory (the mount root).
        :raises click.ClickException: On a missing run or git error.
        """
        repo = self._run_repo(run_id)
        if not os.path.isdir(repo):
            raise click.ClickException(f'no run {run_id!r}')
        parent = self.node_worktree_path(run_id, judge_id)
        if os.path.exists(parent):
            if not replace:
                raise click.ClickException(
                    f'judge worktree exists: {parent}'
                )
            self._remove_under_root(parent)
        os.makedirs(parent, exist_ok=True)
        for cand in candidate_nodes:
            cand = _validate_name(cand, 'node id')
            sub = os.path.join(parent, cand)
            branch = self.node_branch(run_id, cand)
            self._run(['git', 'clone', '--no-checkout', repo, sub])
            self._run(['git', '-C', sub, 'checkout', branch])
        return parent

    def merge_node_into(
        self,
        run_id: str,
        *,
        source_node: str,
        target_node: str,
        message: str | None = None,
    ) -> None:
        """
        Merge one node's branch into another node's branch.

        For a DAG join (orchestrator combines independent branches).
        Runs in *target_node*'s clone: fetches the hub, merges the
        source node's branch, and pushes the result back to the hub.

        :param run_id: Pipeline run id.
        :param source_node: Node whose branch is merged in (must already
            be on the hub).
        :param target_node: Node whose branch receives the merge (its
            clone must exist).
        :param message: Merge commit message; ``None`` uses git default.
        :raises click.ClickException: On a missing clone or a merge
            conflict (git's output is surfaced).
        """
        path = self.node_worktree_path(run_id, target_node)
        if not os.path.isdir(path):
            raise click.ClickException(
                f'no worktree for node {target_node!r}: {path}'
            )
        self._run(['git', '-C', path, 'fetch', 'origin'])
        cmd = [
            'git',
            '-C',
            path,
            '-c',
            'user.name=pipeline-orchestrator',
            '-c',
            'user.email=pipeline@localhost',
            'merge',
            '--no-ff',
        ]
        if message is not None:
            cmd += ['-m', message]
        cmd.append(f'origin/{self.node_branch(run_id, source_node)}')
        self._run(cmd)
        self._run(
            [
                'git',
                '-C',
                path,
                'push',
                'origin',
                self.node_branch(run_id, target_node),
            ]
        )

    def publish_node(
        self,
        run_id: str,
        node_id: str,
        repo_url: str,
        *,
        title: str,
        body: str,
        base_branch: str | None = None,
        base_fallback: str | None = None,
        remote_branch: str | None = None,
        draft: bool = True,
        open_pr: bool = True,
    ) -> str:
        """
        Push a node's branch to *repo_url* (+ optional draft PR).

        Generalizes :meth:`publish_swarm` to a pipeline node branch:
        pushes ``pl/<run>/<node>`` from the shared hub repo (which holds
        the node's committed branch) to *remote_branch* (default
        ``pipeline/<run>``), then — in GitHub mode — opens a draft PR.
        Never pushes onto the base branch.

        :param run_id: Pipeline run id.
        :param node_id: The node whose branch to publish (the selected
            winner or final writer).
        :param repo_url: Push target (GitHub URL for a PR, else any
            remote / local path).
        :param title: PR title (GitHub mode).
        :param body: PR body (GitHub mode).
        :param base_branch: PR base; ``None`` uses the default branch.
        :param base_fallback: Base to use when *base_branch* no longer
            exists on the remote — the stacked case, where the previous
            module's branch was merged and DELETED before this one
            published. ``None`` uses the default branch.
        :param remote_branch: Destination branch on the remote; ``None``
            uses ``pipeline/<run>``.
        :param draft: Open the PR as a draft (GitHub mode).
        :param open_pr: Open a GitHub PR after the push; ``False`` =
            local mode (push only).
        :returns: The PR URL (GitHub mode) or a push summary (local).
        :raises click.ClickException: On a base collision or git error.
        """
        run_id = _validate_name(run_id, 'run id')
        repo = self._run_repo(run_id)
        if not os.path.isdir(repo):
            raise click.ClickException(f'no run {run_id!r}: {repo}')
        src = self.node_branch(run_id, node_id)
        base = base_branch or self._default_branch
        if (
            open_pr
            and base_branch
            and base_branch != self._default_branch
            and not self._remote_has_branch(repo_url, base_branch)
        ):
            base = base_fallback or self._default_branch
            click.echo(
                f'[publish] base {base_branch!r} is no longer on the '
                f'remote (merged and deleted?) — opening against '
                f'{base!r} instead.'
            )
        dst = remote_branch or f'pipeline/{run_id}'
        if dst == base:
            raise click.ClickException(
                f'refusing to publish onto base branch {base!r}'
            )
        self._run_publish(
            [
                'git',
                '-C',
                repo,
                'push',
                repo_url,
                f'{src}:refs/heads/{dst}',
            ]
        )
        if not open_pr:
            return (
                f'Pushed {dst} to {repo_url}. Review and merge it into '
                f'{base} when ready (e.g. `git merge {dst}`).'
            )
        cmd = _pr_create_command(
            github_slug(repo_url),
            head=dst,
            base=base,
            title=title,
            body=body,
            draft=draft,
        )
        return self._run_publish(cmd).strip()

    def _remote_has_branch(self, repo_url: str, branch: str) -> bool:
        """
        Whether *branch* still exists on the remote.

        Asked before stacking a pull request onto another module's
        branch: that branch is routinely merged and DELETED between one
        module publishing and the next, and opening a request against a
        base that is gone fails outright.

        A lookup that cannot be performed at all counts as absent. That
        is the safe direction — falling back to the repo's base branch
        always yields a valid (if noisier) request, whereas assuming
        the base is still there loses the publish entirely.

        :param repo_url: The push target.
        :param branch: Branch name to look for.
        :returns: Whether the remote has it.
        """
        try:
            out = self._run_publish(
                ['git', 'ls-remote', '--heads', repo_url,
                 f'refs/heads/{branch}']
            )
        except click.ClickException:
            return False
        return bool(out.strip())

    def _remove_under_root(self, path: str) -> None:
        """
        Delete *path*, refusing anything outside ``worktree_root``.

        :param path: Directory to remove.
        :raises click.ClickException: If it resolves outside the root.
        """
        root = os.path.realpath(self._worktree_root)
        real = os.path.realpath(path)
        if real != path and not real.startswith(root + os.sep):
            raise click.ClickException(
                f'refusing to remove {path!r}: resolves outside {root!r}'
            )
        if os.path.isdir(path):
            shutil.rmtree(path)

    def dispose_node_worktrees(
        self, run_id: str, node_ids: list[str]
    ) -> int:
        """
        Remove the named node clones; their BRANCHES are untouched.

        A published chunk's clones are dead weight: every writer's tree
        is committed to a hub branch and pushed, the next chunk seeds
        from the hub rather than from any clone, and a resume skips a
        completed chunk wholesale. Keeping them makes disk cost
        CUMULATIVE across a campaign — 2.2-26 GB per writer node for a
        compiled language, which is what ran a host out of space two
        modules into a six-module run.

        Names are passed explicitly rather than matched by prefix: chunk
        ids ``core`` and ``core-extra`` share one, so a prefix sweep
        would delete a later chunk's live worktrees.

        :param run_id: Pipeline run id.
        :param node_ids: Node ids whose clones to remove; unknown or
            already-gone names are skipped.
        :returns: How many clones were removed.
        """
        removed = 0
        for node_id in node_ids:
            path = os.path.join(
                self._nodes_dir(run_id), _validate_name(node_id, 'node id')
            )
            if not os.path.isdir(path):
                continue
            try:
                self._remove_under_root(path)
            except click.ClickException:
                continue
            removed += 1
        return removed

    def metrics_path(self, run_id: str) -> str:
        """
        Where a run's disk measurements are recorded.

        :param run_id: Pipeline run id.
        :returns: The run's JSONL metrics file.

        Under ``canonical_root`` for the same reason as the retained
        bundles: a COMPLETED run deletes its own run directory, so a
        record kept there would survive only the runs that failed —
        exactly the bias that made the planning-session record useless
        (TASKS.md #30). Nothing under ``canonical_root`` is removed.
        """
        return os.path.join(
            self._canonical_root,
            '_metrics',
            f'{_validate_name(run_id, "run id")}.jsonl',
        )

    def retained_root(self) -> str:
        """
        :returns: The directory holding retained losing branches.

        Deliberately under ``canonical_root`` and NOT under
        ``worktree_root``: everything the launcher deletes goes through
        :meth:`_remove_under_root`, which refuses anything outside
        ``worktree_root``, so nothing here can be swept by a run
        teardown. A run id may legally contain ``_``, so a run called
        ``_retained`` would otherwise have collided with this directory
        and taken every retained bundle with it at teardown.
        """
        return os.path.join(self._canonical_root, '_retained')

    def retained_bundle_path(self, run_id: str, node_id: str) -> str:
        """
        :param run_id: Pipeline run id.
        :param node_id: The node whose branch is retained.
        :returns: Where that node's bundle lives.
        """
        return os.path.join(
            self.retained_root(),
            _validate_name(run_id, 'run id'),
            f'{_validate_name(node_id, "node id")}.bundle',
        )

    def retain_node_bundle(
        self, run_id: str, node_id: str, *, against: str
    ) -> str | None:
        """
        Preserve a node's branch as a git bundle that outlives the run.

        For the implementation that LOSES. Two writers build the same
        frozen contract on isolated branches and only the winner
        publishes, so without this the loser — complete, reviewed, and
        test-passing — is deleted with the run hub. That is the most
        valuable comparison data the pipeline produces and the run dir
        holds the only copy (TASKS.md #32).

        A DELTA bundle against *against*, not a full-history one: it is
        roughly twenty times smaller and restores just as well, because
        the base commit it requires is by definition already in the repo
        anyone would compare it against. Restore with::

            git fetch <bundle> refs/heads/<branch>:refs/heads/<name>

        *against* is resolved on the hub, falling back to
        ``origin/<against>``, mirroring :meth:`node_diff_files`.

        :param run_id: Pipeline run id.
        :param node_id: The node whose branch to retain.
        :param against: Ref the branch was cut from, e.g. ``"main"``.
        :returns: The bundle path, or ``None`` when the branch holds
            nothing the base does not (git refuses an empty bundle, and
            a writer that committed nothing is not worth a file).
        :raises click.ClickException: If git fails for any other reason.
        """
        repo = self._run_repo(run_id)
        branch = self.node_branch(run_id, node_id)
        path = self.retained_bundle_path(run_id, node_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        failure: click.ClickException | None = None
        for base in (against, f'origin/{against}'):
            try:
                # The range alone names the ref: a bundle built from
                # `base..branch` still records refs/heads/<branch>, so
                # the restoring side gets a usable branch name for free.
                self._run(
                    ['git', '-C', repo, 'bundle', 'create', path,
                     f'{base}..{branch}']
                )
                return path
            except click.ClickException as exc:
                # An empty range is not a failure worth raising: the
                # writer simply produced nothing beyond the base.
                if 'empty bundle' in str(exc).lower():
                    return None
                failure = exc
        raise failure if failure is not None else click.ClickException(
            f'could not bundle {branch!r} against {against!r}'
        )

    def dispose_run(self, run_id: str) -> None:
        """
        Remove an entire pipeline run (all node clones + hub clone).

        Only a path strictly under ``worktree_root`` is removed, so a
        crafted id can never delete elsewhere.

        :param run_id: Pipeline run id.
        :raises click.ClickException: If the resolved path escapes the
            root.
        """
        self._remove_under_root(self.run_dir(run_id))


# ── CLI (the coordinator invokes this via sys_os_shell) ───────────


@click.group()
@click.option(
    '--canonical-root',
    envvar='OMNI_SBX_CANONICAL_ROOT',
    required=True,
    help='Host dir holding <name>.git bare mirrors.',
)
@click.option(
    '--worktree-root',
    envvar='OMNI_SBX_WORKTREE_ROOT',
    required=True,
    help='Host dir for per-swarm worktrees (match sbx.worktree_root).',
)
@click.option(
    '--default-branch',
    envvar='OMNI_SBX_DEFAULT_BRANCH',
    default='main',
    show_default=True,
    help='Base branch cut from and merged into.',
)
@click.pass_context
def cli(
    ctx: click.Context,
    canonical_root: str,
    worktree_root: str,
    default_branch: str,
) -> None:
    """Manage per-swarm git worktrees (trusted plane)."""
    ctx.obj = WorktreeManager(
        canonical_root=canonical_root,
        worktree_root=worktree_root,
        default_branch=default_branch,
    )


@cli.command('ensure-canonical')
@click.option('--repo-url', required=True)
@click.pass_obj
def _ensure_canonical(mgr: WorktreeManager, repo_url: str) -> None:
    """Create-or-refresh the canonical bare mirror; print its path."""
    click.echo(mgr.ensure_canonical(repo_url))


@cli.command('create-swarm')
@click.option('--swarm-id', required=True)
@click.option('--repo-url', required=True)
@click.option('--base-branch', default=None)
@click.pass_obj
def _create_swarm(
    mgr: WorktreeManager,
    swarm_id: str,
    repo_url: str,
    base_branch: str | None,
) -> None:
    """Cut a worktree on task/<swarm-id>; print its path."""
    click.echo(mgr.create_swarm_worktree(swarm_id, repo_url, base_branch))


@cli.command('commit-swarm')
@click.option('--swarm-id', required=True)
@click.option('--message', required=True)
@click.option(
    '--author', default=None, help="Commit author 'Name <email>' (the coder)."
)
@click.pass_obj
def _commit_swarm(
    mgr: WorktreeManager,
    swarm_id: str,
    message: str,
    author: str | None,
) -> None:
    """Commit the worktree's approved state on the host."""
    made = mgr.commit_worktree(swarm_id, message=message, author=author)
    click.echo('committed' if made else 'nothing-to-commit')


@cli.command('publish-swarm')
@click.option('--swarm-id', required=True)
@click.option('--repo-url', required=True)
@click.option('--title', required=True)
@click.option('--body', default='')
@click.option('--base-branch', default=None)
@click.option('--ready', is_flag=True, help='Open ready (not draft).')
@click.option(
    '--no-pr',
    is_flag=True,
    help='Local mode: push the branch only, no GitHub PR.',
)
@click.pass_obj
def _publish_swarm(
    mgr: WorktreeManager,
    swarm_id: str,
    repo_url: str,
    title: str,
    body: str,
    base_branch: str | None,
    ready: bool,
    no_pr: bool,
) -> None:
    """Push task/<swarm-id>; open a draft PR (or --no-pr for local)."""
    click.echo(
        mgr.publish_swarm(
            swarm_id,
            repo_url,
            title=title,
            body=body,
            base_branch=base_branch,
            draft=not ready,
            open_pr=not no_pr,
        )
    )


def main() -> None:
    """Console-script entry point for ``omni-sbx-worktrees``."""
    cli()


if __name__ == '__main__':
    main()
