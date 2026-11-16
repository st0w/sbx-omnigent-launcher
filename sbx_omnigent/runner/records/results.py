"""The outcome of a run and each of its nodes, and a chunk's span."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

#: A git object id, the only thing let into a link in a PR description.
_COMMIT_RE = re.compile(r'[0-9a-f]{7,64}')


@dataclass(frozen=True)
class ChunkSpan:
    """
    What a chained chunk's pull request adds on the previous chunk.

    :param chunk: The chunk publishing.
    :param prior: The chunk it is built on.
    :param prior_pr: That chunk's PR URL, or ``''``.
    :param start: The commit this chunk was built on, or ``''``.
    :param end: The commit this chunk's PR pushed, or ``''``.
    :param slug: ``owner/repo`` the PRs are on.
    :param base: The branch both PRs target.
    """

    chunk: str
    prior: str
    prior_pr: str
    start: str
    end: str
    slug: str
    base: str


@dataclass
class NodeResult:
    """Execution record for one pipeline node."""

    node_id: str
    kind: str
    branch: str | None = None
    worktree: str | None = None
    session: str | None = None
    output: str = ''
    verdict: str | None = None
    selected: str | None = None


@dataclass
class RunResult:
    """Outcome of a pipeline run."""

    run_id: str
    status: str
    published: str | None = None
    final_node: str | None = None
    blocked_stage: str | None = None
    nodes: dict[str, NodeResult] = field(default_factory=dict)
    bindings: list[dict[str, str]] = field(default_factory=list)
