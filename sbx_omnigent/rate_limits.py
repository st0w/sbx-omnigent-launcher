"""Recognise an LLM usage or rate limit in a turn's text.

A turn that ended because the model was throttled should be reported
as a possible rate limit, not read as a bug. This is text matching
only, shared by the swarm CLI and the pipeline runner's failure notes.
"""

from __future__ import annotations

#: Lowercase substrings that mark an LLM usage/rate limit rather than a
#: task failure. Matched against a turn's error text, its
#: ``last_task_error``, and (as a weak signal) the agent's own reply, so
#: a turn that ended because the model was throttled is flagged instead
#: of read as a bug. Deliberately broad — a false positive only adds a
#: "might be a rate limit" hint, it never fails a good turn.
_RATE_LIMIT_MARKERS: frozenset[str] = frozenset(
    {
        'rate limit',
        'rate-limit',
        'rate_limit',
        'usage limit',
        'usage-limit',
        'usage cap',
        'quota',
        'too many requests',
        'http 429',
        ' 429',
        'overloaded',
        'insufficient_quota',
        'resets at',
        'try again later',
        'capacity',
    }
)


def looks_like_rate_limit(*texts: str | None) -> bool:
    """
    Whether any of *texts* reads like an LLM usage/rate-limit signal.

    A best-effort heuristic (see :data:`_RATE_LIMIT_MARKERS`) so callers
    can warn "this may be a rate limit, not a bug" when a turn ends
    early, empty, or failed. Case-insensitive; ``None`` entries ignored.

    :param texts: Candidate strings (error, ``last_task_error``, reply).
    :returns: ``True`` if any contains a known rate-limit marker.
    """
    for text in texts:
        if not text:
            continue
        low = text.lower()
        if any(marker in low for marker in _RATE_LIMIT_MARKERS):
            return True
    return False
