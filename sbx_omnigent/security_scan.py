"""The scan stage's turn, and reading what claude-security found.

A ``scans:`` stage asks a Claude agent, in a writable clone of the
branch about to ship, to run the claude-security plugin over the
module's changes. The plugin writes ``CLAUDE-SECURITY-<timestamp>/``
into the clone, with one JSON finding per line in
``CLAUDE-SECURITY-RESULTS.jsonl``. That file, not the agent's reply, is
what the stage acts on: the reply a session captures can be the
plugin's opening note rather than its summary. Critical and High
findings block the module; Medium and Low are recorded and filed.
"""

from __future__ import annotations

import errno
import json
import os
import re
import stat
from collections.abc import Sequence
from dataclasses import dataclass

#: The plugin's results file inside each report directory.
RESULTS_NAME = 'CLAUDE-SECURITY-RESULTS.jsonl'

#: The report directories' prefix; a timestamp follows, so names sort
#: oldest first.
_REPORT_DIR_PREFIX = 'CLAUDE-SECURITY-'

#: A report folder's name. Anything else is not the plugin's, and the
#: name becomes a path component of the copy, so it stays plain.
_REPORT_DIR_RE = re.compile(r'CLAUDE-SECURITY-[0-9A-Za-z][0-9A-Za-z._-]*')

#: The files kept from a report folder: the results as Markdown, JSONL
#: and SARIF, and the revision stamp naming the commit scanned. Its own
#: ``.gitignore`` (``*``) is left behind, or the copy would be ignored.
_KEPT_RE = re.compile(
    r'CLAUDE-SECURITY-(?:RESULTS\.(?:md|jsonl|sarif)'
    r'|REVISION-[0-9a-f]{12}(?:-dirty)?\.json)'
)

#: The largest report file read; a larger one fails the scan.
MAX_FILE_BYTES = 16 * 1024 * 1024

#: The plugin's severities, most severe first.
SEVERITIES = ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')

#: Severities that send the module back to its writer.
BLOCKING = frozenset({'CRITICAL', 'HIGH'})

#: The plugin's tiers.
EFFORTS = ('low', 'medium', 'high')

#: A commit id, short or full.
_COMMIT_RE = re.compile(r'[0-9a-f]{7,40}')

#: Fields every finding must carry for the stage to act on it.
_REQUIRED = ('id', 'title', 'file', 'line', 'severity')


class ScanReportError(Exception):
    """The scan's results cannot be read."""


@dataclass(frozen=True)
class ScanFinding:
    """
    One finding from the plugin's JSONL.

    :param id: The plugin's id within this scan, e.g. ``"F1"``.
    :param severity: ``CRITICAL``, ``HIGH``, ``MEDIUM`` or ``LOW``.
    :param title: One-line title.
    :param category: e.g. ``"SQL Injection"``.
    :param file: Repo-relative path.
    :param line: Line number.
    :param description: What is wrong.
    :param exploit_scenario: How it is exploited.
    :param recommendation: How to fix it.
    :param cwe_id: e.g. ``"CWE-89"``.
    """

    id: str
    severity: str
    title: str
    category: str
    file: str
    line: int
    description: str
    exploit_scenario: str
    recommendation: str
    cwe_id: str

    @property
    def blocks(self) -> bool:
        """Whether it sends the module back to its writer."""
        return self.severity in BLOCKING

    @property
    def label(self) -> str:
        """The issue label for it, e.g. ``"security-medium"``."""
        return f'security-{self.severity.lower()}'


def instruction(*, base: str, effort: str) -> str:
    """
    The scan turn: the plugin over the changes since *base*.

    Names the cost in so many words, which is what the plugin takes as
    the user's go-ahead instead of asking (an unattended session cannot
    answer).

    :param base: The commit the module started from.
    :param effort: ``low``, ``medium`` or ``high``.
    :returns: The turn text.
    :raises ValueError: If *base* is not a commit id or *effort* is not
        a tier.
    """
    if not _COMMIT_RE.fullmatch(base):
        raise ValueError(f'a scan base must be a commit id, got {base!r}')
    if effort not in EFFORTS:
        raise ValueError(
            f'a scan effort must be one of {", ".join(EFFORTS)}, got '
            f'{effort!r}'
        )
    return (
        f'/claude-security:claude-security scan changes --base {base} at '
        f'{effort} effort and do not prompt me. I know that it may use a '
        f'lot of time and tokens and I approve that.'
    )


@dataclass(frozen=True)
class ScanReport:
    """
    One scan's report folder, read out of the clone it was written in.

    :param name: The folder's name, ``CLAUDE-SECURITY-<timestamp>``.
    :param files: ``(file name, text)`` for each kept file: the results
        as Markdown, JSONL and SARIF, and the revision stamp.
    """

    name: str
    files: tuple[tuple[str, str], ...]

    @property
    def stamp(self) -> str:
        """The folder's timestamp, e.g. ``"20261114-120000"``."""
        return self.name[len(_REPORT_DIR_PREFIX):]

    @property
    def results(self) -> str:
        """The JSONL every finding is read from."""
        return dict(self.files)[RESULTS_NAME]


def collect_report(worktree: str) -> ScanReport | None:
    """
    Read the newest scan's report folder out of *worktree*.

    The clone is the agent's to write, so nothing in it is trusted to
    be what its name says: a symlinked folder or file, or one that is
    not a regular file, is refused rather than followed, since a link
    could point at any file on the host and it would be committed.

    :param worktree: The clone the scan ran in.
    :returns: The report, or ``None`` if no folder holds results.
    :raises ScanReportError: On a symlink, a file that is not regular,
        too large or not UTF-8, or one that cannot be read.
    """
    try:
        entries = sorted(
            (entry for entry in os.scandir(worktree)
             if _REPORT_DIR_RE.fullmatch(entry.name)),
            key=lambda entry: entry.name,
        )
    except OSError:
        return None
    for entry in reversed(entries):
        if entry.is_symlink():
            raise ScanReportError(
                f'{entry.name} is a symlink; a scan report is not read '
                f'through one'
            )
        if not entry.is_dir(follow_symlinks=False):
            continue
        report = _read_folder(entry.path, entry.name)
        if report is not None:
            return report
    return None


def _read_folder(path: str, name: str) -> ScanReport | None:
    """The kept files in one report folder; ``None`` without results."""
    try:
        dir_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ScanReportError(f'cannot open {name}: {exc}') from exc
    try:
        names = sorted(n for n in os.listdir(dir_fd) if _KEPT_RE.fullmatch(n))
        if RESULTS_NAME not in names:
            return None
        files = tuple(
            (file_name, _read_file(dir_fd, f'{name}/{file_name}', file_name))
            for file_name in names
        )
    finally:
        os.close(dir_fd)
    return ScanReport(name=name, files=files)


def _read_file(dir_fd: int, label: str, file_name: str) -> str:
    """One regular file under *dir_fd* as text; anything else raises."""
    try:
        fd = os.open(
            file_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=dir_fd,
        )
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise ScanReportError(
                f'{label} is a symlink; a scan report is not read through '
                f'one'
            ) from exc
        raise ScanReportError(f'cannot read {label}: {exc}') from exc
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ScanReportError(f'{label} is not a regular file')
        data = handle.read(MAX_FILE_BYTES + 1)
    if len(data) > MAX_FILE_BYTES:
        raise ScanReportError(
            f'{label} is larger than {MAX_FILE_BYTES} bytes'
        )
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise ScanReportError(f'{label} is not UTF-8: {exc}') from exc


def parse_report(text: str) -> tuple[ScanFinding, ...]:
    """
    Every finding in a scan's JSONL.

    :param text: The file's text; empty means no findings.
    :returns: The findings, in the file's order.
    :raises ScanReportError: If a line is not a finding the stage can
        act on.
    """
    out: list[ScanFinding] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except ValueError as exc:
            raise ScanReportError(f'line {number} is not JSON: {exc}') from exc
        if not isinstance(raw, dict):
            raise ScanReportError(f'line {number} is not a finding')
        missing = [key for key in _REQUIRED if key not in raw]
        if missing:
            raise ScanReportError(
                f'line {number} has no {", ".join(missing)}'
            )
        severity = str(raw['severity']).strip().upper()
        if severity not in SEVERITIES:
            raise ScanReportError(
                f'line {number} has an unknown severity '
                f'{raw["severity"]!r}'
            )
        line_no = raw['line']
        if isinstance(line_no, bool) or not isinstance(line_no, int):
            raise ScanReportError(f'line {number} has no line number')
        out.append(ScanFinding(
            id=str(raw['id']),
            severity=severity,
            title=str(raw['title']),
            category=str(raw.get('category') or ''),
            file=str(raw['file']),
            line=line_no,
            description=str(raw.get('description') or ''),
            exploit_scenario=str(raw.get('exploit_scenario') or ''),
            recommendation=str(raw.get('recommendation') or ''),
            cwe_id=str(raw.get('cwe_id') or ''),
        ))
    return tuple(out)


def render_blocking(findings: Sequence[ScanFinding]) -> str:
    """
    The findings a writer must fix, each with where, how and the fix.

    :param findings: The blocking findings.
    :returns: Markdown.
    """
    return '\n\n'.join(_render_one(f) for f in findings)


def render_defect(finding: ScanFinding) -> str:
    """
    One non-blocking finding as a single line, tagged with its label.

    :param finding: A Medium or Low finding.
    :returns: e.g. ``"[security-medium] Verbose errors — app.py:16"``
        and the rest of the finding on the same line.
    """
    cwe = f', {finding.cwe_id}' if finding.cwe_id else ''
    parts = [
        f'[{finding.label}] {finding.title} — {finding.file}:{finding.line}'
        f' ({finding.category}{cwe}).',
        finding.description,
    ]
    if finding.recommendation:
        parts.append(f'Fix: {finding.recommendation}')
    return ' '.join(' '.join(p.split()) for p in parts if p)


def render_report(
    findings: Sequence[ScanFinding], *, base: str, effort: str, round_no: int
) -> str:
    """
    The whole scan as the record a reader sees.

    :param findings: Everything the scan found.
    :param base: The commit the scan diffed from.
    :param effort: The tier it ran at.
    :param round_no: Which scan round this was.
    :returns: Markdown.
    """
    head = (
        f'claude-security scan, round {round_no}: the changes since '
        f'`{base}`, at {effort} effort.'
    )
    if not findings:
        return f'{head}\n\nThe scan reported no findings.'
    counts = ', '.join(
        f'{sum(1 for f in findings if f.severity == s)} {s.lower()}'
        for s in SEVERITIES
        if any(f.severity == s for f in findings)
    )
    return f'{head}\n\n{len(findings)} finding(s): {counts}.\n\n' + (
        '\n\n'.join(_render_one(f) for f in findings)
    )


def _render_one(finding: ScanFinding) -> str:
    cwe = f' ({finding.cwe_id})' if finding.cwe_id else ''
    lines = [
        f'### {finding.id} — {finding.severity} — {finding.category}: '
        f'{finding.title}',
        f'`{finding.file}:{finding.line}`{cwe}',
    ]
    if finding.description:
        lines.append(finding.description)
    if finding.exploit_scenario:
        lines.append(f'Exploit: {finding.exploit_scenario}')
    if finding.recommendation:
        lines.append(f'Fix: {finding.recommendation}')
    return '\n\n'.join(lines)
