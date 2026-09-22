"""Defaults shared by the launcher, the runner and the agy tools.

Kept in a module that imports nothing from this package, so any module
can import them at the top level. They used to live in
:mod:`sbx_omnigent.launcher`, which imports ``agy``; ``agy`` and the
runner therefore had to import them inside a function to avoid a
cycle.
"""

from __future__ import annotations

from collections.abc import Mapping

#: Prebaked Omnigent host image (multi-arch: linux/amd64 +
#: linux/arm64, so it runs natively on Apple Silicon). Pin to
#: ``:vX.Y.Z`` or ``:sha-<short>`` in config to match your server
#: version and avoid host<->server protocol skew.
DEFAULT_HOST_IMAGE = 'ghcr.io/omnigent-ai/omnigent-host:latest'

#: Curated per-VM network allowlist applied to managed VMs when
#: ``sbx.egress_allow`` is unset — the "reasonable baseline" so agents
#: work out of the box: the LLM endpoints the coding harness needs plus
#: common trusted package registries. A config list REPLACES this with
#: a stricter/custom set. The Omnigent dial-back is added automatically
#: on top (see :meth:`SbxLauncher._apply_egress`), so it is not listed.
DEFAULT_EGRESS_ALLOW: tuple[str, ...] = (
    # Claude coding harness (claude-native).
    'api.anthropic.com',
    'statsig.anthropic.com',
    'downloads.claude.ai',
    # Trusted package registries (coder builds / dependency installs).
    'registry.npmjs.org',
    'npmjs.org',
    'pypi.org',
    'files.pythonhosted.org',
    # uv, which every Python project here installs in its verify
    # setup. sbx's own default-package-managers bundle allows
    # `astral.sh:443` with no wildcard, but the installer 301s to
    # `releases.astral.sh` — so
    # `curl -LsSf https://astral.sh/uv/install.sh` yields a 125-byte
    # 403 page rather than a script, and uv never installs. Observed
    # live as a verify gate that could not run at all, which fails a
    # whole chunk as INFRASTRUCTURE after every review has already
    # passed.
    '**.astral.sh',
    # OSV, which `uv audit` / `cargo audit` query for advisories.
    # Without it the audit resolves the lockfile, fails the lookup
    # with a 403, and reports that it could not verify — which a
    # reviewer then files as a finding. Fifteen issues in one m0
    # campaign said exactly that.
    'api.osv.dev',
    # Debian apt, on PORT 80. sbx's own default-os-packages bundle
    # allows **.debian.org:443 but — unlike its Ubuntu entries, which
    # list :80 explicitly — never :80, and the host image's sources are
    # http://deb.debian.org. Without this every apt call is denied, so
    # an agent cannot install a toolchain: observed live as a reviewer
    # burning its whole turn hunting for a cargo that could never be
    # installed, then returning no VERDICT. Port 80 is safe here —
    # packages are GPG-signed, so only which packages are fetched is
    # disclosed, not their integrity.
    'deb.debian.org:80',
)

#: The :data:`DEFAULT_EGRESS_ALLOW` entries that exist only to cover
#: gaps in sbx's own shipped bundles, each with the failure it
#: prevents. An explicit ``sbx.egress_allow`` replaces the default, so
#: a custom list loses these without a word; server startup names any
#: it omits (#40).
SBX_BUNDLE_GAP_HOSTS: Mapping[str, str] = {
    '**.astral.sh': (
        "uv's installer redirects to releases.astral.sh, so without it "
        'uv never installs and a verify gate that needs uv cannot run'
    ),
    'api.osv.dev': (
        'uv audit and cargo audit cannot reach their advisory database, '
        'and reviewers report that they could not verify'
    ),
    'deb.debian.org:80': (
        "every apt call is denied (the image's apt sources are plain "
        'http), so an agent cannot install a toolchain'
    ),
}
