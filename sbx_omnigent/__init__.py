"""Docker Sandboxes (``sbx``) provider for Omnigent managed hosts.

This package adds ``sbx`` as a managed-sandbox provider WITHOUT
modifying the Omnigent source tree. See
:mod:`sbx_omnigent.entrypoint` for how the provider is wired in, and
:mod:`sbx_omnigent.launcher` for the launcher implementation.
"""

from sbx_omnigent.launcher import SbxLauncher

__all__ = ['SbxLauncher']
