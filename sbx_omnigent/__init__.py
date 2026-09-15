"""Docker Sandboxes (``sbx``) provider for Omnigent managed hosts.

This package adds ``sbx`` as a managed-sandbox provider WITHOUT
modifying the Omnigent source tree. See
:mod:`sbx_omnigent.entrypoint` for how the provider is wired in, and
:mod:`sbx_omnigent.launcher` for the launcher implementation.

Deliberately imports nothing. Several submodules are CLIs run as
``python -m sbx_omnigent.<module>``, and anything this file imports is
already loaded by the time runpy executes that module — so it runs a
second time as ``__main__``, with its own copy of every global and
class, and runpy warns on every start. The agy harvester is launched
exactly that way in production. Import from the submodule instead:
``from sbx_omnigent.launcher import SbxLauncher``.

``tests/test_module_entrypoints.py`` keeps this file empty.
"""
