"""The environment note every builder and reviewer is given."""

from __future__ import annotations

from sbx_omnigent import pipeline


def setup_block(config: pipeline.PipelineConfig) -> str:
    """
    The environment/bootstrap note, or ``''`` when unconfigured.

    Relayed to every builder and reviewer. A VM that lacks a
    compiler is exactly where an agent stops verifying and starts
    guessing — observed live, reviewers reported "no toolchain in
    this environment" and then approved by reading the diff, while
    the implementers in identical VMs had installed one and
    compiled. Saying what to install, and that egress allows it,
    removes the excuse.

    :param config: The parsed pipeline.
    :returns: The note, or ``''`` when unconfigured.
    """
    if not config.setup:
        return ''
    return (
        '\n\nEnvironment — prepare your VM before you start (it is '
        'a fresh sandbox, and outbound access for these steps is '
        f'already allowed):\n{config.setup}'
    )
