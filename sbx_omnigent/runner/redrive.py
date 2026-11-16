"""The runner's call to re-drive a writer, as collaborators see it."""

from __future__ import annotations

from typing import Protocol


class RedriveWriter(Protocol):
    """Hands a writer stage findings to fix, and waits for its turn."""

    def __call__(
        self,
        stage_id: str,
        findings: str,
        *,
        message: str | None = None,
        instruction: str | None = None,
    ) -> None:
        """
        Re-drive *stage_id* to fix *findings*.

        :param stage_id: The writer stage.
        :param findings: What it must fix.
        :param message: The commit message for its fix.
        :param instruction: The whole turn, when the default fix
            instruction around *findings* is not the right one.
        """
