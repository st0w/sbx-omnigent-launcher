"""Dispute replies, for tests of how a dispute is heard."""

#: The reply from the live run in #15, verbatim in shape: the writer
#: did exactly what its template asked and the orchestrator could not
#: hear it. The marker is at the END of a heading, so
#: :data:`~sbx_omnigent.runner.parsing.verdicts._DISPUTE_RE` finds
#: nothing.
NEAR_MISS_REPLY = """\
## 1. Assumed-role attribution — **DISPUTED**

This finding cannot be satisfied without editing a frozen test
assertion, so per the rules I left the code alone and am reporting it
rather than obeying it.
"""
