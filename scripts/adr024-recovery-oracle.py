#!/usr/bin/env python3
"""Decision-table examples for review; does not open files or implement recovery.

Run: python3 scripts/adr024-recovery-oracle.py
Inputs are observations supplied by a test, not facts established by this model.
"""
import unittest
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Observed:
    access_ok: bool = True
    empty: bool = False
    lock_ok: bool = True
    unsafe_entry: bool = False
    layout: str = "candidate"  # candidate, legacy, mixed, unknown
    authority: str = "valid"  # valid, missing, invalid
    temporary: bool = False
    segments_match: bool = True
    certified_valid: bool = True
    active_suffix: bool = False


def classify(s):
    """Return the table priority and intended action; no mutation occurs."""
    if not s.access_ok or (not s.empty and s.layout == "unknown"):
        return 1, "refuse"
    if s.empty:
        return 2, "initialize"
    if not s.lock_ok or s.unsafe_entry or s.layout == "mixed":
        return 3, "refuse"
    if s.layout == "legacy":
        return 4, "legacy"
    if s.layout != "candidate":
        raise ValueError("invalid test observation")
    if s.authority != "valid":
        return 5, "refuse"
    if s.temporary:
        return 6, "refuse"
    if not s.segments_match:
        return 7, "refuse"
    if not s.certified_valid:
        return 8, "refuse"
    if s.active_suffix:
        return 10, "verify_then_truncate_and_sync"
    return 9, "open"


class TableCases(unittest.TestCase):
    def test_table_rows_and_collisions(self):
        clean = Observed()
        cases = [
            ("inaccessible and empty", replace(clean, access_ok=False, empty=True), (1, "refuse")),
            ("empty under directory lock", replace(clean, empty=True, lock_ok=False), (2, "initialize")),
            ("nonempty missing lock", replace(clean, lock_ok=False), (3, "refuse")),
            ("unsafe quarantine name", replace(clean, unsafe_entry=True), (3, "refuse")),
            ("mixed layouts", replace(clean, layout="mixed"), (3, "refuse")),
            ("unclassified layout", replace(clean, layout="unknown"), (1, "refuse")),
            ("unclassified and unsafe", replace(clean, layout="unknown", unsafe_entry=True), (1, "refuse")),
            ("legacy dispatch", replace(clean, layout="legacy"), (4, "legacy")),
            ("legacy temporary uses legacy protocol", replace(clean, layout="legacy", temporary=True), (4, "legacy")),
            ("missing authority plus temp", replace(clean, authority="missing", temporary=True), (5, "refuse")),
            ("invalid authority plus orphan", replace(clean, authority="invalid", segments_match=False), (5, "refuse")),
            ("temporary", replace(clean, temporary=True), (6, "refuse")),
            ("temporary plus orphan", replace(clean, temporary=True, segments_match=False), (6, "refuse")),
            ("missing or orphan segment", replace(clean, segments_match=False), (7, "refuse")),
            ("missing segment plus suffix", replace(clean, segments_match=False, active_suffix=True), (7, "refuse")),
            ("corrupt certified data", replace(clean, certified_valid=False), (8, "refuse")),
            ("corruption plus suffix", replace(clean, certified_valid=False, active_suffix=True), (8, "refuse")),
            ("clean authority", clean, (9, "open")),
            ("validated active suffix", replace(clean, active_suffix=True), (10, "verify_then_truncate_and_sync")),
        ]
        for name, state, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(classify(state), expected)

    def test_refusal_precedes_suffix_recovery(self):
        for field in ("access_ok", "lock_ok", "segments_match", "certified_valid"):
            state = replace(Observed(active_suffix=True), **{field: False})
            with self.subTest(field=field):
                self.assertEqual(classify(state)[1], "refuse")
        for field in ("unsafe_entry", "temporary"):
            state = replace(Observed(active_suffix=True), **{field: True})
            with self.subTest(field=field):
                self.assertEqual(classify(state)[1], "refuse")


if __name__ == "__main__":
    unittest.main()
