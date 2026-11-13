#!/usr/bin/env python3
"""Review model for proposed journal decisions; no disk I/O or crash simulation."""
from dataclasses import dataclass, replace
import unittest


@dataclass(frozen=True)
class State:
    accessible: bool = True
    empty: bool = False
    lock_and_entries_safe: bool = True
    layout: str = "journal"
    checkpoint_valid: bool = True
    temporary: bool = False
    file_set_valid: bool = True
    certified_valid: bool = True
    retired: str = "absent"  # absent, valid, invalid
    suffix: bool = False


def decision(s):
    if not s.accessible or (not s.empty and s.layout == "unknown"):
        return 1, "refuse"
    if s.empty:
        return 2, "initialize"
    if not s.lock_and_entries_safe or s.layout == "mixed":
        return 3, "refuse"
    if s.layout == "legacy":
        return 4, "legacy"
    if s.layout != "journal":
        raise ValueError("invalid fixture state")
    if not s.checkpoint_valid:
        return 5, "refuse"
    if s.temporary:
        return 6, "refuse"
    if not s.file_set_valid:
        return 7, "refuse"
    if not s.certified_valid:
        return 8, "refuse"
    if s.retired == "invalid":
        return 9, "refuse"
    if s.retired == "valid" and s.suffix:
        return 12, "refuse_pending_order"
    if s.retired == "valid":
        return 12, "preserve_or_reviewed_cleanup"
    if s.retired != "absent":
        raise ValueError("invalid retired state")
    if s.suffix:
        return 11, "verify_then_truncate_and_sync"
    return 10, "open"


class Cases(unittest.TestCase):
    def test_rows_and_precedence(self):
        clean = State()
        cases = [
            (replace(clean, accessible=False), (1, "refuse")),
            (replace(clean, empty=True), (2, "initialize")),
            (replace(clean, lock_and_entries_safe=False), (3, "refuse")),
            (replace(clean, layout="legacy"), (4, "legacy")),
            (replace(clean, checkpoint_valid=False), (5, "refuse")),
            (replace(clean, temporary=True), (6, "refuse")),
            (replace(clean, file_set_valid=False), (7, "refuse")),
            (replace(clean, certified_valid=False), (8, "refuse")),
            (replace(clean, retired="invalid"), (9, "refuse")),
            (clean, (10, "open")),
            (replace(clean, suffix=True), (11, "verify_then_truncate_and_sync")),
            (replace(clean, retired="valid"), (12, "preserve_or_reviewed_cleanup")),
            (replace(clean, retired="valid", suffix=True), (12, "refuse_pending_order")),
            (replace(clean, checkpoint_valid=False, temporary=True), (5, "refuse")),
            (replace(clean, temporary=True, file_set_valid=False), (6, "refuse")),
            (replace(clean, file_set_valid=False, suffix=True), (7, "refuse")),
            (replace(clean, certified_valid=False, suffix=True), (8, "refuse")),
            (replace(clean, retired="invalid", suffix=True), (9, "refuse")),
        ]
        for state, expected in cases:
            with self.subTest(state=state):
                self.assertEqual(decision(state), expected)


if __name__ == "__main__":
    unittest.main()
