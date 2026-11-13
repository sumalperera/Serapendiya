#!/usr/bin/env python3
"""Review model for proposed journal decisions; no disk I/O or crash simulation."""
from dataclasses import dataclass, replace
from pathlib import Path
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
    retired: str = "no_binding"  # no_binding, bound_missing, valid, invalid
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
    if s.retired not in ("no_binding", "bound_missing"):
        raise ValueError("invalid retired state")
    if s.suffix:
        return 11, "verify_then_truncate_and_sync"
    return 10, "open"


class Cases(unittest.TestCase):
    def test_documented_priority_rows_exist(self):
        # Guard against accidental table renumbering/removal and obvious wording
        # drift. This is not a semantic proof of equivalence or independent review.
        packet = (Path(__file__).resolve().parents[1] / "docs/adr/ADR-024-journal-rotation-candidate.md").read_text()
        section = packet.split("### Candidate opening decision order", 1)[1]
        section = section.split("Rows 10–12 are proposals", 1)[0]
        rows = {}
        for line in section.splitlines():
            cells = [cell.strip() for cell in line.split("|")]
            if len(cells) >= 5 and cells[1].isdigit():
                rows[int(cells[1])] = (cells[2].lower(), cells[3].lower())
        markers = {
            1: ("layout cannot be identified", "refuse"),
            2: ("genuinely empty", "initialize"),
            3: ("nonempty directory", "refuse"),
            4: ("legacy single-segment", "legacy"),
            5: ("candidate checkpoint", "refuse"),
            6: ("temporary checkpoint", "refuse"),
            7: ("extra unbound epoch", "refuse"),
            8: ("certified journal", "refuse"),
            9: ("retired file exists", "refuse"),
            10: ("retired file absent", "open"),
            11: ("uncertified suffix", "truncate"),
            12: ("bound retired journal exists", "preserve"),
        }
        self.assertEqual(set(rows), set(markers))
        for priority, (condition, outcome) in markers.items():
            with self.subTest(priority=priority):
                self.assertIn(condition, rows[priority][0])
                self.assertIn(outcome, rows[priority][1])

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
            (replace(clean, retired="bound_missing"), (10, "open")),
            (replace(clean, retired="bound_missing", suffix=True), (11, "verify_then_truncate_and_sync")),
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
