#!/usr/bin/env python3
"""Boundary model only: not VelaGraph code, a database, or a benchmark of Rust.

Trusted inventory models a private publication witness, not self-supplied disk
metadata. strict_read enumerates every byte; point_read intentionally cannot
certify unrelated rows. No simplified transaction semantics are presented here.
"""
import hashlib
import json
import unittest
from types import MappingProxyType


def digest(rows):
    h = hashlib.sha256(b"velagraph-ledger-boundary-model-v1\0")
    for key, value in sorted(rows.items()):
        for field in (key, value):
            h.update(len(field).to_bytes(8, "big"))
            h.update(field)
    return h.digest()


class View:
    def __init__(self, generation, rows):
        self.generation = generation
        self.rows = MappingProxyType(dict(rows))
        self.expected = digest(self.rows)


class Store:
    def __init__(self, rows):
        self.disk = dict(rows)
        self.generation = 0
        self.published = View(0, rows)

    def strict_read(self, key):
        # Independent trusted witness is essential. Loading expected from the
        # same untrusted store would accept a coherently forged replacement.
        if digest(self.disk) != self.published.expected:
            raise ValueError("complete namespace mismatch")
        return self.disk.get(key)

    def point_read(self, key):
        # This is deliberately NOT a safe replacement for strict_read.
        if self.disk.get(key) != self.published.rows.get(key):
            raise ValueError("point mismatch")
        return self.disk.get(key)

    def commit(self, additions, fail_sync=False, crash_after_sync=False):
        # All staged changes private until successful simulated atomic commit.
        if digest(self.disk) != self.published.expected:
            raise ValueError("predecessor mismatch")
        candidate = dict(self.disk)
        candidate.update(additions)
        next_view = View(self.generation + 1, candidate)
        if fail_sync:
            raise OSError("simulated sync failure before commit")
        self.disk = candidate
        self.generation += 1
        if crash_after_sync:
            return
        self.published = next_view

    def reopen(self, independently_validated_rows):
        # Caller represents FULL semantic replay + derived closure, not cached
        # memory fallback or merely trusting a new root supplied by disk.
        if self.disk != independently_validated_rows:
            raise ValueError("recovery oracle mismatch")
        self.published = View(self.generation, self.disk)


def fixture(n):
    return {i.to_bytes(8, "big"): bytes([i % 251]) * 64 for i in range(n)}


class IntegrityBoundaryTests(unittest.TestCase):
    def test_point_cannot_detect_unrelated_corruption(self):
        s = Store(fixture(3))
        key, unrelated = (0).to_bytes(8, "big"), (2).to_bytes(8, "big")
        original = s.point_read(key)
        s.disk[unrelated] = b"corrupt"
        self.assertEqual(s.point_read(key), original)
        with self.assertRaises(ValueError):
            s.strict_read(key)

    def test_namespace_closure(self):
        for mutation in ("extra", "missing", "foreign", "substitute"):
            with self.subTest(mutation=mutation):
                s = Store(fixture(3))
                key = (0).to_bytes(8, "big")
                other = (2).to_bytes(8, "big")
                if mutation == "extra":
                    s.disk[b"extra"] = b"x"
                elif mutation == "missing":
                    del s.disk[other]
                elif mutation == "foreign":
                    s.disk[b"foreign"] = s.disk.pop(other)
                else:
                    s.disk[other] = b"replacement"
                with self.assertRaises(ValueError):
                    s.strict_read(key)

    def test_forged_disk_root_does_not_replace_private_expectation(self):
        s = Store(fixture(2))
        s.disk[(1).to_bytes(8, "big")] = b"forged"
        forged_disk_root = digest(s.disk)
        self.assertNotEqual(forged_disk_root, s.published.expected)
        with self.assertRaises(ValueError):
            s.strict_read((0).to_bytes(8, "big"))

    def test_failed_sync_never_publishes(self):
        s = Store(fixture(2))
        before, rows = s.published, dict(s.disk)
        with self.assertRaises(OSError):
            s.commit({b"new": b"value"}, fail_sync=True)
        self.assertIs(s.published, before)
        self.assertEqual(s.disk, rows)
        self.assertEqual(s.generation, 0)

    def test_pinned_old_view_is_immutable(self):
        s = Store(fixture(2))
        old = s.published
        s.commit({b"new": b"value"})
        self.assertNotIn(b"new", old.rows)
        self.assertIn(b"new", s.published.rows)
        with self.assertRaises(TypeError):
            old.rows[b"new"] = b"illegal"

    def test_post_sync_crash_requires_recovery(self):
        s = Store(fixture(2))
        old = s.published
        expected = dict(s.disk, **{})
        expected[b"new"] = b"value"
        s.commit({b"new": b"value"}, crash_after_sync=True)
        self.assertIs(s.published, old)
        with self.assertRaises(ValueError):
            s.strict_read(b"new")
        s.reopen(expected)
        self.assertEqual(s.strict_read(b"new"), b"value")

    def test_reopen_rejects_corruption_against_independent_oracle(self):
        rows = fixture(2)
        s = Store(rows)
        s.disk[b"extra"] = b"unvalidated"
        with self.assertRaises(ValueError):
            s.reopen(rows)

    def test_length_prefix_prevents_ambiguous_inventory(self):
        self.assertNotEqual(digest({b"a": b"bc"}), digest({b"ab": b"c"}))


def complexity_report():
    # Deterministic logical work, no misleading wall-time Python comparison.
    return [{"rows": n, "strict_value_bytes_read": n * 64,
             "point_value_bytes_read": 64,
             "point_certifies_unrelated_rows": False}
            for n in (16, 256, 4096, 16384)]


if __name__ == "__main__":
    import sys
    if "--work-counts" in sys.argv:
        print(json.dumps(complexity_report(), indent=2))
    else:
        unittest.main()
