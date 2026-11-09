"""Static ordinary CI contract. No subprocess, compilation or external access."""
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class RemoteDiagnosticCI(unittest.TestCase):
    def test_original_gate_and_failure_artifact_are_preserved(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertEqual(workflow.count("run: cargo test --locked"), 1)
        self.assertNotIn("continue-on-error", workflow)
        test = workflow.split("  test:\n", 1)[1].split("  clippy:\n", 1)[0]
        self.assertIn('VELAGRAPH_REMOTE_DIAGNOSTICS: "1"', test)
        self.assertIn("if: always()", test)
        self.assertIn("uses: actions/upload-artifact@v4", test)
        self.assertIn("if-no-files-found: error", test)
        self.assertIn("github.run_id", test)
        self.assertIn("github.run_attempt", test)
        self.assertIn("github.sha", test)
        self.assertNotIn("--test-threads", test)
        self.assertNotIn("--release", test)
        self.assertNotIn("rerun", test)
        self.assertIn("timeout-minutes: 45", test)

    def test_original_process_budgets_and_assertions_remain(self):
        fixture = (ROOT / "crates/replicated-graph/tests/remote_process.rs").read_text()
        node = (ROOT / "crates/replicated-graph/src/bin/graph-node.rs").read_text()
        self.assertIn("timeout(Duration::from_secs(180), scenario())", fixture)
        self.assertIn("let budget = Duration::from_secs(20);", fixture)
        self.assertIn("GraphService::start_identified(&node, graph_addr, group, shard, Duration::from_secs(3))", node)
        self.assertIn("assert!(pages >= 3)", fixture)
        self.assertIn("assert_eq!(entries, before.entries)", fixture)


if __name__ == "__main__":
    unittest.main()
