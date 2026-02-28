import os
import sys
import unittest
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure we can import strake
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from strake.utils import get_script_dir, get_strake_dir
from strake.tracing import get_emitter
import strake.tracing.session as session


class TestLogDiscovery(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        # Reset emitter singleton for clean tests
        with session._emitter_lock:
            session._emitter = None

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        # Reset emitter again
        with session._emitter_lock:
            session._emitter = None

    def test_happy_path_script_relative(self):
        """HAPPY PATH: Logs are created in .strake/traces/ inside the script directory."""
        project_dir = Path(self.test_dir) / "my_project"
        project_dir.mkdir()
        script_path = project_dir / "app.py"
        script_path.touch()

        with patch.object(sys, "argv", [str(script_path)]):
            sd = get_script_dir()
            self.assertEqual(sd, project_dir.resolve())

            # Verify get_strake_dir
            trace_dir = get_strake_dir("traces")
            self.assertEqual(trace_dir, (project_dir / ".strake" / "traces").resolve())

            # Verify emitter uses this dir
            emitter = get_emitter()
            self.assertEqual(emitter.trace_dir, trace_dir)
            self.assertTrue(trace_dir.exists())

    def test_error_path_read_only(self):
        """ERROR PATH: Fallback to home directory if script directory is not writable."""
        project_dir = Path(self.test_dir) / "readonly_project"
        project_dir.mkdir()
        script_path = project_dir / "app.py"
        script_path.touch()

        # Mock os.access to return False for writability on this directory
        original_access = os.access

        def mock_access(path, mode):
            if str(path) == str(project_dir) and mode == os.W_OK:
                return False
            return original_access(path, mode)

        with patch("os.access", side_effect=mock_access):
            with patch.object(sys, "argv", [str(script_path)]):
                with patch("os.path.expanduser", return_value=self.test_dir):
                    sd = get_script_dir()
                    self.assertIsNone(sd)  # Should reject read-only dir

                    # Should fallback to fake home (self.test_dir)
                    trace_dir = get_strake_dir("traces")
                    expected_dir = (Path(self.test_dir) / "traces").resolve()
                    self.assertEqual(trace_dir, expected_dir)

    def test_edge_case_pytest_runner(self):
        """EDGE CASE: Fallback to home directory if running via pytest."""
        with patch.object(sys, "argv", ["/usr/bin/pytest", "test_file.py"]):
            with patch("os.path.expanduser", return_value=self.test_dir):
                sd = get_script_dir()
                self.assertIsNone(sd)  # Should ignore pytest binary

                trace_dir = get_strake_dir("traces")
                expected_dir = (Path(self.test_dir) / "traces").resolve()
                self.assertEqual(trace_dir, expected_dir)

    def test_edge_case_repl(self):
        """EDGE CASE: Fallback to home directory if running in a REPL (sys.argv[0] is empty)."""
        with patch.object(sys, "argv", [""]):
            with patch("os.path.expanduser", return_value=self.test_dir):
                sd = get_script_dir()
                self.assertIsNone(sd)

                trace_dir = get_strake_dir("traces")
                expected_dir = (Path(self.test_dir) / "traces").resolve()
                self.assertEqual(trace_dir, expected_dir)


if __name__ == "__main__":
    unittest.main()
