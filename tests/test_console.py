"""Tests for ConsoleUI status bar and key handling."""

import queue
from unittest.mock import patch, MagicMock

from specimux_suite.console import ConsoleUI
from specimux_suite.state import PipelineState, SpecimenStatus


def _make_state(specimens=None):
    """Create a PipelineState with optional specimen data."""
    state = PipelineState()
    if specimens:
        for sid, status in specimens.items():
            spec = state.get_specimen(sid)
            spec.status = status
    return state


class TestStatusBarFormatting:
    def test_live_mode_left_side(self):
        state = _make_state()
        ui = ConsoleUI("live", state, queue.Queue())
        left, right = ui._format_bar(80)
        assert "[F] Finalize" in left
        assert "[Q] Quit" in left

    def test_batch_mode_left_side(self):
        state = _make_state()
        ui = ConsoleUI("batch", state, queue.Queue())
        left, right = ui._format_bar(80)
        assert "[Q] Quit" in left
        assert "Finalize" not in left

    def test_batch_complete_left_side(self):
        state = _make_state()
        ui = ConsoleUI("batch_complete", state, queue.Queue())
        left, right = ui._format_bar(80)
        assert "[Enter] Exit" in left

    def test_specimen_counts(self):
        state = _make_state({
            "A": SpecimenStatus.IDENTIFIED,
            "B": SpecimenStatus.IDENTIFIED,
            "C": SpecimenStatus.NO_MATCH,
            "D": SpecimenStatus.CONSENSUS_RUNNING,
            "E": SpecimenStatus.WAITING,
        })
        ui = ConsoleUI("live", state, queue.Queue())
        summary = ui._specimen_summary()
        assert "3 identified" in summary
        assert "1 processing" in summary
        assert "5 specimens" in summary

    def test_specimen_counts_no_processing(self):
        state = _make_state({
            "A": SpecimenStatus.IDENTIFIED,
            "B": SpecimenStatus.WAITING,
        })
        ui = ConsoleUI("live", state, queue.Queue())
        summary = ui._specimen_summary()
        assert "1 identified" in summary
        assert "processing" not in summary
        assert "2 specimens" in summary

    def test_empty_state(self):
        state = _make_state()
        ui = ConsoleUI("live", state, queue.Queue())
        summary = ui._specimen_summary()
        assert "0 identified" in summary
        assert "0 specimens" in summary

    def test_none_state(self):
        ui = ConsoleUI("live", None, queue.Queue())
        assert ui._specimen_summary() == ""


class TestNonTTYFallback:
    def test_noop_when_not_tty(self):
        """ConsoleUI should be a no-op when stdin/stdout are not TTYs."""
        state = _make_state()
        cmd_queue = queue.Queue()
        with patch("specimux_suite.console.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            mock_sys.stdout.isatty.return_value = False
            ui = ConsoleUI("live", state, cmd_queue)
            assert not ui._is_tty

        # enter/exit should not crash
        with ui:
            assert not ui._active
            ui.redraw()  # no-op, should not crash


class TestKeyMapping:
    def _run_key_reader(self, mode, key_bytes):
        """Helper: run _key_reader with mocked stdin.fileno() and os.read."""
        cmd_queue = queue.Queue()
        ui = ConsoleUI(mode, _make_state(), cmd_queue)
        ui._active = True

        with patch("sys.stdin") as mock_stdin, \
             patch("os.read", side_effect=[key_bytes, OSError("done")]):
            mock_stdin.fileno.return_value = 0
            ui._key_reader()

        return cmd_queue

    def test_live_mode_f_key(self):
        q = self._run_key_reader("live", b"f")
        assert q.get_nowait() == "finalize"

    def test_live_mode_q_key(self):
        q = self._run_key_reader("live", b"q")
        assert q.get_nowait() == "quit"

    def test_live_mode_F_key(self):
        q = self._run_key_reader("live", b"F")
        assert q.get_nowait() == "finalize"

    def test_batch_complete_enter_key(self):
        q = self._run_key_reader("batch_complete", b"\r")
        assert q.get_nowait() == "enter"

    def test_batch_complete_ignores_f_key(self):
        q = self._run_key_reader("batch_complete", b"f")
        assert q.empty()

    def test_live_mode_ignores_enter(self):
        q = self._run_key_reader("live", b"\r")
        assert q.empty()

    def test_batch_mode_q_key(self):
        q = self._run_key_reader("batch", b"q")
        assert q.get_nowait() == "quit"

    def test_batch_mode_ignores_f_key(self):
        q = self._run_key_reader("batch", b"f")
        assert q.empty()


def test_web_safe_name_validation():
    """Path params interpolated into globs must reject traversal/metacharacters."""
    from specimux_suite.web.server import _is_safe_name

    assert _is_safe_name("ONT01.01-A01--iNat233404001")
    assert _is_safe_name("ONT01.01-A01--iNat233404001-1.v2")
    assert not _is_safe_name("../etc/passwd")
    assert not _is_safe_name("a/b")
    assert not _is_safe_name(".hidden")
    assert not _is_safe_name("a*")
    assert not _is_safe_name("a..b")
    assert not _is_safe_name("")
