"""Terminal UI: ANSI status bar with single-keypress controls.

Uses termios/tty for cbreak mode and a custom log handler to coordinate
log output with a persistent status bar on the last terminal row.
Before each log line the bar is cleared; after, it is redrawn. This avoids
scroll-region tricks which interact poorly with stderr-based logging.
Zero new dependencies — stdlib only.
"""

import atexit
import logging
import os
import queue
import signal
import sys
import threading

logger = logging.getLogger(__name__)


class ConsoleUI:
    """Persistent status bar with single-keypress controls.

    Modes:
        "live"           — [F] Finalize  [Q] Quit + specimen counts
        "batch"          — [Q] Quit + specimen counts
        "batch_complete" — [Enter] Exit + specimen counts

    Non-TTY: all methods are no-ops.
    """

    def __init__(self, mode: str, state, cmd_queue: queue.Queue):
        self.mode = mode
        self.state = state
        self.cmd_queue = cmd_queue
        self._active = False
        self._old_termios = None
        self._old_sigwinch = None
        self._reader_thread = None
        self._stop_event = threading.Event()
        self._is_tty = sys.stdin.isatty() and sys.stderr.isatty()
        self._term_lock = threading.Lock()
        self._original_handlers: list[logging.Handler] = []
        self._bar_handler: logging.Handler | None = None

    def __enter__(self):
        if not self._is_tty:
            return self
        try:
            self._setup_terminal()
            self._active = True
            self._install_log_handler()
            self._start_reader()
            self.redraw()
        except Exception:
            logger.debug("Failed to set up terminal UI, falling back to no-op", exc_info=True)
            self._active = False
        return self

    def __exit__(self, *exc):
        if self._active:
            self._stop_event.set()
            self._clear_bar()
            self._remove_log_handler()
            self._restore_terminal()
            self._active = False
        return False

    def _setup_terminal(self):
        import termios
        import tty

        fd = sys.stdin.fileno()
        self._old_termios = termios.tcgetattr(fd)
        atexit.register(self._atexit_restore)

        # Set cbreak mode: single-keypress, no echo
        tty.setcbreak(fd)

        # Handle terminal resize
        self._old_sigwinch = signal.getsignal(signal.SIGWINCH)
        signal.signal(signal.SIGWINCH, self._on_resize)

    def _restore_terminal(self):
        # Restore termios
        if self._old_termios is not None:
            import termios
            try:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._old_termios)
            except Exception:
                pass
            self._old_termios = None

        # Restore SIGWINCH
        if self._old_sigwinch is not None:
            try:
                signal.signal(signal.SIGWINCH, self._old_sigwinch)
            except Exception:
                pass
            self._old_sigwinch = None

        atexit.unregister(self._atexit_restore)

    def _atexit_restore(self):
        """Safety net: restore terminal on unexpected exit."""
        if self._old_termios is not None:
            import termios
            try:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._old_termios)
            except Exception:
                pass
            self._old_termios = None

    def _install_log_handler(self):
        """Replace root logger's stderr handler with a bar-aware handler."""
        root = logging.getLogger()
        for h in root.handlers[:]:
            if isinstance(h, logging.StreamHandler) and h.stream in (sys.stderr, sys.__stderr__):
                self._original_handlers.append(h)
                root.removeHandler(h)
        self._bar_handler = _BarAwareHandler(self)
        if self._original_handlers:
            self._bar_handler.setFormatter(self._original_handlers[0].formatter)
            self._bar_handler.setLevel(self._original_handlers[0].level)
        root.addHandler(self._bar_handler)

    def _remove_log_handler(self):
        """Restore original log handlers."""
        root = logging.getLogger()
        if self._bar_handler:
            root.removeHandler(self._bar_handler)
            self._bar_handler = None
        for h in self._original_handlers:
            root.addHandler(h)
        self._original_handlers = []

    def _get_size(self) -> tuple[int, int]:
        """Return (rows, cols) of the terminal."""
        try:
            size = os.get_terminal_size(sys.stderr.fileno())
            return size.lines, size.columns
        except OSError:
            return 24, 80

    def _on_resize(self, sig, frame):
        """SIGWINCH handler: redraw status bar at new position."""
        if self._active:
            self.redraw()

    def _start_reader(self):
        """Start daemon thread that reads keypresses."""
        self._reader_thread = threading.Thread(
            target=self._key_reader, daemon=True, name="console-key-reader"
        )
        self._reader_thread.start()

    def _key_reader(self):
        """Read single bytes from stdin and push commands onto the queue."""
        fd = sys.stdin.fileno()
        while not self._stop_event.is_set():
            try:
                ch = os.read(fd, 1)
            except OSError:
                break
            if not ch:
                break
            c = ch[0]
            if self.mode in ("live", "batch"):
                if c in (ord("f"), ord("F")) and self.mode == "live":
                    self.cmd_queue.put("finalize")
                elif c in (ord("q"), ord("Q")):
                    self.cmd_queue.put("quit")
            elif self.mode == "batch_complete":
                if c in (13, 10):  # Enter / newline
                    self.cmd_queue.put("enter")

    def _clear_bar(self):
        """Erase the status bar line."""
        rows, cols = self._get_size()
        sys.stderr.write(f"\033[s\033[{rows};1H\033[2K\033[u")
        sys.stderr.flush()

    def _draw_bar(self):
        """Draw the status bar on the last row. Caller must hold _term_lock."""
        rows, cols = self._get_size()
        left, right = self._format_bar(cols)

        # Pad/truncate to fill the row
        if right:
            gap = cols - len(left) - len(right)
            if gap > 0:
                content = left + " " * gap + right
            else:
                content = (left + "  " + right)[:cols]
        else:
            content = left[:cols]

        sys.stderr.write(
            f"\033[s"                      # save cursor
            f"\033[{rows};1H"              # move to last row
            f"\033[7m"                     # reverse video
            f"{content:<{cols}}"           # left-aligned, padded to full width
            f"\033[0m"                     # reset attributes
            f"\033[u"                      # restore cursor
        )
        sys.stderr.flush()

    def redraw(self):
        """Render the status bar at the bottom row (thread-safe)."""
        if not self._active:
            return
        with self._term_lock:
            self._draw_bar()

    def _format_bar(self, cols: int) -> tuple[str, str]:
        """Return (left, right) strings for the status bar."""
        if self.mode == "live":
            left = " [F] Finalize  [Q] Quit"
        elif self.mode == "batch":
            left = " [Q] Quit"
        elif self.mode == "batch_complete":
            left = " [Enter] Exit"
        else:
            left = ""

        right = self._specimen_summary()
        return left, right

    def _specimen_summary(self) -> str:
        """Build right-side specimen counts from state."""
        if self.state is None:
            return ""

        from .state import SpecimenStatus

        total = len(self.state.specimens)
        identified = sum(
            1 for s in self.state.specimens.values()
            if s.status in (SpecimenStatus.IDENTIFIED, SpecimenStatus.NO_MATCH)
        )
        processing = sum(
            1 for s in self.state.specimens.values()
            if s.status in (SpecimenStatus.CONSENSUS_RUNNING, SpecimenStatus.CONSENSUS_DONE)
        )

        parts = []
        parts.append(f"{identified} identified")
        if processing > 0:
            parts.append(f"{processing} processing")
        parts.append(f"{total} specimens")
        return " · ".join(parts) + " "


class _BarAwareHandler(logging.StreamHandler):
    """Log handler that clears the status bar before each message and redraws after.

    This ensures log lines scroll naturally above the bar without overlap.
    All terminal writes are serialized via ConsoleUI._term_lock.
    """

    def __init__(self, console: ConsoleUI):
        super().__init__(sys.stderr)
        self.console = console

    def emit(self, record):
        with self.console._term_lock:
            try:
                rows, cols = self.console._get_size()
                msg = self.format(record)
                # Clear bar → write log message → redraw bar
                sys.stderr.write(
                    f"\033[s\033[{rows};1H\033[2K\033[u"   # clear bar line
                    f"{msg}{self.terminator}"                # log message
                )
                sys.stderr.flush()
                self.console._draw_bar()
            except Exception:
                self.handleError(record)
