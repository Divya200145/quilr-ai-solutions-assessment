"""Streaming PII redactor with bounded state.

Idea: PII patterns are made only of "token" characters (email chars) or digit/space/hyphen runs.
After appending a delta we redact the buffer, then emit everything except the *trailing run* that
could still be the prefix of a pattern split across chunks. Only that small tail is held back, so:
  * text before the tail is forwarded immediately (low TTFT / inter-chunk latency),
  * memory is O(MAX_HOLD + chunk), never O(response),
  * each byte is regex-scanned a bounded number of times (buffer = held tail + new delta only).
"""
from __future__ import annotations

import re

REDACTED = "[REDACTED]"
MAX_HOLD = 320  # > longest email (254) and any card number with separators

EMAIL = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}"
SSN = r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)"
CARD = r"(?<![\d])(?:\d[ \-]?){12,18}\d(?![\d])"

_EMAIL_RE = re.compile(EMAIL)
_SSN_RE = re.compile(SSN)
_CARD_RE = re.compile(CARD)
_EMAIL_TAIL = re.compile(r"[A-Za-z0-9._%+\-@]*\Z")
_DIGIT_TAIL = re.compile(r"[0-9 \-]*\Z")


def _luhn(digits: str) -> bool:
    total, alt = 0, False
    for ch in reversed(digits):
        d = int(ch)
        if alt:
            d = d * 2 - (9 if d > 4 else 0)
        total += d
        alt = not alt
    return total % 10 == 0


def _card_sub(m: re.Match) -> str:
    digits = re.sub(r"\D", "", m.group(0))
    return REDACTED if 13 <= len(digits) <= 19 and _luhn(digits) else m.group(0)


def redact(text: str) -> str:
    """Redact a complete string (reference implementation)."""
    text = _EMAIL_RE.sub(REDACTED, text)
    text = _SSN_RE.sub(REDACTED, text)
    return _CARD_RE.sub(_card_sub, text)


class StreamRedactor:
    def __init__(self) -> None:
        self._buf = ""

    def feed(self, delta: str) -> str:
        """Add a delta, return text that is safe to emit now."""
        self._buf += delta
        cut = self._hold_start(self._buf)
        cut = max(cut, len(self._buf) - MAX_HOLD)  # hard bound on memory/latency
        if cut <= 0:
            return ""
        out, self._buf = redact(self._buf[:cut]), self._buf[cut:]
        return out

    def flush(self) -> str:
        out, self._buf = redact(self._buf), ""
        return out

    @staticmethod
    def _hold_start(s: str) -> int:
        e = _EMAIL_TAIL.search(s).start()
        d = _DIGIT_TAIL.search(s).start()
        return min(e, d)
