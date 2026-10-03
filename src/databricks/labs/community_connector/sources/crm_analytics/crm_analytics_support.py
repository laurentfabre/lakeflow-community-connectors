"""Pure naming/validation helpers and per-read resource accounting."""

import hashlib
import json
import re
import time
from decimal import Decimal


class CrmAnalyticsError(RuntimeError):
    """A sanitized configuration, source, or snapshot-integrity failure."""


def _fingerprint(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
        ).encode()
    ).hexdigest()


def _integer(value, name: str, lower: int, upper: int) -> int:
    if isinstance(value, Decimal) and value.is_finite() and value == value.to_integral_value():
        value = int(value)
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        raise CrmAnalyticsError(f"{name} must be an integer in [{lower}, {upper}]")
    result = int(value)
    if not lower <= result <= upper:
        raise CrmAnalyticsError(f"{name} must be an integer in [{lower}, {upper}]")
    return result


def encode_column_name(name: str, mode: str = "readable") -> str:
    """Map one source name; schema construction separately rejects collisions.

    Readable names retain ASCII letters/digits/underscores, replace dots with
    double underscores, and escape other code points as ``_uXXXX_`` (lowercase
    hex, at least four digits). Prefix digit-leading names and reserved ``none``
    with ``c_``. Recovery uses field metadata, not ambiguous string decoding.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("A nonempty source column name is required")
    if mode == "legacy_hex":
        return "c_" + name.encode("utf-8").hex()
    if mode != "readable":
        raise ValueError("column_name_mode must be readable or legacy_hex")
    target = "".join(
        (
            char
            if char.isascii() and (char.isalnum() or char == "_")
            else "__" if char == "." else f"_u{ord(char):04x}_"
        )
        for char in name
    )
    return "c_" + target if target[0].isdigit() or target.lower() == "none" else target


def decode_column_name(name: str, mode: str = "readable") -> str:
    """Decode only explicitly selected legacy names; readable names need metadata."""
    if mode != "legacy_hex":
        raise ValueError(
            "Readable names require source_name field metadata; decoding needs mode=legacy_hex"
        )
    if not isinstance(name, str) or not re.fullmatch(r"c_(?:[0-9a-f]{2})+", name):
        raise ValueError("Not an encoded CRM Analytics column name")
    return bytes.fromhex(name[2:]).decode("utf-8")


def _field_name(value) -> str:
    # Do not guess SAQL escaping rules. Dotted identifiers are safe in quotes;
    # quotes, backslashes and control characters are deliberately unsupported.
    if (
        not isinstance(value, str)
        or not value
        or any(0xD800 <= ord(c) <= 0xDFFF for c in value)
        or len(value.encode("utf-8")) > 1024
        or any(ord(c) < 32 or c in "'\\;" for c in value)
    ):
        raise CrmAnalyticsError("Unsupported or unsafe physical field name in XMD")
    return value


def _field_list(value, option_name: str) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            raise CrmAnalyticsError(
                f"{option_name} must be a JSON array of source field names"
            ) from None
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(name, str) for name in value)
        or len(set(value)) != len(value)
    ):
        raise CrmAnalyticsError(
            f"{option_name} must be a nonempty array of distinct source field names"
        )
    return [_field_name(name) for name in value]


def _response_body(response) -> dict:
    if len(response.content) > 64 * 1024 * 1024:
        raise CrmAnalyticsError("CRM Analytics response exceeds the 64 MiB safety limit")
    try:

        def reject_constant(_):
            raise ValueError("non-finite JSON number")

        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate JSON property")
                result[key] = value
            return result

        body = json.loads(
            response.text,
            parse_float=Decimal,
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except (ValueError, UnicodeError):
        raise CrmAnalyticsError("CRM Analytics returned malformed JSON") from None
    if not isinstance(body, dict):
        raise CrmAnalyticsError("CRM Analytics response must be a JSON object")
    if body.get("warnings"):
        raise CrmAnalyticsError("CRM Analytics returned query warnings; snapshot aborted")
    return body


class _ReadBudget:
    """State for one iterator, never attached to the serializable connector.

    Time includes cold metadata resolution, retries and consumer pauses. The
    socket timeouts are bounded by remaining time but cannot strictly cancel a
    synchronous requests call; any late response is rejected after it returns.
    """

    def __init__(self, started_at: float, options: dict):
        self.started_at = started_at
        self.max_query_requests = options["max_query_requests"]
        self.max_read_seconds = options["max_read_seconds"]
        self.query_requests = 0

    def remaining(self) -> float:
        remaining = self.max_read_seconds - (time.monotonic() - self.started_at)
        if remaining <= 0:
            raise CrmAnalyticsError("Snapshot exceeded max_read_seconds; entire snapshot aborted")
        return remaining

    def request_timeout(self, configured: float, *, query: bool) -> tuple[float, float]:
        timeout = min(configured, self.remaining())
        if query:
            self.ensure_query_available()
            self.query_requests += 1
        return timeout, timeout

    def ensure_query_available(self) -> None:
        if self.query_requests >= self.max_query_requests:
            raise CrmAnalyticsError("Snapshot exceeded max_query_requests; entire snapshot aborted")

    def sleep(self, delay: float, *, query: bool = False) -> None:
        if query:
            self.ensure_query_available()
        if delay >= self.remaining():
            raise CrmAnalyticsError(
                "Retry delay would exhaust max_read_seconds; entire snapshot aborted"
            )
        time.sleep(delay)
        self.remaining()

    def metrics(self) -> dict:
        remaining = self.remaining()
        return {
            "query_requests": self.query_requests,
            "elapsed_seconds": self.max_read_seconds - remaining,
            "max_query_requests": self.max_query_requests,
            "max_read_seconds": self.max_read_seconds,
        }
