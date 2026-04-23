"""Per-user draft state with auto-expiry.

Each Telegram user has at most one in-progress draft. Drafts that sit idle
longer than DRAFT_TIMEOUT_MINUTES are discarded.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import config


# Ordered list of fields the bot collects. Used by the review/edit picker.
FIELDS: list[tuple[str, str]] = [
    ("seasoning", "Seasoning Requested"),
    ("comment", "Comment"),
    ("quantity", "Quantity"),
    ("price_budget", "Selling Price Budget"),
    ("app_method", "Application Method"),
    ("dosage", "Dosage"),
    ("requirement", "Requirement"),
    ("market", "Market"),
    ("deadline", "Deadline"),
    ("taste_check", "Need to Check Taste"),
    ("customer_base", "Customer Base"),
    ("courier", "Preferred Courier"),
    ("company_name", "Customer Company Name"),
    ("receiver_number", "Receiver Number"),
    ("address", "Address"),
    ("receiving_person", "Receiving Person"),
]

FIELD_LABELS = dict(FIELDS)


@dataclass
class Draft:
    user_id: int
    username: str = ""
    data: dict[str, str] = field(default_factory=dict)
    # Seasoning match metadata stored so we can log it.
    matched_code: str = ""
    matched_price: str = ""
    matched_category: str = ""
    # State machine position: name of the next field to ask, or "review".
    stage: str = "seasoning"
    # When the user is editing from the review screen, after the next valid
    # answer we jump back to review.
    return_to_review: bool = False
    last_touch: float = field(default_factory=time.time)
    # Sub-state for multi-step questions (price currency then amount, etc.)
    sub: str = ""
    # Cumulative AI token usage for this draft session.
    tokens_in: int = 0
    tokens_out: int = 0

    @property
    def tokens_total(self) -> int:
        return self.tokens_in + self.tokens_out

    def touch(self) -> None:
        self.last_touch = time.time()

    def is_expired(self) -> bool:
        return time.time() - self.last_touch > config.DRAFT_TIMEOUT_MINUTES * 60


_drafts: dict[int, Draft] = {}
# user_ids whose draft was *just* expired by get(); consumed by consume_expired_flag().
_expired_recently: set[int] = set()


def get(user_id: int) -> Draft | None:
    d = _drafts.get(user_id)
    if d and d.is_expired():
        _drafts.pop(user_id, None)
        _expired_recently.add(user_id)
        return None
    return d


def consume_expired_flag(user_id: int) -> bool:
    """Return True exactly once if the user's draft was just expired by get()."""
    if user_id in _expired_recently:
        _expired_recently.discard(user_id)
        return True
    return False


def start(user_id: int, username: str) -> Draft:
    d = Draft(user_id=user_id, username=username)
    _drafts[user_id] = d
    _expired_recently.discard(user_id)
    return d


def clear(user_id: int) -> None:
    _drafts.pop(user_id, None)
    _expired_recently.discard(user_id)
