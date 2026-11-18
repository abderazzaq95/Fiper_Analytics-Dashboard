"""One place for agent identities across ManyContacts, Maqsam and the contact sheet.

Everything (dashboard, reminders, reports) uses this list, so a person appears
under one name everywhere. Add new spellings to AGENTS.
"""
from __future__ import annotations

import re
import unicodedata

# Display name -> other spellings seen in the source systems.
AGENTS: dict[str, list[str]] = {
    "Alaa Eddin Alesh": ["alaa al eish"],
    "Amjad Alkhawaldi": ["amjad", "amjad mahmoud"],
    "Bashir Hallak": ["bashir", "basher", "basher hallak"],
    "Fatima Zohra Hbada": ["zahrae"],
    "Fatma Aqel": ["fatma", "fatima akel", "fatima aqel"],
    "Feras Zabalawi": ["firas zabalwi", "feras zabalwi"],
    "Hammoudi Abdellatif": ["abdellatif hammoudi", "abdulatif hamoudi"],
    "Hasan Salameh": ["hasan salemeh"],
    "Joseph Kesserwany": ["joseph kesrwany", "joeseph kesserwany"],
    "Maitha Almoqbali": ["maitha", "maitha moqbali", "maitha meqbali", "meqbali"],
    "Menna Khaled": ["menatalla zaky"],
    "Mohamed Dhafeer": ["mohamed zafer", "mohamad zafer", "محمد ضافر"],
    "Mohammed Hussein": ["mohamed hussein", "mohamad husein", "mohamed hssin"],
    "Saif Ghaleb": ["saif talib"],
}

_SPELLING_FIXES = (("jihad", "jehad"), ("mohamad", "mohamed"), ("husein", "hussein"), ("basher", "bashir"))


def normalize_name(name: str | None) -> str:
    value = unicodedata.normalize("NFKD", name or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    for old, new in _SPELLING_FIXES:
        value = value.replace(old, new)
    return value


def _lookup_key(name: str | None) -> str:
    """Normalized Latin key; names without Latin letters keep their own text."""
    raw = " ".join((name or "").split()).lower()
    return normalize_name(name) or raw


_BY_KEY: dict[str, str] = {}
RAW_ALIASES: dict[str, str] = {}  # lower-case spelling -> display name
for _canon, _spellings in AGENTS.items():
    for _spelling in [_canon, *_spellings]:
        _BY_KEY[_lookup_key(_spelling)] = _canon
        RAW_ALIASES[_spelling.lower()] = _canon


def canonical(name: str | None) -> str | None:
    """Known display name for this spelling, or None."""
    return _BY_KEY.get(_lookup_key(name)) if (name or "").strip() else None


def display(name: str | None) -> str | None:
    """Display name: the known one, otherwise the cleaned spelling in Title Case."""
    clean = " ".join((name or "").split())
    return (canonical(clean) or clean.title()) if clean else None


def key(name: str | None) -> str:
    """Stable identity for grouping and matching."""
    return _lookup_key(display(name))


def contact_aliases() -> dict[str, str]:
    """normalized spelling -> normalized display name (contact-sheet lookup)."""
    out = {}
    for spelling, canon in RAW_ALIASES.items():
        left, right = normalize_name(spelling), normalize_name(canon)
        if left and right and left != right:
            out[left] = right
    return out
