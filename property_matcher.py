"""
Entity resolution: matches incoming listing data to an existing Property
(physical apartment/house) or creates a new one.

Matching is deterministic and score-based — no ML needed at this scale.
"""
import re
import logging
from typing import Optional

log = logging.getLogger(__name__)

MATCH_THRESHOLD = 80   # pisteet tästä ylöspäin → sama asunto


# ---------------------------------------------------------------------------
# Osoitteen normalisointi
# ---------------------------------------------------------------------------

# Suomalaisia lyhenteitä joita ei normalisoida pois
_ABBREVS = {
    r"\bkatu\b":   "katu",
    r"\bk\.\b":    "katu",
    r"\btie\b":    "tie",
    r"\bpolku\b":  "polku",
    r"\braitti\b": "raitti",
    r"\bkuja\b":   "kuja",
    r"\bkj\.\b":   "kuja",
    r"\btori\b":   "tori",
    r"\baukio\b":  "aukio",
    r"\bgatan\b":  "gatan",
    r"\bvägen\b":  "vägen",
}

def normalize_address(address: str) -> str:
    """
    Palauttaa normalisoituun muotoon:
    - lowercase
    - poistetaan kaupunginosa/kaupunki (pilkun jälkeen)
    - normalisoidaan huoneistotunnus: "4 E" / "4E" / "4 e" → "4e"
    - poistetaan ylimääräiset välit
    - normalisoidaan lyhenteet
    """
    if not address:
        return ""

    # Poista kaupunginosa ja kaupunki (pilkun jälkeen)
    addr = address.split(",")[0].strip().lower()

    # Normalisoi huoneistotunnus: numero + väli + kirjain(et) → numerokirjain
    # Esim: "4 E" → "4e", "12 A 3" → "12a3", "4B" → "4b"
    addr = re.sub(r"(\d)\s+([a-zäöå])(\s|$)", r"\1\2\3", addr)
    addr = re.sub(r"(\d)\s+([a-zäöå]\d+)", r"\1\2", addr)

    # Normalisoi lyhenteet
    for pattern, replacement in _ABBREVS.items():
        addr = re.sub(pattern, replacement, addr)

    # Poista ylimääräiset välit ja väliviivat
    addr = re.sub(r"\s+", " ", addr).strip()
    addr = re.sub(r"\s*-\s*", "-", addr)

    return addr


# ---------------------------------------------------------------------------
# Pisteytys
# ---------------------------------------------------------------------------

def match_score(prop, data: dict) -> int:
    """
    Laskee pisteet (0–100) sille kuinka todennäköisesti data vastaa
    olemassa olevaa Property-objektia.
    """
    score = 0

    # Osoite (50 pistettä)
    norm_prop = normalize_address(prop.canonical_address or "")
    norm_data = normalize_address(data.get("address") or "")
    if norm_prop and norm_data:
        if norm_prop == norm_data:
            score += 50
        elif _address_prefix_match(norm_prop, norm_data):
            score += 30   # sama katu + numero mutta eri huoneistotunnus

    # Koko m² (25 pistettä, toleranssi ±2 m²)
    size_prop = prop.size_m2
    size_data = data.get("size_m2")
    if size_prop and size_data:
        diff = abs(size_prop - size_data)
        if diff < 1:
            score += 25
        elif diff < 2:
            score += 18
        elif diff < 5:
            score += 8

    # Kerros (15 pistettä)
    floor_prop = (prop.floor or "").strip()
    floor_data = (data.get("floor") or "").strip()
    if floor_prop and floor_data and floor_prop == floor_data:
        score += 15

    # Rakennusvuosi (10 pistettä)
    year_prop = prop.year_built
    year_data = data.get("year_built")
    if year_prop and year_data and year_prop == year_data:
        score += 10

    return score


def _address_prefix_match(a: str, b: str) -> bool:
    """Tarkistaa onko katuosoite + numero sama (ilman huoneistotunnusta)."""
    # Ota kadunnimi + numero: "sundsberginkuja 4e" → "sundsberginkuja 4"
    m_a = re.match(r"^(.*\d+)", a)
    m_b = re.match(r"^(.*\d+)", b)
    if m_a and m_b:
        return m_a.group(1).rstrip() == m_b.group(1).rstrip()
    return False


# ---------------------------------------------------------------------------
# Pääfunktio
# ---------------------------------------------------------------------------

def find_or_create_property(db, Property, data: dict) -> "Property":
    """
    Etsii olemassa olevan Property-rivin joka vastaa dataa.
    Jos löytyy (score >= MATCH_THRESHOLD) → palauttaa sen.
    Jos ei → luo uuden.

    Käyttö:
        prop = find_or_create_property(db, Property, listing_data)
        listing.property_id = prop.id
    """
    from sqlalchemy import or_

    # Hae vain saman postinumeron kohteet (rajoittaa haussa käytävät rivit)
    postal = data.get("postal_code", "")
    candidates = Property.query.filter_by(postal_code=postal).all()

    best_score = 0
    best_prop = None

    for prop in candidates:
        s = match_score(prop, data)
        if s > best_score:
            best_score = s
            best_prop = prop

    if best_score >= MATCH_THRESHOLD:
        log.debug(
            "Matched '%s' → property #%d '%s' (score=%d)",
            data.get("address"), best_prop.id, best_prop.canonical_address, best_score,
        )
        return best_prop

    # Luo uusi
    prop = Property(
        canonical_address = data.get("address", ""),
        postal_code       = postal,
        city              = data.get("city", ""),
        neighborhood      = data.get("neighborhood", ""),
        property_type     = data.get("property_type", ""),
        size_m2           = data.get("size_m2"),
        floor             = data.get("floor", ""),
        year_built        = data.get("year_built"),
    )
    db.session.add(prop)
    db.session.flush()   # saa prop.id käyttöön
    log.debug("New property #%d '%s'", prop.id, prop.canonical_address)
    return prop
