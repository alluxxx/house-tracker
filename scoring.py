"""
Laskee kohteen houkuttelevuuspisteet yhdistämällä LLM-analyysin ja
numeeriset suhteelliset mittarit alueen muihin kohteisiin.

Pisteytys (0–100):
  Neliöhinta vs. alueen keskiarvo   30 pts  (tärkein)
  LLM-analyysin laatu               25 pts
  Kunto vs. muut kohteet            15 pts
  Rakennusvuosi                     15 pts
  Vastike €/m²                      10 pts
  Hintakehitys (laskut/nousut)       5 pts
"""

from __future__ import annotations
from typing import Optional


CONDITION_RANK = {
    "uusi":          5,
    "uudenveroinen": 4,
    "erinomainen":   4,
    "hyvä":          3,
    "tyydyttävä":    2,
    "välttävä":      1,
}


def _condition_score(condition: Optional[str], all_conditions: list[int]) -> int:
    """0–15 pistettä. Vertaa kohteen kuntoa muiden kuntoluokituksiin."""
    if not condition:
        return 7  # neutraali jos tieto puuttuu
    rank = CONDITION_RANK.get(condition.lower(), 3)
    if not all_conditions:
        return int((rank / 5) * 15)
    better = sum(1 for c in all_conditions if c < rank)
    percentile = better / len(all_conditions)
    return round(percentile * 15)


def _price_score(price_per_m2: Optional[float], avg: float, std: float) -> int:
    """0–30 pistettä. Halvempi suhteessa keskiarvoon = enemmän pisteitä."""
    if not price_per_m2 or not avg:
        return 15  # neutraali
    diff_pct = (avg - price_per_m2) / avg  # positiivinen = halvempi kuin keskiarvo
    if diff_pct >= 0.20:   return 30
    if diff_pct >= 0.12:   return 26
    if diff_pct >= 0.06:   return 22
    if diff_pct >= 0.02:   return 18
    if diff_pct >= -0.02:  return 15
    if diff_pct >= -0.06:  return 11
    if diff_pct >= -0.12:  return 7
    if diff_pct >= -0.20:  return 3
    return 0


def _year_score(year_built: Optional[int]) -> int:
    """0–15 pistettä rakennusvuoden mukaan."""
    if not year_built:
        return 7
    if year_built >= 2020: return 15
    if year_built >= 2015: return 13
    if year_built >= 2010: return 10
    if year_built >= 2005: return 7
    if year_built >= 2000: return 5
    if year_built >= 1990: return 3
    return 1


def _fee_score(housing_fee: Optional[float], size_m2: Optional[float]) -> int:
    """0–10 pistettä. Matala vastike/m² = enemmän pisteitä."""
    if not housing_fee or not size_m2:
        return 8  # omakotitalot joilla ei vastiketta saavat bonuksen
    fee_per_m2 = housing_fee / size_m2
    if fee_per_m2 == 0:    return 10
    if fee_per_m2 < 1.5:   return 9
    if fee_per_m2 < 2.5:   return 7
    if fee_per_m2 < 3.5:   return 5
    if fee_per_m2 < 4.5:   return 3
    return 1


def _llm_score(analysis: Optional[dict]) -> int:
    """0–25 pistettä LLM-analyysin perusteella."""
    if not analysis:
        return 12
    base = 12
    # Positiiviset signaalit
    if analysis.get("land_ownership") == "oma":         base += 5
    amenities = analysis.get("amenities") or []
    if "sauna" in amenities:                            base += 2
    if any(a in amenities for a in ["parveke", "terassi"]): base += 1
    if any(a in amenities for a in ["autotalli", "autopaikka"]): base += 1
    # Negatiiviset signaalit
    upcoming = analysis.get("renovations_upcoming") or []
    if any("putki" in r.lower() for r in upcoming):     base -= 6
    if any("julkisivu" in r.lower() for r in upcoming): base -= 4
    negatives = analysis.get("key_negatives") or []
    base -= min(len(negatives) * 2, 6)
    urgency = analysis.get("urgency_signals") or []
    if urgency:                                          base += 1
    return max(0, min(25, base))


def _price_trend_score(price_history: list) -> int:
    """0–5 pistettä. Hinta laskenut = myyjä motivoitunut."""
    if len(price_history) < 2:
        return 2
    first = price_history[0].price_eur or 0
    last  = price_history[-1].price_eur or 0
    if not first:
        return 2
    change = (last - first) / first
    if change <= -0.05:   return 5   # laskenut yli 5%
    if change <= -0.02:   return 4
    if change == 0:       return 2
    return 0   # hinta noussut


def calculate_score(listing, all_listings: list) -> dict:
    """
    Laskee yhdistetyn pistytyksen ja palauttaa dict jossa:
      total_score, price_score, condition_score, year_score,
      fee_score, llm_score, trend_score, price_vs_avg_pct
    """
    active_prices = [
        l.price_per_m2 for l in all_listings
        if l.price_per_m2 and l.id != listing.id
    ]
    avg_price  = sum(active_prices) / len(active_prices) if active_prices else 0
    # Yksinkertainen std (ei tarvita scipy)
    if len(active_prices) > 1:
        variance = sum((p - avg_price) ** 2 for p in active_prices) / len(active_prices)
        std_price = variance ** 0.5
    else:
        std_price = 0

    all_condition_ranks = [
        CONDITION_RANK.get((l.condition or "").lower(), 3)
        for l in all_listings
        if l.condition and l.id != listing.id
    ]

    ps   = _price_score(listing.price_per_m2, avg_price, std_price)
    cs   = _condition_score(listing.condition, all_condition_ranks)
    ys   = _year_score(listing.year_built)
    fs   = _fee_score(listing.housing_fee_eur, listing.size_m2)
    ls   = _llm_score(listing.analysis)
    ts   = _price_trend_score(list(listing.price_history))
    total = ps + cs + ys + fs + ls + ts

    price_vs_avg_pct = None
    if listing.price_per_m2 and avg_price:
        price_vs_avg_pct = round((listing.price_per_m2 - avg_price) / avg_price * 100, 1)

    return {
        "total_score":       total,
        "price_score":       ps,
        "condition_score":   cs,
        "year_score":        ys,
        "fee_score":         fs,
        "llm_score":         ls,
        "trend_score":       ts,
        "avg_price_per_m2":  round(avg_price) if avg_price else None,
        "price_vs_avg_pct":  price_vs_avg_pct,
    }
