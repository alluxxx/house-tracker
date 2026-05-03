"""
Laskee kohteen houkuttelevuuspisteet yhdistämällä LLM-analyysin ja
numeeriset suhteelliset mittarit alueen muihin kohteisiin.

Pisteytys (0–100):
  Neliöhinta vs. alueen keskiarvo   28 pts  (tärkein)
  LLM-analyysin laatu               22 pts
  Kunto vs. muut kohteet            14 pts
  Rakennusvuosi                     12 pts
  Tontin omistus                     8 pts  (vuokratontti iso miinus)
  Vastike €/m²                       7 pts
  Yhtiölaina suhteessa hintaan       5 pts
  Hintakehitys (laskut/nousut)       2 pts
  Aika markkinoilla vs. alue         2 pts
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


_CONDITION_PTS = {
    "uusi":          15,
    "uudenveroinen": 14,
    "erinomainen":   13,
    "hyvä":          10,
    "tyydyttävä":     5,
    "välttävä":       0,
}

def _condition_score(condition: Optional[str], all_conditions: list[int]) -> int:
    """0–15 pistettä absoluuttisen kuntoluokituksen mukaan."""
    if not condition:
        return 7
    return _CONDITION_PTS.get(condition.lower(), 7)


def _price_score(price_per_m2: Optional[float], avg: float, std: float) -> int:
    """0–28 pistettä. Halvempi suhteessa keskiarvoon = enemmän pisteitä."""
    if not price_per_m2 or not avg:
        return 14  # neutraali
    diff_pct = (avg - price_per_m2) / avg  # positiivinen = halvempi kuin keskiarvo
    if diff_pct >= 0.20:   return 28
    if diff_pct >= 0.12:   return 24
    if diff_pct >= 0.06:   return 20
    if diff_pct >= 0.02:   return 17
    if diff_pct >= -0.02:  return 14
    if diff_pct >= -0.06:  return 10
    if diff_pct >= -0.12:  return 6
    if diff_pct >= -0.20:  return 2
    return 0


def _year_score(year_built: Optional[int]) -> int:
    """0–12 pistettä rakennusvuoden mukaan."""
    if not year_built:
        return 6
    if year_built >= 2020: return 12
    if year_built >= 2015: return 10
    if year_built >= 2010: return 8
    if year_built >= 2005: return 6
    if year_built >= 2000: return 4
    if year_built >= 1990: return 2
    return 1


def _fee_score(housing_fee: Optional[float], size_m2: Optional[float]) -> int:
    """0–7 pistettä. Matala vastike/m² = enemmän pisteitä."""
    if not housing_fee or not size_m2:
        return 6  # omakotitalot joilla ei vastiketta saavat bonuksen
    fee_per_m2 = housing_fee / size_m2
    if fee_per_m2 == 0:    return 7
    if fee_per_m2 < 1.5:   return 6
    if fee_per_m2 < 2.5:   return 5
    if fee_per_m2 < 3.5:   return 3
    if fee_per_m2 < 4.5:   return 1
    return 0


def _land_score(land_ownership: Optional[str], land_lease_fee: Optional[float]) -> int:
    """
    0–8 pistettä. Tontin omistus on kriittinen pitkän aikavälin kustannustekijä.
    Vuokratontti = jatkuva kulu joka voi nousta merkittävästi sopimusehtojen mukaan.
    """
    if land_ownership == "oma":
        return 8
    if land_ownership == "vuokra":
        # Tonttivuokra lisäkustannuksena — mitä suurempi, sitä enemmän miinus
        if land_lease_fee and land_lease_fee > 400:  return -2  # yli 400€/kk = iso rasite
        if land_lease_fee and land_lease_fee > 200:  return 1
        return 3  # vuokra mutta ei tiedeta summaa
    return 5  # ei tietoa — neutraali


def _debt_score(share_of_debt: Optional[int], price_eur: Optional[int]) -> int:
    """
    0–5 pistettä. Korkea yhtiölaina suhteessa kauppahintaan = todellinen hinta
    paljon korkeampi. Myös korkoriski ja rahoitusvastike nostavat kuukausikuluja.
    """
    if not share_of_debt or not price_eur or price_eur == 0:
        return 3  # neutraali jos ei dataa
    debt_ratio = share_of_debt / price_eur
    if debt_ratio == 0:      return 5   # ei yhtiölainaa
    if debt_ratio < 0.05:    return 5   # alle 5% — ei merkittävä
    if debt_ratio < 0.15:    return 4   # 5–15%
    if debt_ratio < 0.30:    return 3   # 15–30%
    if debt_ratio < 0.50:    return 1   # 30–50% — merkittävä rasite
    return 0                            # yli 50% — erittäin korkea


def _llm_score(analysis: Optional[dict]) -> int:
    """0–22 pistettä LLM-analyysin perusteella."""
    if not analysis:
        return 11
    base = 11
    # Positiiviset signaalit
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
    return max(0, min(22, base))


def _price_trend_score(price_history: list) -> int:
    """0–2 pistettä. Hinta laskenut = myyjä motivoitunut."""
    if len(price_history) < 2:
        return 1
    first = price_history[0].price_eur or 0
    last  = price_history[-1].price_eur or 0
    if not first:
        return 1
    change = (last - first) / first
    if change <= -0.05:   return 2   # laskenut yli 5%
    if change <= -0.02:   return 2
    if change == 0:       return 1
    return 0   # hinta noussut


def _dom_score(days_on: int, all_days: list[int]) -> int:
    """
    0–2 pistettä. Aika markkinoilla vs. alueen keskiarvo.
    """
    if not all_days:
        return 1
    avg_days = sum(all_days) / len(all_days)
    if avg_days == 0:
        return 1
    ratio = days_on / avg_days
    if ratio <= 0.25:   return 2
    if ratio <= 0.75:   return 2
    if ratio <= 1.25:   return 1
    if ratio <= 2.0:    return 0
    return -1


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

    from datetime import datetime
    def _days(l):
        if l.first_seen_at:
            return (datetime.utcnow() - l.first_seen_at).days
        return 0

    listing_days = _days(listing)
    all_days = [_days(l) for l in all_listings if l.id != listing.id]

    ps   = _price_score(listing.price_per_m2, avg_price, std_price)
    cs   = _condition_score(listing.condition, all_condition_ranks)
    ys   = _year_score(listing.year_built)
    fs   = _fee_score(listing.housing_fee_eur, listing.size_m2)
    ls   = _llm_score(listing.analysis)
    ts   = _price_trend_score(list(listing.price_history))
    ds   = _dom_score(listing_days, all_days)
    lnd  = _land_score(listing.land_ownership, listing.land_lease_fee_eur)
    dbt  = _debt_score(listing.share_of_debt_eur, listing.price_eur)
    total = max(0, min(100, ps + cs + ys + fs + ls + ts + ds + lnd + dbt))

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
        "dom_score":         ds,
        "land_score":        lnd,
        "debt_score":        dbt,
        "days_on_market":    listing_days,
        "avg_days_on_market": round(sum(all_days) / len(all_days)) if all_days else None,
        "avg_price_per_m2":  round(avg_price) if avg_price else None,
        "price_vs_avg_pct":  price_vs_avg_pct,
    }
