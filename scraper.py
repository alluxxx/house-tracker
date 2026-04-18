"""
Scrapes active house listings in Sundsberg (Kirkkonummi) from Oikotie and Etuovi
using Playwright (headless Chromium) so JS-rendered content loads properly.
"""
import logging
import re
import time
from typing import Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

log = logging.getLogger(__name__)

SUNDSBERG_POSTAL = "02430"

# Oikotie location ID for Sundsberg, Kirkkonummi  (from autocomplete API)
# URL fragment: locations=[[4270,4,"Sundsberg, Kirkkonummi"]]
OIKOTIE_SEARCH_URL = (
    "https://asunnot.oikotie.fi/myytavat-asunnot"
    "?pagination=1"
    "&locations=%5B%5B4270%2C4%2C%22Sundsberg%2C%20Kirkkonummi%22%5D%5D"
    "&cardType=100%2C101%2C102%2C103%2C104"
)

ETUOVI_SEARCH_URL = (
    f"https://www.etuovi.com/myytavat-asunnot?postcode={SUNDSBERG_POSTAL}"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(re.sub(r"[^\d]", "", str(val)))
    except (ValueError, TypeError):
        return None


def _float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        cleaned = re.sub(r"[^\d,\.]", "", str(val)).replace(",", ".")
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def _dismiss_consent(page):
    """Click 'Hyväksy kaikki' in the CMP iframe and force-remove the overlay."""
    for frame in page.frames:
        if "cmpv2" in frame.url or "sp_message" in frame.url:
            try:
                frame.click("button:has-text('Hyväksy kaikki')", timeout=6000)
                time.sleep(1)
            except Exception:
                pass
    # Force-remove the overlay element regardless
    page.evaluate(
        "() => document.querySelectorAll(\"[id*='sp_message']\").forEach(el => el.remove())"
    )
    time.sleep(0.5)


def _new_browser_context(pw):
    return pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    ).new_context(
        locale="fi-FI",
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
    )


# ---------------------------------------------------------------------------
# Oikotie – DOM scraping of rendered Angular cards
# ---------------------------------------------------------------------------

OIKOTIE_CARD_TYPES = {
    "kerrostalo": "kerrostalo",
    "rivitalo":   "rivitalo",
    "omakotitalo": "omakotitalo",
    "paritalo":   "paritalo",
    "erillistalo": "omakotitalo",
    "luhtitalo":  "luhtitalo",
}


def _parse_oikotie_card(card, seen: set) -> Optional[dict]:
    try:
        link = card.query_selector("a[href*='myytavat-asunnot']")
        if not link:
            return None
        href = link.get_attribute("href") or ""
        ext_id = re.search(r"/(\d+)$", href)
        ext_id = ext_id.group(1) if ext_id else href
        if not ext_id or ext_id in seen:
            return None
        seen.add(ext_id)

        txt = card.inner_text()
        lines = [l.strip() for l in txt.split("\n") if l.strip()]

        address = lines[0] if lines else ""

        price_match     = re.search(r"([\d\s]{4,10})\s*€", txt)
        size_match      = re.search(r"([\d,]+)\s*/\s*[\d,]+\s*m²|([\d,]+)\s*m²", txt)
        rooms_match     = re.search(r"Huoneita\s+(\d+)", txt)
        floor_match     = re.search(r"Kerros\s+([\d]+)\s*/\s*([\d]+)", txt)
        type_match      = re.search(
            r"(Kerrostalo|Rivitalo|Omakotitalo|Paritalo|Luhtitalo|Erillistalo)",
            txt, re.I
        )
        year_match      = re.search(r",\s*(19|20)\d{2}\b", txt)
        debt_match      = re.search(r"[Vv]elaton\s+(?:hinta\s+)?([\d\s]{4,10})\s*€", txt)
        fee_match       = re.search(r"([\d,]+)\s*€\s*/\s*kk", txt)
        condition_match = re.search(
            r"\b(Erinomainen|Hyvä|Tyydyttävä|Välttävä|Uusi|Uudenveroinen)\b", txt
        )

        price    = _int(price_match.group(1)) if price_match else None
        raw_size = size_match.group(1) or size_match.group(2) if size_match else None
        size_m2  = _float(raw_size)
        floor    = f"{floor_match.group(1)}/{floor_match.group(2)}" if floor_match else ""
        prop_type = type_match.group(1).lower() if type_match else ""
        prop_type = OIKOTIE_CARD_TYPES.get(prop_type, prop_type)
        year     = int(year_match.group().strip(", ")) if year_match else None
        debt_free = _int(debt_match.group(1)) if debt_match else None
        fee       = _float(fee_match.group(1)) if fee_match else None
        condition = condition_match.group(1) if condition_match else ""

        # Skip listings outside Sundsberg — check full card text and address
        known_non_sundsberg = {"masala", "veikkola", "jorvas", "tolsa", "kantvik",
                               "lapinkylä", "porkkala", "strömsby", "framnäs",
                               "nupuri", "luoma", "lappböle"}
        full_lower = txt.lower()
        if any(kw in full_lower for kw in known_non_sundsberg):
            log.debug("Skipping non-Sundsberg listing: %s", address)
            return None

        return {
            "source":        "oikotie",
            "external_id":   ext_id,
            "url":           href,
            "address":       address,
            "postal_code":   SUNDSBERG_POSTAL,
            "city":          "Kirkkonummi",
            "neighborhood":  "Sundsberg",
            "property_type": prop_type,
            "rooms":         rooms_match.group(1) if rooms_match else "",
            "size_m2":       size_m2,
            "price_eur":     price,
            "price_per_m2":  round(price / size_m2, 0) if (price and size_m2) else None,
            "debt_free_price_eur": debt_free,
            "floor":         floor,
            "year_built":    year,
            "condition":     condition,
            "housing_fee_eur": fee,
        }
    except Exception as exc:
        log.debug("Oikotie card parse error: %s", exc)
        return None


def _scrape_detail(page, url: str) -> dict:
    """
    Visit a single Oikotie listing page and return extra fields:
    debt_free_price_eur, housing_fee_eur, condition, _neighborhood, _postal_code.

    _neighborhood and _postal_code are prefixed with _ so run_scrape can use
    them for validation without writing them to the Listing model directly.
    Returns empty dict on any failure.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        try:
            page.wait_for_selector("[class*='info-table'], [class*='details'], dt", timeout=8000)
        except PlaywrightTimeout:
            pass

        txt = page.inner_text("body")

        debt_match = re.search(
            r"[Vv]elaton\s+(?:myyntihinta|hinta)[^\d]*([\d\s]{4,10})\s*€", txt
        )
        fee_match = re.search(
            r"(?:Hoitovastike|Yhtiövastike|Vastike)[^\d]*([\d,]+)\s*€", txt
        )
        condition_match = re.search(
            r"[Kk]unto[^\n]*\n\s*(Erinomainen|Hyvä|Tyydyttävä|Välttävä|Uusi|Uudenveroinen)",
            txt
        )
        if not condition_match:
            condition_match = re.search(
                r"\b(Erinomainen|Hyvä|Tyydyttävä|Välttävä|Uusi|Uudenveroinen)\b", txt
            )

        # Kaupunginosa ja postinumero — käytetään validointiin
        neighborhood_match = re.search(
            r"(?:Kaupunginosa|Alue)[^\n]*\n\s*([^\n]{2,40})", txt
        )
        postal_match = re.search(r"\b(0\d{4})\b", txt)

        result = {}
        if debt_match:
            result["debt_free_price_eur"] = _int(debt_match.group(1))
        if fee_match:
            result["housing_fee_eur"] = _float(fee_match.group(1))
        if condition_match:
            result["condition"] = condition_match.group(1)
        if neighborhood_match:
            result["_neighborhood"] = neighborhood_match.group(1).strip()
        if postal_match:
            result["_postal_code"] = postal_match.group(1)

        return result
    except Exception as exc:
        log.debug("Detail page error %s: %s", url, exc)
        return {}


def scrape_oikotie() -> list[dict]:
    results = []

    with sync_playwright() as pw:
        ctx = _new_browser_context(pw)
        page = ctx.new_page()

        try:
            page.goto(OIKOTIE_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeout:
            log.warning("Oikotie initial load timed out")

        # Dismiss cookie consent (blocks clicks if left open)
        _dismiss_consent(page)

        # Wait for Angular to render cards
        try:
            page.wait_for_selector(".ot-card-v3-container", timeout=20000)
        except PlaywrightTimeout:
            log.warning("Oikotie: no cards appeared after consent")

        # Paginate through all result pages
        seen: set[str] = set()
        page_num = 1
        while True:
            time.sleep(1)
            cards = page.query_selector_all(".ot-card-v3-container")
            page_results = [r for c in cards if (r := _parse_oikotie_card(c, seen))]
            results.extend(page_results)
            log.debug("Oikotie page %d: %d cards", page_num, len(page_results))

            # Try next page button
            next_btn = page.query_selector("button:has-text('Seuraava')")
            if not next_btn or page_num >= 10:
                break
            try:
                next_btn.click()
                page.wait_for_selector(".ot-card-v3-container", timeout=10000)
                page_num += 1
            except Exception:
                break

        # Enrich each listing with detail-page data; validate neighborhood/postal
        detail_page = ctx.new_page()
        validated = []
        for i, listing in enumerate(results):
            extra = _scrape_detail(detail_page, listing["url"])

            # Neighborhood validation: if detail page reveals a non-Sundsberg area, drop it
            scraped_nb = extra.pop("_neighborhood", "").lower()
            scraped_pc = extra.pop("_postal_code", "")
            if scraped_nb and "sundsberg" not in scraped_nb:
                log.info("Dropping non-Sundsberg listing (neighborhood=%r): %s",
                         scraped_nb, listing["address"])
                time.sleep(0.8)
                continue
            if scraped_pc and scraped_pc != SUNDSBERG_POSTAL:
                log.info("Dropping non-Sundsberg listing (postal=%s): %s",
                         scraped_pc, listing["address"])
                time.sleep(0.8)
                continue

            listing.update(extra)
            validated.append(listing)
            log.debug("Detail %d/%d %s → %s", i + 1, len(results), listing["external_id"], extra)
            time.sleep(0.8)

        results = validated

        ctx.browser.close()

    log.info("Oikotie: scraped %d listings", len(results))
    return results


# ---------------------------------------------------------------------------
# Etuovi – DOM scraping after loading
# ---------------------------------------------------------------------------

def scrape_etuovi() -> list[dict]:
    results = []

    with sync_playwright() as pw:
        ctx = _new_browser_context(pw)
        page = ctx.new_page()

        try:
            page.goto(ETUOVI_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeout:
            log.warning("Etuovi initial load timed out")

        _dismiss_consent(page)

        try:
            page.wait_for_selector('a[href*="/kohde/"]', timeout=20000)
        except PlaywrightTimeout:
            log.warning("Etuovi: no listing links appeared")

        seen: set[str] = set()
        page_num = 1
        while True:
            time.sleep(1)
            page_results = _etuovi_extract_page(page, seen)
            results.extend(page_results)
            log.debug("Etuovi page %d: %d cards", page_num, len(page_results))

            next_btn = page.query_selector(
                "button:has-text('Seuraava'), [aria-label='Seuraava sivu']"
            )
            if not next_btn or page_num >= 10:
                break
            try:
                next_btn.click()
                page.wait_for_load_state("networkidle", timeout=10000)
                page_num += 1
            except Exception:
                break

        ctx.browser.close()

    log.info("Etuovi: scraped %d listings", len(results))
    return results


def _etuovi_extract_page(page, seen: set) -> list[dict]:
    results = []
    links = page.query_selector_all('a[href*="/kohde/"]')

    for a in links:
        try:
            href = a.get_attribute("href") or ""
            ext_id = re.search(r"/kohde/(\d+)", href)
            ext_id = ext_id.group(1) if ext_id else href
            if not ext_id or ext_id in seen:
                continue
            seen.add(ext_id)

            url = "https://www.etuovi.com" + href if href.startswith("/") else href

            card_text = a.evaluate(
                "el => el.closest('li, article, [class*=\"Card\"], [class*=\"card\"], "
                "[class*=\"Item\"], [class*=\"item\"]')?.innerText || el.innerText"
            )

            price_match  = re.search(r"([\d\s]{3,8})\s*€", card_text)
            size_match   = re.search(r"([\d,]+)\s*(?:/\s*[\d,]+\s*)?m²", card_text)
            rooms_match  = re.search(r"(\d+)\s*h(?:\W|$)", card_text)
            year_match   = re.search(r"\b(19|20)\d{2}\b", card_text)
            type_match   = re.search(
                r"(Kerrostalo|Rivitalo|Omakotitalo|Paritalo|Luhtitalo)", card_text, re.I
            )

            lines = [l.strip() for l in card_text.split("\n") if l.strip()]
            address = lines[0] if lines else ""

            # Filter: only include if address/text mentions Sundsberg/02430/Kirkkonummi
            if not any(
                kw in card_text
                for kw in ("Sundsberg", "02430", "Kirkkonummi")
            ):
                continue

            price   = _int(price_match.group(1)) if price_match else None
            size_m2 = _float(size_match.group(1)) if size_match else None

            results.append({
                "source":        "etuovi",
                "external_id":   ext_id,
                "url":           url,
                "address":       address,
                "postal_code":   SUNDSBERG_POSTAL,
                "city":          "Kirkkonummi",
                "neighborhood":  "Sundsberg",
                "property_type": type_match.group(1).lower() if type_match else "",
                "rooms":         rooms_match.group(1) if rooms_match else "",
                "size_m2":       size_m2,
                "price_eur":     price,
                "price_per_m2":  round(price / size_m2, 0) if (price and size_m2) else None,
                "debt_free_price_eur": None,
                "floor":         "",
                "year_built":    int(year_match.group()) if year_match else None,
                "condition":     "",
                "housing_fee_eur": None,
            })

        except Exception as exc:
            log.debug("Etuovi card error: %s", exc)

    return results


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

def scrape_all() -> dict[str, list[dict]]:
    results = {}
    for name, fn in [("oikotie", scrape_oikotie), ("etuovi", scrape_etuovi)]:
        try:
            results[name] = fn()
        except Exception as exc:
            log.error("Scraper %s crashed: %s", name, exc, exc_info=True)
            results[name] = None
    return results
