import atexit
import logging
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import desc, func
from sqlalchemy.orm import subqueryload

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App + DB
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

_db_url = os.environ.get("DATABASE_URL", "sqlite:///sundsberg.db")
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = _db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

from models import db, Listing, PriceHistory, ScrapeRun, Property  # noqa: E402

db.init_app(app)

with app.app_context():
    db.create_all()

# ---------------------------------------------------------------------------
# Scrape job
# ---------------------------------------------------------------------------

PRICE_DROP_THRESHOLD = 0.03   # ilmoita jos hinta laskee yli 3 %


def _send_notification(subject: str, body_html: str):
    """Lähetä sähköposti ADMIN_EMAIL:iin Resend-palvelun kautta."""
    import requests as _requests

    admin = os.environ.get("ADMIN_EMAIL")
    api_key = os.environ.get("RESEND_API_KEY")

    if not all([admin, api_key]):
        log.debug("Resend ei konfiguroitu, ohitetaan ilmoitus: %s", subject)
        return

    try:
        resp = _requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "from": "House Tracker <onboarding@resend.dev>",
                "to": [admin],
                "subject": subject,
                "html": body_html,
            },
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Ilmoitus lähetetty: %s", subject)
    except Exception as exc:
        log.error("Sähköpostin lähetys epäonnistui: %s", exc)


def _new_listing_email(listings: list) -> str:
    rows = "".join(
        f"<tr><td><a href='{l.url}'>{l.address}</a></td>"
        f"<td>{l.price_eur:,} €</td>"
        f"<td>{l.size_m2 or '–'} m²</td>"
        f"<td>{l.property_type or '–'}</td>"
        f"<td>{l.year_built or '–'}</td></tr>"
        for l in listings
    )
    return f"""<h2>🏠 {len(listings)} uusi asuntoilmoitus Sundssbergissä</h2>
<table border='1' cellpadding='6' style='border-collapse:collapse;font-family:sans-serif'>
<tr><th>Osoite</th><th>Hinta</th><th>Koko</th><th>Tyyppi</th><th>Vuosi</th></tr>
{rows}
</table>"""


def _price_drop_email(listing, old_price: int, new_price: int) -> str:
    drop_pct = (old_price - new_price) / old_price * 100
    return f"""<h2>📉 Hinta laskenut {drop_pct:.1f}% — {listing.address}</h2>
<p><b>Vanha hinta:</b> {old_price:,} €<br>
<b>Uusi hinta:</b> {new_price:,} €<br>
<b>Lasku:</b> {old_price - new_price:,} €</p>
<p><a href='{listing.url}'>Katso ilmoitus →</a></p>"""


def run_scrape():
    from scraper import scrape_all
    from property_matcher import find_or_create_property
    log.info("Scrape started")
    source_results = scrape_all()

    with app.app_context():
        for source, listings in source_results.items():
            run = ScrapeRun(source=source, started_at=datetime.utcnow())
            db.session.add(run)

            if listings is None:
                run.ok = False
                run.error = "Scraper crashed — see logs"
                run.finished_at = datetime.utcnow()
                db.session.commit()
                continue

            active_ids = {
                row[0]
                for row in db.session.query(Listing.external_id)
                             .filter_by(source=source, is_active=True)
                             .all()
            }
            scraped_ids = set()
            new_listings = []
            price_drops = []
            new_count = updated_count = 0

            for data in listings:
                ext_id = data["external_id"]
                scraped_ids.add(ext_id)

                existing = Listing.query.filter_by(
                    source=source, external_id=ext_id
                ).first()

                if existing is None:
                    prop = find_or_create_property(db, Property, data)
                    listing = Listing(**data, property_id=prop.id)
                    db.session.add(listing)
                    db.session.flush()   # saa listing.id käyttöön
                    # Kirjaa aloitushinta historiaan
                    if listing.price_eur:
                        db.session.add(PriceHistory(
                            listing_id=listing.id,
                            price_eur=listing.price_eur,
                        ))
                    new_listings.append(listing)
                    new_count += 1
                else:
                    new_price = data.get("price_eur")
                    old_price = existing.price_eur

                    # Hintahistoria — kirjaa jos hinta muuttui
                    if new_price and new_price != old_price:
                        db.session.add(PriceHistory(
                            listing_id=existing.id,
                            price_eur=new_price,
                        ))
                        # Tarkista onko lasku yli kynnysarvon
                        if old_price and new_price < old_price:
                            drop = (old_price - new_price) / old_price
                            if drop >= PRICE_DROP_THRESHOLD:
                                price_drops.append((existing, old_price, new_price))

                    changed = False
                    for key, val in data.items():
                        if getattr(existing, key) != val:
                            setattr(existing, key, val)
                            changed = True
                    existing.is_active = True
                    existing.last_seen_at = datetime.utcnow()
                    if changed:
                        updated_count += 1

            # Merkitse poistuneet myydyiksi
            removed_ids = active_ids - scraped_ids
            removed_count = 0
            for ext_id in removed_ids:
                listing = Listing.query.filter_by(source=source, external_id=ext_id).first()
                if listing:
                    listing.is_active = False
                    listing.sold_at = datetime.utcnow()
                    removed_count += 1

            run.new_count = new_count
            run.updated_count = updated_count
            run.removed_count = removed_count
            run.ok = True
            run.finished_at = datetime.utcnow()
            db.session.commit()

            log.info(
                "%s: +%d new, %d updated, %d removed",
                source, new_count, updated_count, removed_count,
            )

            # Sähköposti-ilmoitukset
            if new_listings:
                _send_notification(
                    f"🏠 {len(new_listings)} uusi ilmoitus Sundssbergissä",
                    _new_listing_email(new_listings),
                )
            for listing, old_p, new_p in price_drops:
                drop_pct = (old_p - new_p) / old_p * 100
                _send_notification(
                    f"📉 Hinta -{drop_pct:.0f}% — {listing.address}",
                    _price_drop_email(listing, old_p, new_p),
                )

    log.info("Scrape finished")


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

_scheduler = BackgroundScheduler(timezone="Europe/Helsinki", daemon=True)
_scheduler.add_job(
    run_scrape,
    trigger="cron",
    hour=7,
    minute=0,
    id="daily_scrape",
    max_instances=1,
    misfire_grace_time=3600,
)
_scheduler.start()
atexit.register(lambda: _scheduler.shutdown(wait=False))

# On startup: run immediately if it's past 07:00 and DB is empty or stale
_hki = ZoneInfo("Europe/Helsinki")
_now_hki = datetime.now(_hki)
with app.app_context():
    last_run = ScrapeRun.query.order_by(desc(ScrapeRun.started_at)).first()
    last_run_date = last_run.started_at.date() if last_run else None

if _now_hki.hour >= 7 and last_run_date != date.today():
    _scheduler.add_job(
        run_scrape,
        trigger="date",
        run_date=datetime.now(_hki),
        id="startup_scrape",
        max_instances=1,
    )
    log.info("Startup scrape scheduled immediately")

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    active = (
        Listing.query
        .filter_by(is_active=True)
        .options(subqueryload(Listing.price_history))
        .order_by(desc(Listing.first_seen_at))
        .all()
    )
    stats = _get_stats()
    last_runs = ScrapeRun.query.order_by(desc(ScrapeRun.started_at)).limit(5).all()
    return render_template("index.html", listings=active, stats=stats, last_runs=last_runs, now=datetime.utcnow())


@app.route("/api/listings")
def api_listings():
    active = Listing.query.filter_by(is_active=True).order_by(desc(Listing.first_seen_at)).all()
    return jsonify([l.to_dict() for l in active])


@app.route("/api/listings/all")
def api_listings_all():
    all_listings = Listing.query.order_by(desc(Listing.first_seen_at)).all()
    return jsonify([l.to_dict() for l in all_listings])


@app.route("/api/stats")
def api_stats():
    return jsonify(_get_stats())


@app.route("/api/scrape", methods=["POST"])
def api_scrape_now():
    _scheduler.add_job(
        run_scrape,
        trigger="date",
        run_date=datetime.now(_hki),
        id=f"manual_scrape_{int(datetime.utcnow().timestamp())}",
        max_instances=1,
    )
    return jsonify({"status": "started"})


def _get_stats() -> dict:
    active = db.session.query(func.count(Listing.id)).filter_by(is_active=True).scalar() or 0
    avg_price = db.session.query(func.avg(Listing.price_eur)).filter_by(is_active=True).scalar()
    avg_m2 = db.session.query(func.avg(Listing.price_per_m2)).filter_by(is_active=True).scalar()
    min_price = db.session.query(func.min(Listing.price_eur)).filter_by(is_active=True).scalar()
    max_price = db.session.query(func.max(Listing.price_eur)).filter_by(is_active=True).scalar()
    total_ever = db.session.query(func.count(Listing.id)).scalar() or 0
    last_run = ScrapeRun.query.filter_by(ok=True).order_by(desc(ScrapeRun.started_at)).first()

    return {
        "active_listings":  active,
        "total_ever":       total_ever,
        "avg_price_eur":    round(avg_price) if avg_price else None,
        "avg_price_per_m2": round(avg_m2) if avg_m2 else None,
        "min_price_eur":    min_price,
        "max_price_eur":    max_price,
        "last_scraped_at":  last_run.finished_at.isoformat() if last_run else None,
    }


if __name__ == "__main__":
    app.run(debug=True, port=5050)
