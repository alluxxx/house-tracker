"""
Analysoi asuntoilmoituksen tekstin Claude API:n avulla.
Palauttaa strukturoidun JSON-objektin.
"""
import json
import logging
import os

log = logging.getLogger(__name__)

PROMPT = """Olet suomalaisen kiinteistömarkkinan asiantuntija. Analysoi seuraava asuntoilmoitusteksti ja palauta analyysi JSON-muodossa.

Palauta VAIN validi JSON, ei muuta tekstiä. Käytä tätä rakennetta:
{
  "energy_class": "A|B|C|D|E|F|G|null",
  "land_ownership": "oma|vuokra|null",
  "amenities": ["sauna", "parveke", "autotalli", "pesutupa", "varastotila"],
  "renovations_done": ["esim. katto 2020", "ikkunat 2018"],
  "renovations_upcoming": ["esim. putkiremontti", "julkisivuremontti"],
  "sentiment_score": 0-100,
  "key_positives": ["max 3 lyhyttä pointtia"],
  "key_negatives": ["max 3 lyhyttä pointtia"],
  "urgency_signals": ["esim. vapautuu heti", "nopea kauppa mahdollinen"],
  "summary_fi": "1-2 lauseen tiivistelmä suomeksi"
}

Ilmoitusteksti:
"""


def analyze_listing(description: str) -> dict | None:
    """
    Lähettää ilmoitustekstin Claude Haikulle ja palauttaa strukturoidun analyysin.
    Palauttaa None jos API-avain puuttuu tai kutsu epäonnistuu.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key or not description:
        return None

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[
                {"role": "user", "content": PROMPT + description[:3000]}
            ],
        )

        raw = message.content[0].text.strip()
        # Varmista että JSON on validi
        return json.loads(raw)

    except json.JSONDecodeError as exc:
        log.warning("Analyysi palautti virheellistä JSONia: %s", exc)
        return None
    except Exception as exc:
        log.error("Analyysi epäonnistui: %s", exc)
        return None
