from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Property(db.Model):
    """Physical apartment/house — one row per real-world dwelling."""
    __tablename__ = "properties"

    id                = db.Column(db.Integer, primary_key=True)
    canonical_address = db.Column(db.String(256))
    postal_code       = db.Column(db.String(10))
    city              = db.Column(db.String(64))
    neighborhood      = db.Column(db.String(64))
    property_type     = db.Column(db.String(32))
    size_m2           = db.Column(db.Float)
    floor             = db.Column(db.String(16))
    year_built        = db.Column(db.Integer)
    created_at        = db.Column(db.DateTime, default=datetime.utcnow)

    listings = db.relationship("Listing", backref="property", lazy="dynamic")


class Listing(db.Model):
    __tablename__ = "listings"

    id            = db.Column(db.Integer, primary_key=True)
    source        = db.Column(db.String(32), nullable=False)   # "oikotie" | "etuovi"
    external_id   = db.Column(db.String(64), nullable=False)   # listing id on source site
    url           = db.Column(db.Text, nullable=False)
    address       = db.Column(db.String(256))
    postal_code   = db.Column(db.String(10))
    city          = db.Column(db.String(64))
    neighborhood  = db.Column(db.String(64))
    property_type = db.Column(db.String(32))                   # "kerrostalo" | "omakotitalo" etc.
    rooms         = db.Column(db.String(16))
    size_m2       = db.Column(db.Float)
    floor         = db.Column(db.String(16))
    year_built    = db.Column(db.Integer)
    price_eur     = db.Column(db.Integer)                      # asking price in euros
    price_per_m2  = db.Column(db.Float)
    debt_free_price_eur = db.Column(db.Integer)                # velaton hinta
    housing_fee_eur     = db.Column(db.Float)                  # vastike €/kk
    condition     = db.Column(db.String(32))
    first_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen_at  = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    sold_at       = db.Column(db.DateTime)                     # set when listing disappears
    is_active     = db.Column(db.Boolean, default=True, nullable=False)
    property_id   = db.Column(db.Integer, db.ForeignKey("properties.id"), nullable=True)

    __table_args__ = (
        db.UniqueConstraint("source", "external_id", name="uq_source_external_id"),
    )

    def to_dict(self):
        return {
            "id":            self.id,
            "source":        self.source,
            "external_id":   self.external_id,
            "url":           self.url,
            "address":       self.address,
            "postal_code":   self.postal_code,
            "property_type": self.property_type,
            "rooms":         self.rooms,
            "size_m2":       self.size_m2,
            "price_eur":     self.price_eur,
            "price_per_m2":  self.price_per_m2,
            "debt_free_price_eur": self.debt_free_price_eur,
            "year_built":    self.year_built,
            "condition":     self.condition,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "last_seen_at":  self.last_seen_at.isoformat() if self.last_seen_at else None,
            "sold_at":       self.sold_at.isoformat() if self.sold_at else None,
            "is_active":     self.is_active,
        }


class PriceHistory(db.Model):
    __tablename__ = "price_history"

    id          = db.Column(db.Integer, primary_key=True)
    listing_id  = db.Column(db.Integer, db.ForeignKey("listings.id"), nullable=False)
    price_eur   = db.Column(db.Integer)
    recorded_at = db.Column(db.DateTime, default=datetime.utcnow)

    listing = db.relationship("Listing", backref=db.backref("price_history", order_by="PriceHistory.recorded_at"))


class ScrapeRun(db.Model):
    __tablename__ = "scrape_runs"

    id          = db.Column(db.Integer, primary_key=True)
    started_at  = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime)
    source      = db.Column(db.String(32))
    new_count   = db.Column(db.Integer, default=0)
    updated_count = db.Column(db.Integer, default=0)
    removed_count = db.Column(db.Integer, default=0)
    error       = db.Column(db.Text)
    ok          = db.Column(db.Boolean)
