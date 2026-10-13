"""
Ingest biodiversity occurrence records from GBIF into PostgreSQL.
Usage:
    python -m pipelines.ingestion.ingest_gbif
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
from sqlalchemy import (
    BigInteger,
    DateTime,
    Numeric,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.engine import URL


GBIF_API_URL = "https://api.gbif.org/v1/occurrence/search"
DEFAULT_COUNTRY = "US"
DEFAULT_LIMIT = 100
PAGE_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class GBIFOccurrence(Base):
    """Raw, normalized fields from a GBIF occurrence record."""

    __tablename__ = "gbif_occurrences"
    __table_args__ = {"schema": "raw"}

    gbif_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True
    )
    scientific_name: Mapped[str | None] = mapped_column(Text)
    species: Mapped[str | None] = mapped_column(Text)
    taxon_key: Mapped[int | None] = mapped_column(BigInteger)
    event_date: Mapped[str | None] = mapped_column(String(100))
    country_code: Mapped[str | None] = mapped_column(String(2))
    state_province: Mapped[str | None] = mapped_column(Text)
    decimal_latitude: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 7)
    )
    decimal_longitude: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 7)
    )
    basis_of_record: Mapped[str | None] = mapped_column(String(100))
    dataset_key: Mapped[str | None] = mapped_column(String(100))
    source_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


def get_database_url() -> URL:
    """Build a database URL without hardcoding credentials."""
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is required.")

    return URL.create(
        drivername="postgresql+psycopg",
        username=os.getenv("POSTGRES_USER", "bip"),
        password=password,
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "biodiversity"),
    )


def fetch_occurrences(
    country: str = DEFAULT_COUNTRY,
    limit: int = DEFAULT_LIMIT,
) -> list[dict]:
    """Fetch a bounded sample of georeferenced GBIF records."""
    if not 1 <= limit <= 10000:
        raise ValueError("Limit must be between 1 and 10,000.")

    records = []
    offset = 0

    with requests.Session() as session:
        while len(records) < limit:
            page_limit = min(PAGE_SIZE, limit - len(records))

            response = session.get(
                GBIF_API_URL,
                params={
                    "country": country,
                    "hasCoordinate": "true",
                    "limit": page_limit,
                    "offset": offset,
                },
                headers={
                    "User-Agent": "BiodiversityIntelligencePlatform/0.1"
                },
                timeout=(5, 30),
            )
            response.raise_for_status()
            payload = response.json()

            results = payload.get("results", [])
            if not results:
                break

            records.extend(results)
            offset += len(results)

            if offset >= payload.get("count", 0):
                break

    logger.info("Fetched %d records from GBIF.", len(records))
    return records


def to_decimal(value):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def normalize_record(record: dict, retrieved_at: datetime) -> dict | None:
    """Map a GBIF response to our raw table's columns."""
    gbif_id = record.get("key")
    if gbif_id is None:
        logger.warning("Skipping record without a GBIF key.")
        return None

    latitude = to_decimal(record.get("decimalLatitude"))
    longitude = to_decimal(record.get("decimalLongitude"))

    # Reject invalid coordinates rather than storing them as valid.
    if (
        latitude is None
        or longitude is None
        or not Decimal("-90") <= latitude <= Decimal("90")
        or not Decimal("-180") <= longitude <= Decimal("180")
    ):
        logger.debug("Skipping record %s with invalid coordinates.", gbif_id)
        return None

    taxon_key = record.get("taxonKey")

    return {
        "gbif_id": int(gbif_id),
        "scientific_name": record.get("scientificName"),
        "species": record.get("species"),
        "taxon_key": int(taxon_key) if taxon_key is not None else None,
        "event_date": record.get("eventDate"),
        "country_code": record.get("countryCode"),
        "state_province": record.get("stateProvince"),
        "decimal_latitude": latitude,
        "decimal_longitude": longitude,
        "basis_of_record": record.get("basisOfRecord"),
        "dataset_key": record.get("datasetKey"),
        "source_payload": record,
        "retrieved_at": retrieved_at,
    }


def load_records(engine, records: list[dict]) -> int:
    """Create the raw schema and upsert records in a transaction."""
    if not records:
        logger.warning("No valid records to load.")
        return 0

    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    Base.metadata.create_all(engine)

    statement = insert(GBIFOccurrence).values(records)
    update_columns = {
        column.name: getattr(statement.excluded, column.name)
        for column in GBIFOccurrence.__table__.columns
        if column.name != "gbif_id"
    }

    upsert = statement.on_conflict_do_update(
        index_elements=[GBIFOccurrence.gbif_id],
        set_=update_columns,
    )

    with engine.begin() as connection:
        result = connection.execute(upsert)

    return result.rowcount or 0


def main() -> int:
    try:
        country = os.getenv("GBIF_COUNTRY", DEFAULT_COUNTRY)
        limit = int(os.getenv("GBIF_LIMIT", str(DEFAULT_LIMIT)))

        engine = create_engine(
            get_database_url(),
            pool_pre_ping=True,
        )

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        records = fetch_occurrences(country=country, limit=limit)
        retrieved_at = datetime.now(timezone.utc)

        normalized = [
            item
            for record in records
            if (
                item := normalize_record(record, retrieved_at)
            ) is not None
        ]

        affected = load_records(engine, normalized)

        logger.info(
            "Ingestion complete: fetched=%d valid=%d affected=%d",
            len(records),
            len(normalized),
            affected,
        )
        engine.dispose()
        return 0

    except (requests.RequestException, ValueError, RuntimeError):
        logger.exception("GBIF ingestion failed.")
        return 1
    except Exception:
        logger.exception("Unexpected ingestion failure.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

