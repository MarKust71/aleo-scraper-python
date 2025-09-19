# file: common.py
from __future__ import annotations
import os, re, time, logging
from dataclasses import dataclass
from pprint import pprint
from typing import Iterable, Optional, Protocol, Dict, Any, Set

import psycopg2
import psycopg2.extras

# (opcjonalnie) .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----------------------------- konfiguracja & logowanie -----------------------------
@dataclass
class Config:
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "postgres")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")

    mailerlite_api_key: str = os.getenv("MAILERLITE_API_KEY", "")
    mailerlite_api_base: str = os.getenv("MAILERLITE_API_BASE", "https://connect.mailerlite.com/api")
    mailerlite_group_id: Optional[str] = os.getenv("MAILERLITE_GROUP_ID")  # opcjonalnie

    query_limit: int = int(os.getenv("QUERY_LIMIT", "0"))   # 0 = bez limitu
    batch_sleep_sec: float = float(os.getenv("BATCH_SLEEP_SEC", "0.0"))
    retry_count: int = int(os.getenv("RETRY_COUNT", "3"))
    retry_backoff_base: float = float(os.getenv("RETRY_BACKOFF_BASE", "0.8"))
    http_timeout: float = float(os.getenv("HTTP_TIMEOUT", "10"))  # sekundy

    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

def setup_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")

logger = logging.getLogger("sync-mailerlite")

# ----------------------------- walidacja NIP -----------------------------
def _sanitize_nip(nip: str | None) -> str | None:
    if not nip:
        return None
    # Usuń wszystko poza cyframi
    digits = re.sub(r"\D+", "", nip)
    # Opcjonalnie: prosty warunek długości (PL NIP = 10 cyfr)
    if len(digits) == 10:
        return digits
    # Jeśli nie 10 cyfr – zachowaj „jak jest”, ale bez spacji/kresek
    return digits or None

# ----------------------------- walidacja email -----------------------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def valid_email(email: str) -> bool:
    if not email:
        return False
    if not EMAIL_RE.match(email):
        return False
    role_like = {"support@", "info@", "admin@", "no-reply@", "noreply@"}
    low = email.lower()
    if any(tok in low for tok in role_like):
        logger.debug("Pomijam role-based email: %s", email)
        return False
    return True

# ----------------------------- DB: źródło emaili -----------------------------
def yield_emails_from_db(cfg: Config, limit: int = 0) -> Iterable[tuple[str, Optional[str]]]:
    sql = """
          SELECT DISTINCT
              LOWER(email) AS email,
              nip
          FROM connections
          WHERE email IS NOT NULL
            AND email <> ''
            AND search_city = %s
          """
    # AND search_registry_type = %s
    # params: list[object] = ["Wrocław", "CEIDG"]
    params: list[object] = ["Wrocław"]
    if limit and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    logger.info("Łączenie z PostgreSQL (%s:%s/%s)", cfg.db_host, cfg.db_port, cfg.db_name)
    conn = psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port, dbname=cfg.db_name, user=cfg.db_user, password=cfg.db_password
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            for row in cur:
                yield row["email"], row.get("nip")
    finally:
        conn.close()

# ----------------------------- Kontrakt adaptera -----------------------------
class MailerLiteAdapter(Protocol):
    def upsert_subscriber(self, email: str, status: Optional[str], fields: Optional[Dict[str, Any]]) -> Dict[str, Any]: ...
    def assign_to_group(self, subscriber_id: int, group_id: int) -> bool: ...

# ----------------------------- Wspólna orkiestracja -----------------------------
def _extract_id(resp: Dict[str, Any]) -> Optional[int]:
    if not isinstance(resp, dict):
        return None
    return resp.get("data", {}).get("id") or resp.get("id")

def run_sync(adapter: MailerLiteAdapter, cfg: Config) -> None:
    logger.info("Start synchronizacji.")

    if not cfg.mailerlite_api_key:
        raise RuntimeError("Brak MAILERLITE_API_KEY w środowisku.")
    setup_logging(cfg.log_level)

    raw_emails = list(yield_emails_from_db(cfg, limit=cfg.query_limit))  # ustaw QUERY_LIMIT=1 w .env na pierwszy run
    total = len(raw_emails)

    seen: Set[str] = set()
    for idx, (email, nip) in enumerate(raw_emails, start=1):
        logger.info("[%d/%d] Upsert subskrybenta: %s, NIP: %s", idx, total, email, nip)

        if not email or email in seen:
            continue
        seen.add(email)

        # Zbuduj fields
        fields: Dict[str, Any] = {"created_from_api": "aleo-scraper"}
        nip_clean = _sanitize_nip(nip)
        if nip_clean:
            fields["tax_id"] = nip_clean

        status = None

        try:
            resp = adapter.upsert_subscriber(
                email=email,
                status=status,
                fields=fields,
            )

            errors = resp.get("errors", [])
            if errors:
                pprint(f'ERROR: {errors}')

            subscriber_id = _extract_id(resp)
            if subscriber_id and cfg.mailerlite_group_id:
                adapter.assign_to_group(int(subscriber_id), int(cfg.mailerlite_group_id))

        except Exception as e:
            logger.error("Błąd przy przetwarzaniu %s: %s", email, e)

        if cfg.batch_sleep_sec > 0:
            time.sleep(cfg.batch_sleep_sec)

    logger.info("Zakończono synchronizację.")
