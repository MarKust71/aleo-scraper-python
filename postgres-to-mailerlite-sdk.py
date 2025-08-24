# file: sync_mailerlite_from_db.py
from __future__ import annotations
import os
import re
import time
import logging
from typing import Iterable, Set, Optional

import psycopg2
import psycopg2.extras

import mailerlite as MailerLite  # pip install mailerlite

# (opcjonalnie) .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----------------------------- konfiguracja -----------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

MAILERLITE_API_KEY = os.getenv("MAILERLITE_API_KEY", "")
MAILERLITE_GROUP_ID = os.getenv("MAILERLITE_GROUP_ID")  # opcjonalnie

QUERY_LIMIT = int(os.getenv("QUERY_LIMIT", "0"))  # 0 = bez limitu
BATCH_SLEEP_SEC = float(os.getenv("BATCH_SLEEP_SEC", "0.0"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "0.8"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s %(message)s")
logger = logging.getLogger("sync-mailerlite")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ----------------------------- helpers -----------------------------
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


def yield_emails_from_db(limit: int = 0) -> Iterable[str]:
    sql = """
          SELECT email
          FROM connections
          WHERE email IS NOT NULL
            AND email <> ''
            AND search_city = %s
            AND search_registry_type = %s \
          """
    params: list[object] = ["Katowice", "CEIDG"]
    if limit and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    logger.info("Łączenie z PostgreSQL (%s:%s/%s)", DB_HOST, DB_PORT, DB_NAME)
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            for row in cur:
                yield row["email"]
    finally:
        conn.close()


def mailerlite_client(api_key: str) -> "MailerLite.Client":
    if not api_key:
        raise RuntimeError("Brak MAILERLITE_API_KEY w środowisku.")
    return MailerLite.Client({"api_key": api_key})


def create_or_update_subscriber(
        client: "MailerLite.Client",
        email: str,
        status: Optional[str] = "active",
        fields: Optional[dict] = None
) -> dict:
    last_err = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            return client.subscribers.create(email, status=status, fields=fields or {})
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "rate", "timeout"]) or msg.startswith("5"):
                sleep_for = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning("Błąd API (%s), próba %d/%d, czekam %.1fs...",
                               e, attempt, RETRY_COUNT, sleep_for)
                time.sleep(sleep_for)
                continue
            raise
    raise last_err


def assign_to_group_if_needed(client: "MailerLite.Client", subscriber_id: int, group_id: Optional[int]) -> None:
    if not group_id:
        return
    try:
        client.subscribers.assign_subscriber_to_group(subscriber_id, int(group_id))
    except Exception as e:
        logger.info("Nie udało się przypisać do grupy (%s). Kontynuuję.", e)


# ----------------------------- main -----------------------------
def run() -> None:
    client = mailerlite_client(MAILERLITE_API_KEY)

    raw_emails = yield_emails_from_db(limit=QUERY_LIMIT)  # ustaw QUERY_LIMIT=1 w .env na pierwszy run
    seen: Set[str] = set()
    to_process = []
    for em in raw_emails:
        em = em.strip()
        if not valid_email(em):
            continue
        key = em.lower()
        if key in seen:
            continue
        seen.add(key)
        to_process.append(em)

    logger.info("Do dodania: %d adresów (po filtrach i deduplikacji).", len(to_process))

    for idx, email in enumerate(to_process, 1):
        # status=None
        status="active"
        logger.info("[%d/%d] Upsert subskrybenta: %s", idx, len(to_process), email)
        try:
            resp = create_or_update_subscriber(
                client,
                email=email,
                status=status,
                fields={
                    "created_from_api": "aleo-scraper"
                }
            )
            subscriber_id = resp.get("data", {}).get("id") or resp.get("id")
            if subscriber_id and MAILERLITE_GROUP_ID:
                assign_to_group_if_needed(client, int(subscriber_id), int(MAILERLITE_GROUP_ID))
        except Exception as e:
            logger.error("Błąd przy przetwarzaniu %s: %s", email, e)
        if BATCH_SLEEP_SEC > 0:
            time.sleep(BATCH_SLEEP_SEC)

    logger.info("Zakończono synchronizację.")


if __name__ == "__main__":
    run()
