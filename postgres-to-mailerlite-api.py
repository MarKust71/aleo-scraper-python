# file: sync_mailerlite_from_db.py
from __future__ import annotations
import os
import re
import time
import logging
from typing import Iterable, Set, Optional, Any

import psycopg2
import psycopg2.extras
import requests

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
MAILERLITE_API_BASE = os.getenv("MAILERLITE_API_BASE", "https://connect.mailerlite.com/api")
MAILERLITE_GROUP_ID = os.getenv("MAILERLITE_GROUP_ID")  # np. "123456" (opcjonalnie)

QUERY_LIMIT = int(os.getenv("QUERY_LIMIT", "0"))  # 0 = bez limitu
BATCH_SLEEP_SEC = float(os.getenv("BATCH_SLEEP_SEC", "0.0"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "0.8"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))  # sekundy

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s %(message)s")
logger = logging.getLogger("sync-mailerlite")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {MAILERLITE_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
})

# ----------------------------- helpers -----------------------------
def valid_email(email: str) -> bool:
    if not email:
        return False
    if not EMAIL_RE.match(email):
        return False
    role_like = {"support@", "info@", "admin@", "no-reply@", "noreply@"}
    if any(tok in email.lower() for tok in role_like):
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


def ml_upsert_subscriber(email: str, status: Optional[str] = "active", fields: Optional[dict] = None) -> dict:
    url = f"{MAILERLITE_API_BASE}/subscribers"
    payload: dict[str, Any] = {
        "email": email,
        "status": status
    }
    if fields:
        payload["fields"] = fields

    last_err = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = SESSION.post(url, json=payload, timeout=HTTP_TIMEOUT)
            if resp.status_code in (200, 201):
                return resp.json()
            if resp.status_code == 202:
                try:
                    return resp.json()
                except Exception:
                    return {"status": 202, "email": email}
            if resp.status_code == 409:
                logger.info("Subskrybent już istnieje (%s) – traktuję jako sukces.", email)
                try:
                    return resp.json()
                except Exception:
                    return {"status": 409, "email": email}
            if resp.status_code == 422:
                logger.error("Walidacja 422 dla %s: %s", email, resp.text)
                break
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_for = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning("HTTP %s z API, próba %d/%d, czekam %.1fs... Odp: %s",
                               resp.status_code, attempt, RETRY_COUNT, sleep_for, resp.text[:300])
                time.sleep(sleep_for)
                continue
            logger.error("Nieoczekiwany kod %s dla %s: %s",
                         resp.status_code, email, resp.text[:300])
            break
        except requests.RequestException as e:
            last_err = e
            sleep_for = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning("Błąd sieci (%s), próba %d/%d, czekam %.1fs...", e, attempt, RETRY_COUNT, sleep_for)
            time.sleep(sleep_for)
    if last_err:
        raise last_err
    raise RuntimeError(f"Nie udało się utworzyć/zaktualizować subskrybenta: {email}")


def ml_assign_to_group(subscriber_id: int, group_id: int) -> bool:
    url = f"{MAILERLITE_API_BASE}/subscribers/{subscriber_id}/groups/{group_id}"
    try:
        resp = SESSION.post(url, timeout=HTTP_TIMEOUT)
        if resp.status_code in (200, 201, 204):
            return True
        if resp.status_code == 409:
            return True  # już przypisany
        logger.info("Nie udało się przypisać do grupy (%s): %s", resp.status_code, resp.text[:200])
    except requests.RequestException as e:
        logger.info("Błąd sieci przy przypisywaniu do grupy: %s", e)
    return False


# ----------------------------- main -----------------------------
def run() -> None:
    if not MAILERLITE_API_KEY:
        raise RuntimeError("Brak MAILERLITE_API_KEY w środowisku.")

    raw_emails = yield_emails_from_db(limit=QUERY_LIMIT)
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
            data = ml_upsert_subscriber(
                email=email,
                status=status,
                fields={
                    "created_from_api": "aleo-scraper"
                }
            )
            subscriber_id = None
            if isinstance(data, dict):
                subscriber_id = data.get("data", {}).get("id") or data.get("id")

            if subscriber_id and MAILERLITE_GROUP_ID:
                ml_assign_to_group(int(subscriber_id), int(MAILERLITE_GROUP_ID))
        except Exception as e:
            logger.error("Błąd przy przetwarzaniu %s: %s", email, e)
        if BATCH_SLEEP_SEC > 0:
            time.sleep(BATCH_SLEEP_SEC)

    logger.info("Zakończono synchronizację.")


if __name__ == "__main__":
    run()
