"""
biznesgovpl_sync.py
-------------------
Synchronizes CEIDG/Biznes.gov.pl JSON for rows in `connections` into a new table `biznesgovpl`.

Env vars used (with sensible defaults):
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD  -> Postgres connection
- CEIDG_API_TOKEN  -> Bearer token for dane.biznes.gov.pl
- CEIDG_API_BASE   -> Base URL, default 'https://dane.biznes.gov.pl/api/ceidg/v2'

Usage:
    python biznesgovpl_sync.py --all-missing
    python biznesgovpl_sync.py --connection-id 123
    python biznesgovpl_sync.py --nip 5210123456
"""
from __future__ import annotations
import os
import json
import argparse
import time
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.extras
import requests


def get_db_conn():
    from dotenv import load_dotenv
    load_dotenv(override=True)

    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = int(os.getenv("DB_PORT", 5432))
    DB_NAME     = os.getenv("DB_NAME", "booksy_scraper")
    DB_USER     = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=30,
        gssencmode='disable',
        sslmode='prefer',
        client_encoding='UTF8'
    )


def ensure_table():
    """Create biznesgovpl table if not exists (idempotent)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS biznesgovpl (
        id SERIAL PRIMARY KEY,
        connection_id INTEGER UNIQUE REFERENCES connections(id) ON DELETE CASCADE,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    -- trigger function and trigger
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
       NEW.updated_at = NOW();
       RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS set_updated_at_biznesgovpl ON biznesgovpl;
    CREATE TRIGGER set_updated_at_biznesgovpl
    BEFORE UPDATE ON biznesgovpl
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def fetch_ceidg_by_nip(nip: str) -> Optional[Dict[str, Any]]:
    """
    Calls dane.biznes.gov.pl CEIDG API searching by NIP.
    Returns the FIRST firm JSON object on success, or None if nothing found.
    """
    base = os.getenv("CEIDG_API_BASE", "https://dane.biznes.gov.pl/api/ceidg/v3")
    token = os.getenv("CEIDG_API_TOKEN")
    if not token:
        raise RuntimeError("Missing CEIDG_API_TOKEN env var")

    url = f"{base}/firma"
    params = {"nip": nip}
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code == 404 or resp.status_code == 204:
        return None
    resp.raise_for_status()
    payload = resp.json()

    # v2 returns {'count': X, 'items': [ ... ]} ; v3 returns similar but check defensively
    items = None
    if isinstance(payload, dict):
        # common patterns
        for key in ("items", "data", "firma", "results"):
            if key in payload and isinstance(payload[key], list):
                items = payload[key]
                break
    if items is None and isinstance(payload, list):
        items = payload

    if not items:
        return None

    # choose the first matching item
    return items[0]


def upsert_biznesgovpl(connection_id: int, json_data: Dict[str, Any]) -> None:
    """UPSERT JSON data into biznesgovpl by connection_id."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO biznesgovpl (connection_id, data)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (connection_id) DO UPDATE
                SET data = EXCLUDED.data, updated_at = CURRENT_TIMESTAMP
                """,
                (connection_id, json.dumps(json_data))
            )
        conn.commit()


def find_connection_by_nip(nip: str) -> Optional[Dict[str, Any]]:
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, nip, name FROM connections WHERE nip = %s",
                (nip,)
            )
            row = cur.fetchone()
            return dict(row) if row else None


def sync_one_by_connection_id(conn_id: int, backoff: float = 0.0) -> bool:
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT id, nip, name FROM connections WHERE id = %s", (conn_id,))
            row = cur.fetchone()
            if not row:
                print(f"[WARN] No connection with id={conn_id}")
                return False
            nip = row["nip"]
            if not nip:
                print(f"[WARN] connection id={conn_id} has no NIP")
                return False
            # polite pacing
            if backoff:
                time.sleep(backoff)
            data = fetch_ceidg_by_nip(nip)
            if not data:
                print(f"[INFO] No CEIDG data for NIP={nip} (id={conn_id})")
                return False
            upsert_biznesgovpl(conn_id, data)
            print(f"[OK] Saved CEIDG for id={conn_id}, NIP={nip}")
            return True


def sync_all_missing(limit: int = 1, backoff: float = 0.2) -> int:
    """
    For every connection with NIP and no biznesgovpl row, fetch & upsert.
    Returns number of rows updated/inserted.
    """
    count = 0
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT c.id, c.nip
                FROM connections c
                LEFT JOIN biznesgovpl b ON b.connection_id = c.id
                WHERE b.id IS NULL AND c.nip IS NOT NULL
                ORDER BY c.id
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    for row in rows:
        if backoff:
            time.sleep(backoff)
        data = fetch_ceidg_by_nip(row["nip"])
        if data:
            upsert_biznesgovpl(row["id"], data)
            count += 1
            print(f"[OK] id={row['id']} NIP={row['nip']}")
        else:
            print(f"[MISS] id={row['id']} NIP={row['nip']}")

    print(f"[DONE] Upserted {count} rows")
    return count


def main():
    parser = argparse.ArgumentParser(description="Sync Biznes.gov.pl (CEIDG) JSON to Postgres table biznesgovpl.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--all-missing", action="store_true", help="Fetch for all connections missing biznesgovpl rows.")
    g.add_argument("--connection-id", type=int, help="Fetch for a specific connections.id")
    g.add_argument("--nip", type=str, help="Fetch for a given NIP; will map to existing connection by NIP.")
    parser.add_argument("--limit", type=int, default=1, help="Max rows for --all-missing")
    parser.add_argument("--backoff", type=float, default=0.2, help="Sleep seconds between API calls")
    args = parser.parse_args()

    ensure_table()

    if args.connection_id:
        ok = sync_one_by_connection_id(args.connection_id, backoff=args.backoff)
        return 0 if ok else 2

    if args.nip:
        row = find_connection_by_nip(args.nip)
        if not row:
            print(f"[ERR] No connection found for NIP={args.nip}")
            return 2
        ok = sync_one_by_connection_id(row["id"], backoff=args.backoff)
        return 0 if ok else 2

    if args.all_missing:
        sync_all_missing(limit=args.limit, backoff=args.backoff)
        return 0

    return 0


# %%
sync_all_missing(limit=100, backoff=0.2)


# %%
if __name__ == "__main__":
    raise SystemExit(main())
