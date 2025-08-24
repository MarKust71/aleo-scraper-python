# file: adapters/http_adapter.py
from __future__ import annotations
import logging, time
from typing import Dict, Any, Optional

import requests

from common import Config, MailerLiteAdapter

logger = logging.getLogger(__name__)

class HttpAdapter(MailerLiteAdapter):
    def __init__(self, cfg: Config):
        if not cfg.mailerlite_api_key:
            raise RuntimeError("Brak MAILERLITE_API_KEY w środowisku.")
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.mailerlite_api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def upsert_subscriber(self, email: str, status: Optional[str], fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.cfg.mailerlite_api_base}/subscribers"
        payload: Dict[str, Any] = {"email": email, "status": status}
        if fields:
            payload["fields"] = fields

        last_err: Optional[Exception] = None
        for attempt in range(1, self.cfg.retry_count + 1):
            try:
                resp = self.session.post(url, json=payload, timeout=self.cfg.http_timeout)
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
                    sleep_for = self.cfg.retry_backoff_base * (2 ** (attempt - 1))
                    logger.warning("HTTP %s z API, próba %d/%d, czekam %.1fs... Odp: %s",
                                   resp.status_code, attempt, self.cfg.retry_count, sleep_for, resp.text[:300])
                    time.sleep(sleep_for)
                    continue
                logger.error("Nieoczekiwany kod %s dla %s: %s",
                             resp.status_code, email, resp.text[:300])
                break
            except requests.RequestException as e:
                last_err = e
                sleep_for = self.cfg.retry_backoff_base * (2 ** (attempt - 1))
                logger.warning("Błąd sieci (%s), próba %d/%d, czekam %.1fs...",
                               e, attempt, self.cfg.retry_count, sleep_for)
                time.sleep(sleep_for)
        if last_err:
            raise last_err
        raise RuntimeError(f"Nie udało się utworzyć/zaktualizować subskrybenta: {email}")

    def assign_to_group(self, subscriber_id: int, group_id: int) -> bool:
        url = f"{self.cfg.mailerlite_api_base}/subscribers/{subscriber_id}/groups/{group_id}"
        try:
            resp = self.session.post(url, timeout=self.cfg.http_timeout)
            if resp.status_code in (200, 201, 204):
                return True
            if resp.status_code == 409:
                return True  # już przypisany
            logger.info("Nie udało się przypisać do grupy (%s): %s",
                        resp.status_code, resp.text[:200])
        except requests.RequestException as e:
            logger.info("Błąd sieci przy przypisywaniu do grupy: %s", e)
        return False
