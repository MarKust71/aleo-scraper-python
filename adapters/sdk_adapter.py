# file: adapters/sdk_adapter.py
from __future__ import annotations
import logging, time
from typing import Dict, Any, Optional

import mailerlite as MailerLite  # pip install mailerlite

from common import Config, MailerLiteAdapter

logger = logging.getLogger(__name__)

class SdkAdapter(MailerLiteAdapter):
    def __init__(self, cfg: Config):
        if not cfg.mailerlite_api_key:
            raise RuntimeError("Brak MAILERLITE_API_KEY w środowisku.")
        self.cfg = cfg
        self.client = MailerLite.Client({"api_key": cfg.mailerlite_api_key})

    def upsert_subscriber(self, email: str, status: Optional[str], fields: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(1, self.cfg.retry_count + 1):
            try:
                return self.client.subscribers.create(email, status=status, fields=fields or {})
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if any(x in msg for x in ["429", "rate", "timeout"]) or msg.startswith("5"):
                    sleep_for = self.cfg.retry_backoff_base * (2 ** (attempt - 1))
                    logger.warning("Błąd API (%s), próba %d/%d, czekam %.1fs...",
                                   e, attempt, self.cfg.retry_count, sleep_for)
                    time.sleep(sleep_for)
                    continue
                raise
        raise last_err or RuntimeError("Nie udało się utworzyć/zaktualizować (SDK)")

    def assign_to_group(self, subscriber_id: int, group_id: int) -> bool:
        try:
            self.client.subscribers.assign_subscriber_to_group(subscriber_id, int(group_id))
            return True
        except Exception as e:
            logger.info("Nie udało się przypisać do grupy (%s). Kontynuuję.", e)
            return False
