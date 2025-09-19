# file: api_server.py
"""
API serwer (GET) z prostą autoryzacją API-Key w nagłówku.
Wykorzystuje funkcje scrapera z pliku main.py do pobierania danych o firmie z aleo.com po NIP.

Uruchomienie:
  export API_KEY="twoj_tajny_klucz"
  # (opcjonalnie) inne zmienne środowiskowe Selenium/Chromedriver zgodnie z Twoją konfiguracją
  uvicorn api_server:app --host 0.0.0.0 --port 8000

Użycie:
  GET /?nip=1234563218
  Nagłówek: X-API-Key: <twój_klucz>

Zwracany JSON (przykład):
  {
    "query_nip": "1234563218",
    "count": 1,
    "results": [
      {
        "name": "...",
        "url": "https://aleo.com/...",
        "address": "...",
        "nip": "1234563218",
        "regon": "...",
        "krs": null,
        "phone": "...",
        "email": "...",
        "website": "...",
        "source": "aleo.com"
      }
    ]
  }
"""
from __future__ import annotations
import os, re, logging
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

# Upewnij się, że plik main.py leży w tym samym katalogu co ten moduł
# (albo że jego katalog jest na PYTHONPATH).
import importlib

try:
    scraper = importlib.import_module("main-scraper")
except Exception as e:
    raise RuntimeError("Nie mogę zaimportować scrapera z main.py. Upewnij się, że uruchamiasz w katalogu projektu.") from e

# Lokalny import BeautifulSoup – wykorzystujemy go do pobrania listy pozycji ze strony wyników,
# a właściwe parsowanie pól zostawiamy Twoim funkcjom z main.py (extract_companies, augment_companies_with_contacts).
from bs4 import BeautifulSoup

logger = logging.getLogger("api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

API_KEY_ENV = "API_KEY"
ALEO_BASE_URL = "https://aleo.com/pl"

app = FastAPI(title="Aleo Scraper API", version="1.0.0")


def _sanitize_nip(nip: str | None) -> Optional[str]:
    if not nip:
        return None
    digits = re.sub(r"\D+", "", nip)
    return digits if len(digits) == 10 else None


def _scrape_company_by_nip(nip: str) -> Dict[str, Any]:
    """Zwraca dict z danymi firmy (po NIP) lub podnosi HTTPException, jeśli nic nie znaleziono."""
    nip_digits = _sanitize_nip(nip)
    if not nip_digits:
        raise HTTPException(status_code=400, detail="Parametr 'nip' musi zawierać dokładnie 10 cyfr.")

    # 1) Nowa instancja drivera na każde zapytanie (bez współdzielenia – Selenium nie jest thread-safe).
    driver = scraper.init()
    try:
        # 2) Ustaw globalne parametry w istniejącym skrypcie
        scraper.set_globals(
            count=1,                # wystarczy jedna pozycja
            phrase=nip_digits,      # szukamy po NIP
            voivodeships="",
            city="",
            registry_type="",       # bez ograniczeń (CEIDG/KRS/REGON), bo NIP jest jednoznaczny
            page=1,
            base_url=ALEO_BASE_URL,
        )

        # 3) Otwórz stronę wyników dla zapytania po NIP
        scraper.load_aleo_page(
            driver,
            phrase=nip_digits,
            count=1,
            voivodeships="",
            city="",
            registry_type="",
            page=1,
        )

        # 4) Zbierz kontenery firm ze strony wyników i zbuduj listę bazową
        soup = BeautifulSoup(driver.page_source, "html.parser")
        companies_on_page = soup.find_all("div", class_="catalog-row-container")
        base_list: List[Dict[str, Any]] = scraper.extract_companies(companies_on_page)

        if not base_list:
            raise HTTPException(status_code=404, detail=f"Nie znaleziono firmy dla NIP {nip_digits}.")

        # 5) Jeśli na liście jest więcej niż jedna pozycja, spróbuj dopasować tę o identycznym NIP
        picked = None
        for item in base_list:
            item_nip = _sanitize_nip(item.get("nip"))
            if item_nip == nip_digits:
                picked = item
                break
        if picked is None:
            picked = base_list[0]

        # 6) Wzbogacenie o kontakt (telefon, email, strona www) z widoku szczegółowego
        enriched_list = scraper.augment_companies_with_contacts(driver, [picked], BASE_URL=ALEO_BASE_URL)
        enriched = enriched_list[0] if enriched_list else picked
        enriched["source"] = "aleo.com"
        enriched["query_nip"] = nip_digits

        return {
            "query_nip": nip_digits,
            "count": 1,
            "results": [enriched],
        }

    finally:
        try:
            driver.quit()
        except Exception:
            pass


@app.get("/api", response_class=JSONResponse)
def lookup_company(
    nip: str = Query(..., description="NIP (10 cyfr)"),
    x_api_key: Optional[str] = Header(None, convert_underscores=False)
):
    from dotenv import load_dotenv
    load_dotenv(override=True)

    expected = os.getenv("API_KEY_ENV", "")
    if not expected:
        logger.warning("Brak API_KEY w środowisku – odrzucam żądanie.")
        raise HTTPException(status_code=500, detail="Serwer nie jest skonfigurowany (brak API_KEY).")

    if not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Nieprawidłowy X-API-Key.")

    data = _scrape_company_by_nip(nip)
    return JSONResponse(content=data, status_code=200)
