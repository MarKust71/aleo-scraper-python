"""
Refaktoryzowany moduł scrapujący dane firm z Aleo i zapisujący je do PostgreSQL.
Założenia:
- Oddzielone warstwy: konfiguracja/logowanie, scraping (Selenium + BS4), model danych, persystencja (psycopg2), uruchomienie CLI.
- Brak `time.sleep` – używamy WebDriverWait.
- Typy, dataclasses, logika normalizacji pól (www/telefon/email).
- Upsert w Postgres z deduplikacją po `profile_url` (lub `website` jeśli brak profilu) i walidacją danych.
- Łatwość testów: każda część ma mały, czysty interfejs.
"""
from __future__ import annotations

import os
import re
import sys
import json
import time
import logging
import argparse
from dataclasses import dataclass, asdict, field
from typing import Iterable, Optional, List, Dict

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import psycopg2
from psycopg2.extras import Json

# ---------------------------
# Konfiguracja i logowanie
# ---------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("aleo_scraper")

BASE_URL = "https://aleo.com/pl"
SEARCH_URL = f"{BASE_URL}/firms/search"

# ---------------------------
# Model danych
# ---------------------------

PHONE_RE = re.compile(r"\d+")
WWW_RE = re.compile(r"^https?://", re.I)
EMAIL_RE = re.compile(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$")


def _norm_phone(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = "".join(PHONE_RE.findall(value))
    return digits or None


def _norm_www(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    if not WWW_RE.search(v):
        v = "https://" + v
    # usuń trailing slash
    v = v.rstrip('/')
    return v


def _norm_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip()
    return v if EMAIL_RE.match(v) else None


@dataclass
class Company:
    name: Optional[str] = None
    profile_url: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    postal_code: Optional[str] = None
    # surowe dane dodatkowe – trzymamy w JSON
    extra: Dict = field(default_factory=dict)

    def normalize(self) -> "Company":
        self.website = _norm_www(self.website)
        self.email = _norm_email(self.email)
        self.phone = _norm_phone(self.phone)
        return self


# ---------------------------
# Selenium: driver utils
# ---------------------------

def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    return driver


# ---------------------------
# Scraper
# ---------------------------

class AleoScraper:
    def __init__(self, driver: webdriver.Chrome, timeout: int = 20):
        self.driver = driver
        self.wait = WebDriverWait(driver, timeout)

    def search(self, phrase: str, count: int = 100, voivodeships: Optional[List[str]] = None) -> List[Company]:
        """Wyszukuje firmy i zwraca listę Company (cz. uzupełnionych)."""
        url = f"{SEARCH_URL}?query={phrase}"
        logger.info("Otwieram stronę wyszukiwania: %s", url)
        self.driver.get(url)
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='search-results']")))
        except TimeoutException:
            logger.warning("Nie udało się załadować wyników – spróbuję dalej.")

        companies: List[Company] = []
        seen_profiles: set[str] = set()

        while len(companies) < count:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            cards = soup.select("[data-testid='search-results'] a[href*='/firmy/']")
            if not cards:
                logger.info("Brak kolejnych kart wyników.")
                break

            for a in cards:
                href = a.get("href")
                if not href:
                    continue
                profile_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                if profile_url in seen_profiles:
                    continue
                seen_profiles.add(profile_url)

                name_el = a.select_one("h2, h3")
                name = name_el.get_text(strip=True) if name_el else None
                company = Company(name=name, profile_url=profile_url)
                companies.append(company)
                if len(companies) >= count:
                    break

            # paginacja – kliknij "następna" jeśli istnieje
            try:
                next_btn = self.driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
                self.driver.execute_script("arguments[0].click();", next_btn)
                self.wait.until(EC.staleness_of(next_btn))
            except NoSuchElementException:
                logger.info("Brak przycisku następnej strony.")
                break
            except TimeoutException:
                logger.info("Timeout przy przejściu na następną stronę.")
                break

        return companies

    def enrich_from_profile(self, companies: List[Company]) -> List[Company]:
        """Dla każdej firmy odwiedza profil i uzupełnia szczegóły."""
        original = self.driver.current_window_handle
        for c in companies:
            if not c.profile_url:
                continue
            self.driver.switch_to.new_window('tab')
            self.driver.get(c.profile_url)
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "main")))
            except TimeoutException:
                logger.warning("Nie załadował się profil: %s", c.profile_url)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # website
            site = None
            for sel in ["a[href^='http']", "[data-testid='company-website'] a"]:
                el = soup.select_one(sel)
                if el and el.get("href"):
                    site = el.get("href")
                    break
            if not site:
                # alternatywne – czasem strona jest w tekście
                site_span = soup.find("span", string=re.compile(r"www|http", re.I))
                if site_span:
                    site = site_span.get_text(strip=True)
            c.website = _norm_www(site)

            # email
            email = None
            mail_link = soup.select_one("a[href^='mailto:']")
            if mail_link:
                email = mail_link.get("href", "").replace("mailto:", "")
            if not email:
                txt = soup.get_text(" ", strip=True)
                m = re.search(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}", txt)
                email = m.group(0) if m else None
            c.email = _norm_email(email)

            # phone
            phone = None
            phone_el = soup.select_one("a[href^='tel:'], [data-testid='phone']")
            if phone_el:
                phone = phone_el.get("href", "").replace("tel:", "") or phone_el.get_text(strip=True)
            c.phone = _norm_phone(phone)

            # address – zachowujemy surowe pole i prostą heurystykę na kod/city
            addr = None
            addr_el = soup.select_one("[data-testid='company-address'], address, .address, .company-address")
            if addr_el:
                addr = addr_el.get_text(" ", strip=True)
            c.address = addr

            if addr:
                # prosta heurystyka: "00-000 Miasto" gdziekolwiek w adresie
                m = re.search(r"(\d{2}-\d{3})\s+([A-ZĄĆĘŁŃÓŚŹŻ][\wĄĆĘŁŃÓŚŹŻąęółśźćń-]+)", addr)
                if m:
                    c.postal_code = m.group(1)
                    c.city = m.group(2)

            # extra JSON z dowolnymi dodatkami
            c.extra.update({
                "source": "aleo",
                "scraped_at": int(time.time()),
            })

            logger.debug("Uzupełniono: %s", c)
            self.driver.close()
            self.driver.switch_to.window(original)

        return companies


# ---------------------------
# Persystencja Postgres
# ---------------------------

class Database:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.name = os.getenv("DB_NAME", "scraper")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "postgres")
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.name,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = False

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def ensure_schema(self):
        sql = """
              CREATE TABLE IF NOT EXISTS companies (
                                                       id SERIAL PRIMARY KEY,
                                                       name TEXT,
                                                       profile_url TEXT UNIQUE,
                                                       website TEXT,
                                                       email TEXT,
                                                       phone TEXT,
                                                       address TEXT,
                                                       city TEXT,
                                                       district TEXT,
                                                       postal_code TEXT,
                                                       extra JSONB DEFAULT '{}'::jsonb,
                                                       created_on TIMESTAMPTZ DEFAULT now(),
                                                       updated_on TIMESTAMPTZ DEFAULT now()
              );
              CREATE INDEX IF NOT EXISTS companies_website_idx ON companies(website);
              CREATE INDEX IF NOT EXISTS companies_phone_idx ON companies(phone);

              CREATE OR REPLACE FUNCTION set_updated_on()
                  RETURNS trigger AS $$
              BEGIN
                  NEW.updated_on = now();
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;

              DROP TRIGGER IF EXISTS trg_companies_updated_on ON companies;
              CREATE TRIGGER trg_companies_updated_on
                  BEFORE UPDATE ON companies
                  FOR EACH ROW EXECUTE FUNCTION set_updated_on(); \
              """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()

    def upsert_companies(self, companies: List[Company]) -> int:
        if not companies:
            return 0
        sql = """
              INSERT INTO companies (name, profile_url, website, email, phone, address, city, district, postal_code, extra)
              VALUES (%(name)s, %(profile_url)s, %(website)s, %(email)s, %(phone)s, %(address)s, %(city)s, %(district)s, %(postal_code)s, %(extra)s)
              ON CONFLICT (profile_url) DO UPDATE
                  SET name = COALESCE(EXCLUDED.name, companies.name),
                      website = COALESCE(EXCLUDED.website, companies.website),
                      email = COALESCE(EXCLUDED.email, companies.email),
                      phone = COALESCE(EXCLUDED.phone, companies.phone),
                      address = COALESCE(EXCLUDED.address, companies.address),
                      city = COALESCE(EXCLUDED.city, companies.city),
                      district = COALESCE(EXCLUDED.district, companies.district),
                      postal_code = COALESCE(EXCLUDED.postal_code, companies.postal_code),
                      extra = companies.extra || EXCLUDED.extra \
              """
        with self.conn.cursor() as cur:
            for c in companies:
                c.normalize()
                rec = asdict(c)
                rec["extra"] = Json(rec.get("extra") or {})
                cur.execute(sql, rec)
            self.conn.commit()
        return len(companies)


# ---------------------------
# CLI / main
# ---------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scraper Aleo -> Postgres")
    parser.add_argument("phrase", help="Fraza wyszukiwania, np. 'salon fryzjerski'")
    parser.add_argument("--count", type=int, default=50, help="Liczba firm do pobrania")
    parser.add_argument("--headless", action="store_true", help="Uruchom Chrome w trybie headless")
    args = parser.parse_args(argv)

    db = Database()
    try:
        db.connect()
        db.ensure_schema()

        driver = build_driver(headless=args.headless)
        scraper = AleoScraper(driver)
        try:
            companies = scraper.search(args.phrase, count=args.count)
            companies = scraper.enrich_from_profile(companies)
        finally:
            driver.quit()

        inserted = db.upsert_companies(companies)
        logger.info("Zapisano/uzupełniono rekordy: %d", inserted)
        return 0
    except Exception:
        logger.exception("Błąd wykonania")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
