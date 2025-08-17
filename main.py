# %%
# INIT
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

from selenium import webdriver
driver = webdriver.Chrome()



# %%
COUNT = 100
PHRASE = "a"
# VOIVODSHIPS = ""
VOIVODSHIPS = "SLASKIE"
CITY = "Katowice"
# CITY = "Zgorzelec"
REGISTRY_TYPE = "CEIDG"
PAGE = 1

base_url = "https://aleo.com/pl"



# %%
def get_company_count(soup) -> int | None:

    # znajdź element <span> z napisem "Baza firm"
    baza_firm_span = soup.find("span", string="Baza firm")
    if not baza_firm_span:
        return None

    # poszukaj następnego <span> z liczbą
    count_span = baza_firm_span.find_next("span", class_="tab-header-count")
    if count_span:
        text = count_span.get_text(strip=True).replace(" ", "").replace("\xa0", "")
        if text.isdigit():
            return int(text)

    return None


def get_page_count(company_count: int, company_per_page: int) -> int | None:
    if company_count is None:
        return None

    return (company_count + company_per_page - 1) // company_per_page


def load_aleo_page(
        driver,
        phrase: str = "a",
        count: int = 100,
        voivodeships: str = "SLASKIE",
        city: str = "Katowice",
        registry_type: str = "CEIDG",
        page: int = 1
) -> None:
    query_page = ""

    if phrase:
        PHRASE = phrase
    if count:
        COUNT = count
    if voivodeships:
        VOIVODSHIPS = voivodeships
    if city:
        CITY = city
    if registry_type:
        REGISTRY_TYPE = registry_type
    if page > 1:
        PAGE = page
        query_page = f"/{PAGE}"

    ALEO_PAGE_URL=f"{base_url}/firmy{query_page}?phrase={PHRASE}&count={COUNT}"
    if VOIVODSHIPS:
        ALEO_PAGE_URL += f"&voivodeships={VOIVODSHIPS}"
    if CITY:
        ALEO_PAGE_URL += f"&city={CITY}"
    if REGISTRY_TYPE:
        ALEO_PAGE_URL += f"&registryType={REGISTRY_TYPE}"

    driver.get(f"{ALEO_PAGE_URL}")

    print(f"Page {page} loaded")


from bs4 import BeautifulSoup

load_aleo_page(driver, "f")

page_source = driver.page_source
soup = BeautifulSoup(page_source, "html.parser")

company_count = get_company_count(soup)
print(f"Found {company_count} companies")

page_count = get_page_count(company_count, COUNT)
print(f"Found {page_count} pages")



# %%
def extract_companies(companies_on_page: list) -> list[dict]:
    results = []
    for company in companies_on_page:
        # nazwa i url
        a_tag = company.find("a", class_="catalog-row-first-line__company-name")
        if not a_tag:
            continue
        name = a_tag.get_text(strip=True)
        url = a_tag.get("href", "").strip()

        # adres
        address_tag = company.find("div", class_="catalog-row-company-info__address")
        address = ""
        if address_tag:
            span = address_tag.find("span")
            if span:
                address = span.get_text(strip=True)

        # NIP
        nip_tag = company.find("span", class_="tax-id")
        nip = nip_tag.get_text(strip=True) if nip_tag else ""

        # REGON
        regon_tag = company.find("span", class_="regon")
        regon = regon_tag.get_text(strip=True) if regon_tag else ""

        results.append({
            "name": name,
            "url": f"{base_url}/{url}",
            "address": address,
            "nip": nip,
            "regon": regon
        })

    return results


def _norm_site(url: str) -> str | None:
    import re

    from urllib.parse import urlparse

    EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

    if not url:
        return None
    url = url.strip()

    # jeżeli to email → nie jest stroną
    if EMAIL_RE.match(url):
        return None

    # jeśli brak schematu – dołóż https://
    if not urlparse(url).scheme:
        url = "https://" + url

    return url


def augment_companies_with_contacts(driver, companies_list: list[dict], base_url: str = "") -> list[dict]:
    from time import sleep
    from urllib.parse import urljoin
    from pprint import pprint

    original_window = driver.current_window_handle

    for company in companies_list:
        url = (company.get("url") or "").strip()
        if not url:
            continue
        if base_url:
            url = urljoin(base_url, url)

        driver.switch_to.new_window("tab")
        try:
            driver.get(url)
            sleep(1)  # ewentualnie zastąp WebDriverWait

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # EMAIL
            email_container = soup.select_one("div.e-mail")
            if email_container:
                # szukaj <a> lub <span> z tekstem, pomijając .tooltip-icon
                a = email_container.select_one("a[href^='mailto:']")
                email = None
                if a:
                    email = a.get("href", "").split("mailto:", 1)[-1].strip()
                else:
                    spans = [s for s in email_container.select("span") if "tooltip-icon" not in s.get("class", [])]
                    if spans:
                        email = spans[-1].get_text(strip=True)
                if email:
                    company["email"] = email

            # PHONE
            phone_container = soup.select_one("div.phone")
            if phone_container:
                a = phone_container.select_one("a[href^='tel:']")
                phone = None
                if a:
                    phone = a.get("href", "").split("tel:", 1)[-1].strip()
                else:
                    spans = [s for s in phone_container.select("span") if "tooltip-icon" not in s.get("class", [])]
                    if spans:
                        phone = spans[-1].get_text(strip=True)
                if phone:
                    company["phone"] = phone

            # WEBSITE (pomijamy tooltip-icon; bierzemy <a> lub drugi <span>)
            site_container = soup.select_one("div.site")
            if site_container:
                site = None
                spans = [s for s in site_container.select("span") if "tooltip-icon" not in s.get("class", [])]
                if spans:
                    site = spans[-1].get_text(strip=True)
                if _norm_site(site):
                    company["website"] = _norm_site(site)

        finally:
            pprint(company)
            driver.close()
            driver.switch_to.window(original_window)

    return companies_list


def store_companies(companies_list: list) -> None:
    from pprint import pprint
    import os, psycopg2

    from dotenv import load_dotenv
    load_dotenv(override=True)

    # parametrów połączenia nie trzeba wczytywać ponownie, .env jest już załadowane :contentReference[oaicite:1]{index=1}
    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = os.getenv("DB_PORT", 5432)
    DB_NAME     = os.getenv("DB_NAME", "booksy_scraper")
    DB_USER     = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    # ——— ZAPIS DO BAZY POSTGRES ———
    try:
        print("Zapisywanie do bazy PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=30,        # zwiększony timeout
            gssencmode='disable',      # wyłączenie GSSAPI
            sslmode='prefer',          # elastyczne podejście do SSL
            client_encoding='UTF8'
        )
        print(f"  PostgreSQL connection: {conn}")

        cur = conn.cursor()
        print(f"  PostgreSQL cursor: {cur}")

        ## dodawanie rekordów do tabeli
        number = 0
        for c in companies_list:
            pprint(c)
            cur.execute("""
                        INSERT INTO connections
                        (name, aleo_url, address, nip, regon, email, phone, website, search_phrase,
                         search_voivodships, search_city)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """, (
                            c["name"],
                            c["url"],
                            c["address"],
                            c["nip"],
                            c["regon"],
                            c["email"] if "email" in c else None,
                            c["phone"] if "phone" in c else None,
                            c["website"] if "website" in c else None,
                            PHRASE,
                            VOIVODSHIPS,
                            CITY
                        ))
            conn.commit()
            number += 1
            print(number)
        ## koniec dodawania rekordów

        # zamknięcie połączenia
        cur.close()

    except psycopg2.OperationalError as e:
        print(f"Błąd połączenia z bazą danych: {e}")
        raise
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            print("PostgreSQL connection closed.")

    # ——— KONIEC ZAPISU ———


from time import sleep

for page in range(1, page_count + 1):
    load_aleo_page(driver, "f", page=page)

    sleep(1)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")
    companies_on_page = soup.find_all("div", class_="catalog-row-container")
    companies_list = extract_companies(companies_on_page)

    print(f"Found {len(companies_list)} companies on page")

    print(f"Processed {len(augment_companies_with_contacts(driver, companies_list))} companies")

    store_companies(companies_list)



# %%
def db_create_tables() -> None:
    import os, psycopg2

    from dotenv import load_dotenv
    load_dotenv(override=True)

    # parametrów połączenia nie trzeba wczytywać ponownie, .env jest już załadowane :contentReference[oaicite:1]{index=1}
    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = os.getenv("DB_PORT", 5432)
    DB_NAME     = os.getenv("DB_NAME", "booksy_scraper")
    DB_USER     = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    # ——— ZAPIS DO BAZY POSTGRES ———
    try:
        print("Zapisywanie do bazy PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=30,        # zwiększony timeout
            gssencmode='disable',      # wyłączenie GSSAPI
            sslmode='prefer',          # elastyczne podejście do SSL
            client_encoding='UTF8'
        )
        print(f"  PostgreSQL connection: {conn}")

        cur = conn.cursor()
        print(f"  PostgreSQL cursor: {cur}")

        ###############################################
        ## tworzymy tabelę (jeśli nie istnieje)
        print("  Tworzenie tabel...")
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS connections (
                                                               id                   SERIAL PRIMARY KEY,
                                                               name                 TEXT,
                                                               aleo_url             TEXT UNIQUE,
                                                               address              TEXT,
                                                               nip                  TEXT UNIQUE,
                                                               regon                TEXT UNIQUE,
                                                               email                TEXT,
                                                               phone                TEXT,
                                                               website              TEXT,
                                                               search_phrase        TEXT,
                                                               search_voivodships   TEXT,
                                                               search_city          TEXT,
                                                               created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                               updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)
        conn.commit()
        print("  Tworzenie tabel zakończone.")

        ## dodajemy funkcję do aktualizacji updated_at
        print("  Dodawanie funkcji do aktualizacji updated_at...")
        cur.execute("""
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """)
        conn.commit()
        print("  Dodawanie funkcji zakończone.")

        ## dodajemy trigger do tabeli connections
        print("  Dodawanie triggera do tabeli connections...")
        cur.execute("""
                        DROP TRIGGER IF EXISTS set_updated_at ON connections;
                        CREATE TRIGGER set_updated_at
                        BEFORE UPDATE ON connections
                        FOR EACH ROW
                        EXECUTE FUNCTION update_updated_at_column();
            """)
        conn.commit()
        print("  Dodawanie triggera zakończone.")


        ## ——— Dodaj unikalny indeks na aleo_url ———
        print("  Dodawanie unikalnego indeksu...")
        cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS unique_connection_url
                        ON connections (aleo_url);
                    """)
        conn.commit()
        print("  Dodawanie unikalnego indeksu zakończone.")

        print("Tworzenie tabeli zakończone.")
        ## koniec tworzenia tabeli
        ###############################################




        # cur.execute(f"SELECT booksy_url FROM connections WHERE search_group='{SEARCH_GROUP}' "
        #             f"AND search_location='{SEARCH_LOCATION}';")
        # seen_connections = {row[0] for row in cur.fetchall() if row[0]}
        # print(f"Liczba unikalnych połączeń: {len(seen_connections)}")

        # zamknięcie połączenia
        cur.close()

    except psycopg2.OperationalError as e:
        print(f"Błąd połączenia z bazą danych: {e}")
        raise
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            print("PostgreSQL connection closed.")

    # ——— KONIEC ZAPISU ———



# %%
# db_create_tables()
driver.close()
