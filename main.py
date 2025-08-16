# %%
# INIT
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

from selenium import webdriver
driver = webdriver.Chrome()


# %%
base_url = "https://aleo.com/pl"

COUNT = 100
PHRASE = "b"
# VOIVODSHIPS = ""
VOIVODSHIPS = "SLASKIE"
CITY = "Katowice"
# CITY = "Zgorzelec"
REGISTRY_TYPE = "CEIDG"

ALEO_PAGE_URL=f"{base_url}/firmy?phrase={PHRASE}&count={COUNT}"
if VOIVODSHIPS:
    ALEO_PAGE_URL += f"&voivodeships={VOIVODSHIPS}"
if CITY:
    ALEO_PAGE_URL += f"&city={CITY}"
if REGISTRY_TYPE:
    ALEO_PAGE_URL += f"&registryType={REGISTRY_TYPE}"

# ALEO_PAGE_URL=f"{base_url}/firmy?phrase={PHRASE}&count={COUNT}&voivodeships={VOIVODSHIPS}&city={CITY}&registryType={REGISTRY_TYPE}"

driver.get(f"{ALEO_PAGE_URL}")


# %%
# pobranie źródła strony i sparsowanie BeautifulSoup
from bs4 import BeautifulSoup
from pprint import pprint

page_source = driver.page_source
soup = BeautifulSoup(page_source, "html.parser")


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


companies_on_page = soup.find_all("div", class_="catalog-row-container")
companies_list = extract_companies(companies_on_page)

print(f"Found {len(companies_list)} companies")


# %%
from urllib.parse import urljoin, urlparse
import time
import re

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")


def _norm_site(url: str) -> str | None:
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
            time.sleep(1)  # ewentualnie zastąp WebDriverWait

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


print(f"Processed {len(augment_companies_with_contacts(driver, companies_list))} companies")
