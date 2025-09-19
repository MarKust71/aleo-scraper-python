# API do scrapowania ALEO po NIP

## Instalacja
```
pip install -r requirements.txt
pip install -r requirements_api.txt
```

## Uruchomienie
```
export API_KEY="super-tajny-klucz"
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

## Zapytanie
```
curl -G "http://localhost:8000/"   -H "X-API-Key: $API_KEY"   --data-urlencode "nip=5210123456"
```

## Uwaga
- Moduł korzysta z istniejących funkcji w `main.py`:
  - `init()` — tworzy przeglądarkę (Selenium)
  - `set_globals(...)` — konfiguruje `BASE_URL` i inne parametry
  - `load_aleo_page(...)` — otwiera stronę z wynikami
  - `extract_companies(...)` — buduje listę firm z wyników
  - `augment_companies_with_contacts(...)` — dociąga telefon/email/stronę z widoku firmy

- Dla stabilności tworzony jest **nowy driver na każde żądanie** i zamykany po obsłudze.
- Jeśli chcesz uruchamiać Selenium w trybie headless, rozważ modyfikację `init()` w `main.py`.
