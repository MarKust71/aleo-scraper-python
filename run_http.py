# file: run_http.py
from common import Config, run_sync
from adapters.http_adapter import HttpAdapter

def main():
    cfg = Config()
    run_sync(HttpAdapter(cfg), cfg)

if __name__ == "__main__":
    main()
