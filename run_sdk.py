# file: run_sdk.py
from common import Config, run_sync
from adapters.sdk_adapter import SdkAdapter

def main():
    cfg = Config()
    run_sync(SdkAdapter(cfg), cfg)

if __name__ == "__main__":
    main()
