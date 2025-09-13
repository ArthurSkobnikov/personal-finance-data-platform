import json
import os


from extractors.ibkr.IBAccountSummary import main as get_ibkr_account_summary
from loaders.local_datalake import write_json_to_datalake

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOCAL_DATALAKE_PATH = os.getenv("LOCAL_DATALAKE_PATH", os.path.join(PROJECT_ROOT, "datalake"))

def main():
    # Extract data from IBKR and write to local datalake
    ibkr_account_summary = get_ibkr_account_summary()
    write_json_to_datalake(ibkr_account_summary, namespace="ibkr", stream="IBAccountSummary", datalake_root=LOCAL_DATALAKE_PATH)


if __name__ == "__main__":
    main()
