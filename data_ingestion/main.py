
import json
import os


from extractors.ibkr.IBAccountSummary import main as get_ibkr_account_summary
from loaders.local_datalake import write_json_to_datalake

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOCAL_DATALAKE_PATH = os.path.join(PROJECT_ROOT, "datalake")

data = get_ibkr_account_summary()

write_json_to_datalake(data, namespace="ibkr", stream="IBAccountSummary", datalake_root=LOCAL_DATALAKE_PATH)
print("Data written to local datalake.")
