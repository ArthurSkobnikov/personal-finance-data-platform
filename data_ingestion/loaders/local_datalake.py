import os
import json
from datetime import datetime

def write_json_to_datalake(data:dict, namespace, stream, datalake_root):
	"""
	Write the given data to a JSON file in the local datalake datalake/<namespace>/<stream> folder.
	The filename will include a timestamp for uniqueness.
	"""
	folder = os.path.join(datalake_root, namespace, stream)
	
	try:
		os.makedirs(folder, exist_ok=True)
		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		filename = f"{timestamp}.json"
		filepath = os.path.join(folder, filename)
		with open(filepath, "w", encoding="utf-8") as f:
			json.dump(data, f, ensure_ascii=False, indent=2)
		return filepath
	except Exception as e:
		print(f"[ERROR] Failed to write file: {e}")
		return None
