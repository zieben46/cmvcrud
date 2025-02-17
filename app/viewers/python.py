import requests

BASE_URL = "http://127.0.0.1:8000"
TOKEN = "YOUR_ACCESS_TOKEN"

headers = {"Authorization": f"Bearer {TOKEN}"}

# 🔒 Lock Table
resp = requests.post(f"{BASE_URL}/person/lock", headers=headers)
print(resp.json())

# 🚀 Try to Update While Locked
update_payload = {"name": "Alice Updated"}
resp = requests.put(f"{BASE_URL}/person/update/1", json=update_payload, headers=headers)
print(resp.json())  # Should work because we locked it

# 🔓 Unlock Table
resp = requests.post(f"{BASE_URL}/person/unlock", headers=headers)
print(resp.json())
