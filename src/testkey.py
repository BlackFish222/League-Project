# test_simple.py in src folder
from Config import api_key
import requests

print(f"API Key from Config: {api_key}...")

# Simple test
url = "https://na1.api.riotgames.com/lol/status/v4/platform-data"
headers = {"X-Riot-Token": api_key}

try:
    response = requests.get(url, headers=headers, timeout=10)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print("Success! API key works.")
    else:
        print(f"Error {response.status_code}: {response.text[:100]}")
except Exception as e:
    print(f"Exception: {e}")