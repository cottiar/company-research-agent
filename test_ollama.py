import requests
import json

# Force IPv4
url = "http://127.0.0.1:11434/v1/chat/completions"

payload = {
    "model": "llama3",  # Change this if you use tinyllama or mistral
    "messages": [{"role": "user", "content": "Are you working?"}]
}

print(f"Testing connection to: {url}...")

try:
    response = requests.post(url, json=payload, timeout=300)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("SUCCESS! Response:")
        print(response.json()['choices'][0]['message']['content'])
    else:
        print("FAILED. Error message:")
        print(response.text)
except Exception as e:
    print(f"CRASH: {e}")