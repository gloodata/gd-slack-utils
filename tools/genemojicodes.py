import requests
import json

emojis = {}

# url = "https://raw.githubusercontent.com/muan/unicode-emoji-json/refs/heads/main/data-by-emoji.json"
# response = requests.get(url)
# response.raise_for_status()
# data = response.json()
# emojis = {value["slug"]: key for key, value in data.items()}


def emoji_unified_to_unicode_string(s):
    return "".join(chr(int(code, 16)) for code in s.split("-"))


response = requests.get(
    "https://raw.githubusercontent.com/iamcal/emoji-data/refs/heads/master/emoji_pretty.json"
)
response.raise_for_status()  # Raise an error if the request failed
data = response.json()
for item in data:
    for key in item.get("short_names", []):
        emojis[key] = emoji_unified_to_unicode_string(item["unified"])

# Save the new dictionary to a file
with open("emoji_shortcodes.json", "w") as f:
    json.dump(emojis, f, indent=2)
