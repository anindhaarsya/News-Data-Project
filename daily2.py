import os
import json
import sys
import hashlib
from collections import defaultdict
from datetime import datetime
import pycountry
import tldextract

# Cek apakah ada argumen nama file
specific_file = None
for arg in sys.argv[1:]:
    if arg.endswith(".json"):
        specific_file = arg
        break

force = "--force" in sys.argv

# === Load mapping hasil scraping
with open("D:/proyek_folder/news_websites_by_country_mapped_UPDATED.json", "r", encoding="utf-8") as f:
    scraped_map = json.load(f)

SCRAPED_SOURCE_TO_COUNTRY = {}
for country, sources in scraped_map.items():
    for source in sources:
        SCRAPED_SOURCE_TO_COUNTRY[source.strip()] = country

manual_country_name_map = {
    "Russia": "Russian Federation",
    "South Korea": "Korea, Republic of",
    "North Korea": "Korea, Democratic People's Republic of",
    "Iran": "Iran, Islamic Republic of",
    "Syria": "Syrian Arab Republic",
    "Turkey": "Türkiye"
}

def safe_get(d, keys):
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return None
    return d

def get_md5(filepath):
    with open(filepath, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def get_iso_country_code(country_name):
    country_name = manual_country_name_map.get(country_name, country_name)
    try:
        return pycountry.countries.lookup(country_name).alpha_2
    except LookupError:
        return "UN"

def get_country_fullname(iso_code):
    try:
        return pycountry.countries.get(alpha_2=iso_code).name
    except:
        return "Unknown"

def find_country_from_scraped_title(source_title):
    for name, country in SCRAPED_SOURCE_TO_COUNTRY.items():
        if name.lower() in source_title.lower():
            return country
    return "Unknown"

# === Path
input_dir = "D:/proyek_folder/input_jsons"
output_dir = "D:/proyek_folder/output_jsons/daily"
log_file_path = "D:/proyek_folder/processed_files_hash.log"

os.makedirs(output_dir, exist_ok=True)

existing_hashes = set()
if os.path.exists(log_file_path):
    with open(log_file_path, "r") as f:
        existing_hashes = {line.strip().split()[0] for line in f}

new_hash_lines = []

daily_data = defaultdict(lambda: {
    "totalNews": 0,
    "totalPublishers": set(),
    "totalEvents": 0,
    "distributionChart": [],
    "geoMapChart": defaultdict(int)
})

# === Loop file
all_files = [specific_file] if specific_file else sorted(os.listdir(input_dir))
for filename in all_files:
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(input_dir, filename) if not os.path.isabs(filename) else filename
    if not os.path.exists(filepath):
        print(f"❌ File tidak ditemukan: {filepath}")
        continue

    file_hash = get_md5(filepath)
    if file_hash in existing_hashes and not force:
        print(f"⚠️  Lewati (sudah diproses): {filename}")
        continue

    print(f"🔄 Proses: {filename}")
    new_hash_lines.append(f"{file_hash} {filename}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    events = data.get("events", {}).get("results", []) if isinstance(data, dict) else data
    for event in events:
        top_story = event.get("stories", [])[0] if event.get("stories") else None
        if not top_story:
            continue

        medoid = top_story.get("medoidArticle")
        if not medoid:
            continue

        date_time_pub = safe_get(medoid, ["dateTimePub"])
        if not date_time_pub:
            continue

        try:
            pub_date = datetime.strptime(date_time_pub, "%Y-%m-%dT%H:%M:%SZ")
            event_date = pub_date.strftime("%Y-%m-%d")
        except ValueError:
            continue

        daily = daily_data[event_date]
        daily["totalEvents"] += 1
        daily["totalNews"] += top_story.get("articleCount", 0)

        source_title = (safe_get(medoid, ["source", "title"]) or "").strip()
        source_country = safe_get(medoid, ["source", "location", "country", "label", "eng"])

        if not source_country or not source_country.strip():
            source_country = SCRAPED_SOURCE_TO_COUNTRY.get(source_title)
            if not source_country:
                source_country = find_country_from_scraped_title(source_title)

        if not source_country or source_country == "Unknown":
            source_url = safe_get(medoid, ["source", "uri"]) or ""
            if source_url:
                domain = tldextract.extract(source_url).suffix
                cc_map = {
                    "uk": "United Kingdom", "us": "United States", "ca": "Canada",
                    "au": "Australia", "in": "India", "cn": "China", "jp": "Japan",
                    "de": "Germany", "fr": "France", "ru": "Russia", "za": "South Africa",
                    "br": "Brazil", "kr": "South Korea", "ng": "Nigeria", "ae": "United Arab Emirates",
                    "pk": "Pakistan", "bd": "Bangladesh"
                }
                source_country = cc_map.get(domain.lower(), "Unknown")

        source_country = manual_country_name_map.get(source_country, source_country)
        iso_code = get_iso_country_code(source_country)

        daily["geoMapChart"][iso_code] += top_story.get("articleCount", 0)
        daily["totalPublishers"].add(source_title)
        daily["distributionChart"].append({
            "title": safe_get(medoid, ["title"]),
            "articleCount": top_story.get("articleCount"),
            "percentage": None,
            "dateTimePub": date_time_pub,
            "url": safe_get(medoid, ["url"])
        })

# === Save JSON harian
for date_str, content in sorted(daily_data.items()):
    total_articles = content["totalNews"]
    chart_sorted = sorted(content["distributionChart"], key=lambda x: x["articleCount"] or 0, reverse=True)[:10]

    for item in chart_sorted:
        if item["articleCount"] and total_articles:
            item["percentage"] = f"{(item['articleCount'] / total_articles) * 100:.2f}%"

    geo_list = [{"country": get_country_fullname(k), "code": k, "articleCount": v} for k, v in content["geoMapChart"].items()]
    formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    final_output = {
        "date": formatted_date,
        "totalNews": content["totalNews"],
        "totalPublishers": len(content["totalPublishers"]),
        "totalEvents": content["totalEvents"],
        "distributionChart": chart_sorted,
        "geoMapChart": geo_list
    }

    output_path = os.path.join(output_dir, f"{formatted_date}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

    print(f"✅ Disimpan: {output_path}")

# Simpan log hash
if new_hash_lines:
    with open(log_file_path, "a") as f:
        f.write("\n".join(new_hash_lines) + "\n")
    print("📝 Hash baru ditambahkan ke log.")

print("✅ Selesai semua 🎉")
