import json
import os
import hashlib
import sys
from collections import defaultdict
from datetime import datetime, timedelta
import tldextract
import pycountry
from eventregistry import EventRegistry, Analytics
import traceback

# === Cek apakah flag --force dipakai ===
force = "--force" in sys.argv

# === CONFIGURATION ===
INPUT_DIR = "D:/proyek_folder/input_jsons"
OUTPUT_DIR = "D:/proyek_folder/output_jsons/weekly_b"
LOG_FILE = "D:/proyek_folder/weekly_hashed.log"
SCRAPED_MAP_FILE = "D:/proyek_folder/news_websites_by_country_mapped_UPDATED.json"

# === INIT ===
er = EventRegistry(apiKey="05e8c442-4458-4df3-9446-45251a0ad374")
analytics = Analytics(er)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === LOAD MAPPING ===
SCRAPED_SOURCE_MAP = {}
if os.path.exists(SCRAPED_MAP_FILE):
    with open(SCRAPED_MAP_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
        for country, sources in raw.items():
            for site in sources:
                SCRAPED_SOURCE_MAP[site.strip().lower()] = country

# === UTILS ===
def get_md5(filepath):
    with open(filepath, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def safe_get(d, keys):
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return None
    return d

def get_country_from_domain(domain):
    tld = domain.split('.')[-1].lower()
    tld_map = {
        'uk': 'United Kingdom', 'us': 'United States', 'id': 'Indonesia',
        'sg': 'Singapore', 'au': 'Australia', 'ca': 'Canada', 'jp': 'Japan',
        'de': 'Germany', 'fr': 'France', 'in': 'India', 'cn': 'China', 'ru': 'Russia'
    }
    return tld_map.get(tld, "Unknown")

def get_country_name(iso_code):
    try:
        return pycountry.countries.get(alpha_2=iso_code).name
    except:
        return "Unknown"

def get_country_code(country):
    try:
        return pycountry.countries.lookup(country).alpha_2
    except:
        return "UN"

def get_week_number(pub_date):
    first_day = pub_date.replace(day=1)
    dom = pub_date.day
    adjusted_dom = dom + first_day.weekday()
    week = (adjusted_dom - 1) // 7 + 1
    return week

def generate_week_key(pub_date):
    return f"{pub_date.strftime('%Y-%m')}-W{get_week_number(pub_date):02d}"

PRONOUNS = {
    "i", "you", "he", "she", "it", "we", "they",
    "me", "him", "her", "us", "them",
    "my", "your", "his", "her", "its", "our", "their",
    "mine", "yours", "hers", "ours", "theirs",
    "myself", "yourself", "himself", "herself", "itself",
    "ourselves", "yourselves", "themselves"
}
def extract_entities(text):
    try:
        # Hanya proses teks yang cukup panjang
        if not text or len(text) < 50:
            return []
        
        limited_text = text[:25000]  # Ambil hanya 25000 karakter pertama
        if len(text) > 25000:
            print(f"⚠️ Trimming text to 25000 characters from {len(text)}")
            
        result = analytics.ner(limited_text)
        entities = result.get('entities', []) if isinstance(result, dict) else []
        return [
            (e.get('label') or e.get('text', '')).strip()
            for e in entities
            if isinstance(e, dict) and e.get('type', '').upper() in {'PERSON', 'ORGANIZATION', 'PLACE'}
            and (e.get('label') or e.get('text', '')).strip().lower() not in PRONOUNS 
        ]
        
    except Exception as e:
        print("⚠️ NER Error:", e)
        return []

def process_weekly():
    print("🚀 Starting weekly process")

    # Dapatkan daftar file yang akan diproses
    if len(sys.argv) > 1 and sys.argv[1].endswith('.json'):
        # Jika ada argumen berupa nama file, proses hanya file tersebut
        files_to_process = [sys.argv[1]]
        print(f"🔍 Processing specific file: {sys.argv[1]}")
    else:
        # Jika tidak ada argumen, proses semua file
        files_to_process = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith(".json")])
        print(f"🔍 Processing all files in directory ({len(files_to_process)} files)")

    existing_hashes = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            existing_hashes = {line.strip().split()[0] for line in f}

    new_hashes = []
    weekly_data = defaultdict(lambda: {
        "startDate": None,
        "endDate": None,
        "totalNews": 0,
        "totalPublishers": set(),
        "totalEvents": 0,
        "distributionChart": [],
        "geoMapChart": defaultdict(int),
        "weeklyEntityData": defaultdict(lambda: defaultdict(int))
    })
    seen_event_uris_per_week = defaultdict(set)

    for filename in files_to_process:
        filepath = os.path.join(INPUT_DIR, filename)
        file_hash = get_md5(filepath)
        if not force and file_hash in existing_hashes:
            print(f"⏩ Skipping {filename} (already processed)")
            continue

        print(f"📄 Processing {filename}")
        new_hashes.append(f"{file_hash} {filename}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        event_count = 0
        total_events = len(data.get("events", {}).get("results", []))
        
        for event in data.get("events", {}).get("results", []):
            event_count += 1
            if event_count % 10 == 0:
                print(f"  ⏳ Processing event {event_count}/{total_events} in {filename}")
                
            event_uri = event.get("uri")
            stories = event.get("stories", [])
            if not stories or not event_uri:
                continue

            for story in stories:
                medoid = story.get("medoidArticle")
                if not medoid or medoid.get("lang", "").lower() != "eng":
                    continue

                try:
                    pub_date_str = medoid.get("dateTimePub")
                    pub_date = datetime.strptime(pub_date_str, "%Y-%m-%dT%H:%M:%SZ")
                    week_key = generate_week_key(pub_date)
                    week = weekly_data[week_key]

                    if not week["startDate"]:
                        start = pub_date - timedelta(days=pub_date.weekday())
                        week["startDate"] = start.strftime("%Y-%m-%d")
                        week["endDate"] = (start + timedelta(days=6)).strftime("%Y-%m-%d")

                    article_count = story.get("articleCount", 1)
                    week["totalNews"] += article_count

                    if event_uri not in seen_event_uris_per_week[week_key]:
                        week["totalEvents"] += 1
                        seen_event_uris_per_week[week_key].add(event_uri)

                    body = medoid.get("body", "")
                    text_for_ner = body
                    source_title = (safe_get(medoid, ["source", "title"]) or "").strip()
                    uri = safe_get(medoid, ["source", "uri"]) or ""

                    country = (
                        safe_get(medoid, ["source", "location", "country", "label", "eng"]) or
                        SCRAPED_SOURCE_MAP.get(source_title.lower()) or
                        (get_country_from_domain(tldextract.extract(uri).suffix) if uri else "Unknown"
                    ))

                    iso_code = get_country_code(country)
                    week["geoMapChart"][iso_code] += article_count
                    week["totalPublishers"].add(source_title)

                    week["distributionChart"].append({
                        "title": medoid.get("title", ""),
                        "articleCount": article_count,
                        "dateTimePub": pub_date_str,
                        "url": medoid.get("url", ""),
                        "source": source_title
                    })

                    # Hanya proses NER untuk artikel penting
                    if article_count >= 5:
                        for entity in set(extract_entities(text_for_ner)):
                            week["weeklyEntityData"][pub_date.weekday() + 1][entity] += 1

                except Exception as e:
                    print(f"❌ Error processing article: {e}")
                    # traceback.print_exc()  # Komentari jika tidak perlu detail

    for week_key, content in weekly_data.items():
        if content["totalNews"] == 0:
            continue

        top_articles = sorted(content["distributionChart"], key=lambda x: x["articleCount"], reverse=True)[:10]
        total_top = sum(a["articleCount"] for a in top_articles)
        for a in top_articles:
            a["percentage"] = f"{(a['articleCount'] / total_top * 100):.2f}%" if total_top else "0%"

        geo_map = [
            {"country": get_country_name(code), "countryCode": code.lower(), "articleCount": count}
            for code, count in sorted(content["geoMapChart"].items(), key=lambda x: x[1], reverse=True)
        ]

        ner_data = [
            {
                "day": day,
                "data": [
                    {"title": k, "count": v}
                    for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True)[:10]
                ]
            }
            for day, d in sorted(content["weeklyEntityData"].items())
        ]

        output_file = os.path.join(OUTPUT_DIR, f"{week_key}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({
                "startDate": content["startDate"],
                "endDate": content["endDate"],
                "totalNews": content["totalNews"],
                "totalPublishers": len(content["totalPublishers"]),
                "totalEvents": content["totalEvents"],
                "distributionChart": top_articles,
                "geoMapChart": geo_map,
                "weeklyEntityData": ner_data
            }, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved: {output_file}")

    if new_hashes:
        with open(LOG_FILE, "a") as f:
            f.write("\n".join(new_hashes) + "\n")

    print("✅ DONE. Processed files:", ", ".join([f.split()[1] for f in new_hashes]) if new_hashes else "No new files")

if __name__ == "__main__":
    process_weekly()