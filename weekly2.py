import os
import json
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
import math
import pycountry
import tldextract
import glob
from eventregistry import EventRegistry, Analytics

# Initialize EventRegistry
er = EventRegistry(apiKey="05e8c442-4458-4df3-9446-45251a0ad374")
analytics = Analytics(er)

# === Load mapping JSON hasil scraping
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

def determine_country(source_title, source_country, source_url):
    if source_country and source_country.strip() and source_country != "Unknown":
        return source_country
    
    if source_title:
        source_country = SCRAPED_SOURCE_TO_COUNTRY.get(source_title)
        if source_country:
            return source_country
        
        source_country = find_country_from_scraped_title(source_title)
        if source_country != "Unknown":
            return source_country
    
    if source_url:
        domain = tldextract.extract(source_url).suffix
        cc_map = {
            "uk": "United Kingdom", "us": "United States", "ca": "Canada",
            "au": "Australia", "in": "India", "cn": "China", "jp": "Japan",
            "de": "Germany", "fr": "France", "ru": "Russia", "za": "South Africa",
            "br": "Brazil", "kr": "South Korea", "ng": "Nigeria", "ae": "United Arab Emirates",
            "pk": "Pakistan", "bd": "Bangladesh"
        }
        return cc_map.get(domain.lower(), "Unknown")
    
    return "Unknown"

def extract_entities(text):
    try:
        ner_result = analytics.ner(text)
        
        entities = []
        if isinstance(ner_result, dict):
            entities = ner_result.get('entities', [])
        elif isinstance(ner_result, list):
            entities = ner_result[0].get('entities', []) if ner_result else []
        
        filtered_entities = []
        for entity in entities:
            if isinstance(entity, dict):
                label = entity.get('label') or entity.get('text', '')
                etype = entity.get('type', '').upper()
                if label and etype not in {'DATE', 'NUMBER', 'TIME', 'PERCENT'}:
                    filtered_entities.append(label)  # Hanya menyimpan label, tanpa type
        return filtered_entities
    except Exception as e:
        print(f"NER Error: {str(e)}")
        return []

# === Paths and directories
input_dir = "D:/proyek_folder/input_jsons"
output_dir = "D:/proyek_folder/output_jsons/weekly"
log_file_path = "D:/proyek_folder/processed_weekly_hash.log"
unmatched_log_path = "D:/proyek_folder/unmatched_sources_weekly.txt"

os.makedirs(output_dir, exist_ok=True)

# === Hash log for processed files
existing_hashes = set()
if os.path.exists(log_file_path):
    with open(log_file_path, "r") as f:
        existing_hashes = {line.strip().split()[0] for line in f}

new_hash_lines = []
unmatched_sources = set()

# === Process files
for filename in sorted(os.listdir(input_dir)):
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(input_dir, filename)
    file_hash = get_md5(filepath)

    if file_hash in existing_hashes:
        print(f"⚠️  Skipped (already processed): {filename}")
        continue

    print(f"🔄 Processing: {filename}")
    new_hash_lines.append(f"{file_hash} {filename}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error loading {filename}: {str(e)}")
        continue

    weekly_reports = defaultdict(lambda: {
        "startDate": None,
        "endDate": None,
        "totalNews": 0,
        "totalPublishers": set(),
        "totalEvents": 0,
        "articles": [],
        "geoData": defaultdict(int),
        "dailyEntities": defaultdict(lambda: defaultdict(int))  # Menyimpan entitas tanpa type
    })

    for event in data.get('events', {}).get('results', []):
        for story in event.get('stories', []):
            medoid = story.get('medoidArticle')
            if not medoid:
                continue
            
            pub_date_str = medoid.get('dateTimePub')
            if not pub_date_str:
                continue
            
            try:
                pub_date = datetime.strptime(pub_date_str, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                continue
            
            week_start = pub_date - timedelta(days=pub_date.weekday())
            week_end = week_start + timedelta(days=6)
            week_key = f"{week_start.year}-{week_start.month:02d}-Week{math.ceil(week_start.day/7)}"
            
            source_title = safe_get(medoid, ['source', 'title'])
            source_country = safe_get(medoid, ['source', 'location', 'country', 'label', 'eng'])
            source_url = safe_get(medoid, ['source', 'uri'])
            
            final_country = determine_country(source_title, source_country, source_url)
            if final_country == "Unknown":
                unmatched_sources.add(source_title or source_url or "Unknown Source")
            
            final_country = manual_country_name_map.get(final_country, final_country)
            iso_code = get_iso_country_code(final_country)
            
            report = weekly_reports[week_key]
            if not report["startDate"] or pub_date.date() < datetime.strptime(report["startDate"], '%Y-%m-%d').date():
                report["startDate"] = week_start.strftime('%Y-%m-%d')
            if not report["endDate"] or pub_date.date() > datetime.strptime(report["endDate"], '%Y-%m-%d').date():
                report["endDate"] = week_end.strftime('%Y-%m-%d')
            
            report["totalNews"] += 1
            if source_title:
                report["totalPublishers"].add(source_title)
            
            article_title = medoid.get('title', 'No title')
            report["articles"].append({
                "title": article_title,
                "dateTimePub": pub_date_str,
                "url": medoid.get('url', ''),
                "articleCount": story.get('articleCount', 1)
            })
            
            report["geoData"][iso_code] += story.get('articleCount', 1)
            
            if article_title:
                entities = extract_entities(article_title)
                day_of_week = pub_date.weekday() + 1
                for entity in entities:
                    report["dailyEntities"][day_of_week][entity] += 1  # Hanya menyimpan nama entitas

    for week_key, report_data in weekly_reports.items():
        if report_data["totalNews"] == 0:
            continue
        
        article_counts = defaultdict(int)
        for article in report_data["articles"]:
            article_counts[(article["title"], article["url"])] += article["articleCount"]
        
        top_articles = sorted(article_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        total_articles = sum(count for _, count in top_articles)
        
        distribution_chart = []
        for (title, url), count in top_articles:
            distribution_chart.append({
                "title": title,
                "articleCount": count,
                "percentage": f"{(count / total_articles * 100):.2f}%",
                "dateTimePub": next(a["dateTimePub"] for a in report_data["articles"] if a["title"] == title),
                "url": url
            })
        
        geo_map_chart = []
        for iso_code, count in sorted(report_data["geoData"].items(), 
                                    key=lambda x: x[1], reverse=True):
            geo_map_chart.append({
                "country": get_country_fullname(iso_code),
                "countryCode": iso_code.lower(),
                "articleCount": count
            })
        
        weekly_entity_data = []
        for day in sorted(report_data["dailyEntities"].keys()):
            day_entities = sorted(report_data["dailyEntities"][day].items(), 
                                key=lambda x: x[1], reverse=True)[:10]
            weekly_entity_data.append({
                "day": day,
                "data": [{"title": entity, "count": cnt} for entity, cnt in day_entities]  # Hanya nama entitas
            })
        
        final_report = {
            "startDate": report_data["startDate"],
            "endDate": report_data["endDate"],
            "totalNews": report_data["totalNews"],
            "totalPublishers": len(report_data["totalPublishers"]),
            "totalEvents": len(data.get('events', {}).get('results', [])),
            "distributionChart": distribution_chart,
            "geoMapChart": geo_map_chart,
            "weeklyEntityData": weekly_entity_data
        }
        
        output_filename = f"{week_key}_report.json"
        output_path = os.path.join(output_dir, output_filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Generated weekly report: {output_path}")

# Update hash log
if new_hash_lines:
    with open(log_file_path, "a") as f:
        f.write("\n".join(new_hash_lines) + "\n")
    print(f"📝 Updated hash log with {len(new_hash_lines)} new entries")

# Save unmatched sources
if unmatched_sources:
    with open(unmatched_log_path, "w", encoding="utf-8") as f:
        for source in sorted(unmatched_sources):
            f.write(source + "\n")
    print(f"🧾 Saved {len(unmatched_sources)} unmatched sources to {unmatched_log_path}")
else:
    print("✅ All sources were matched successfully!")

print("🎉 Weekly processing complete!")