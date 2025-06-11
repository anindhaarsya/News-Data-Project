import requests
from bs4 import BeautifulSoup
import time
import json

BASE_URL = "https://en.wikipedia.org"
CATEGORY_URL = f"{BASE_URL}/wiki/Category:News_websites_by_country"

# Ambil link halaman kategori situs berita di setiap negara
def get_country_links():
    response = requests.get(CATEGORY_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Gunakan .mw-category a karena #mw-pages tidak berisi data
    country_links = soup.select('.mw-category a')
    
    country_url_map = {}
    for link in country_links:
        country = link.text.strip()
        href = link.get('href')
        if href:
            full_url = BASE_URL + href
            country_url_map[country] = full_url
    return country_url_map

# Ambil daftar situs berita dari halaman negara
def get_news_sites_for_country(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Di halaman negara, daftar situs biasanya di .mw-category-group a
    site_links = soup.select('.mw-category-group a')
    
    site_names = []
    for link in site_links:
        name = link.text.strip()
        if name and not name.startswith("Category:"):
            site_names.append(name)
    return site_names

# Main
def main():
    all_data = {}

    print("📥 Mengambil daftar negara...")
    country_links = get_country_links()

    for i, (country, url) in enumerate(country_links.items()):
        print(f"[{i+1}/{len(country_links)}] 🔍 {country}")
        try:
            news_sites = get_news_sites_for_country(url)
            if news_sites:
                all_data[country] = news_sites
        except Exception as e:
            print(f"❌ Gagal mengambil data dari {country}: {e}")
        time.sleep(1)  # delay agar tidak diblokir

    # Simpan hasil scraping ke file JSON
    with open("news_websites_by_country.json", "w", encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print("\n✅ Selesai! File disimpan di 'news_websites_by_country.json'")

if __name__ == "__main__":
    main()
