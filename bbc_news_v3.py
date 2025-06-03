# pip install feedparser spacy pycountry
# python -m spacy download en_core_web_sm

import feedparser
import json
from datetime import datetime
import spacy
import pycountry
from collections import defaultdict

# 1) 수집할 RSS 피드 목록 (원하는 만큼 추가)
SOURCES = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/travel/rss.xml",
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
    "https://feeds.bbci.co.uk/news/world/asia/rss.xml",
    # 예: "https://feeds.bbci.co.uk/news/world/africa/rss.xml",
]

# 사건·사고 키워드
incident_keywords = {
    "attack", "killed", "murder", "shooting", "explosion", "crash", "disaster",
    "terror", "riot", "violence", "accident", "earthquake", "fire", "storm",
    "protest", "assault", "hurricane", "drowning", "death", "injured",
    "outbreak", "virus", "infection", "pandemic", "epidemic", "disease",
    "flu", "covid", "ebola", "zika", "sars", "health", "vaccine", "contagion"
}

# spaCy NER 모델 로드
nlp = spacy.load("en_core_web_sm")

# pycountry 국가명 집합 (lowercase)
countries = {c.name.lower() for c in pycountry.countries}
for c in pycountry.countries:
    if hasattr(c, 'official_name'):
        countries.add(c.official_name.lower())

def is_incident(text: str) -> bool:
    return any(k in text.lower() for k in incident_keywords)

def extract_regions(text: str) -> list:
    doc = nlp(text)
    return list({ent.text for ent in doc.ents if ent.label_ == "GPE"})

def select_primary_region(regions: list) -> str:
    for r in regions:
        if r.lower() in countries:
            return r
    return regions[0] if regions else "Unknown"

def crawl_and_group(limit=100):
    seen_urls = set()
    grouped = defaultdict(list)
    processed = 0

    # 2) 각 피드별로 파싱
    for rss_url in SOURCES:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            if processed >= limit:
                break

            url = entry.link
            if url in seen_urls:
                continue
            seen_urls.add(url)

            combined = f"{entry.title} {entry.summary}"
            if not is_incident(combined):
                continue

            # 날짜 ISO 변환
            published = None
            if hasattr(entry, 'published_parsed'):
                published = datetime(*entry.published_parsed[:6]).isoformat()

            regions = extract_regions(combined)
            primary = select_primary_region(regions)

            item = {
                "title": entry.title,
                "url": url,
                "summary": entry.summary,
                "date": published,
                "source": "BBC",
                "primary_region": primary,
                "all_regions": regions or ["Unknown"]
            }

            grouped[primary].append(item)
            processed += 1

        if processed >= limit:
            break

    # 3) 결과 저장
    with open("bbc_incident_by_region_v8.json", "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    print(f"Processed {processed} unique incident articles from {len(SOURCES)} feeds.")

if __name__ == "__main__":
    crawl_and_group(limit=100)
