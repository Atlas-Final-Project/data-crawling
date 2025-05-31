# pip install feedparser spacy
# python -m spacy download en_core_web_sm

import feedparser
import json
from datetime import datetime
import spacy
from collections import defaultdict, OrderedDict

# BBC World RSS 피드
RSS_URL = "https://feeds.bbci.co.uk/news/world/rss.xml"

# 사건·사고 키워드
incident_keywords = {
    "attack", "killed", "murder", "shooting", "explosion", "crash", "disaster",
    "terror", "riot", "violence", "accident", "earthquake", "fire", "storm",
    "protest", "assault", "hurricane", "drowning", "death", "injured"
}

# spaCy NER 모델 로드
nlp = spacy.load("en_core_web_sm")

def is_incident(text: str) -> bool:
    return any(k in text.lower() for k in incident_keywords)

def extract_regions(text: str) -> list:
    doc = nlp(text)
    return list({ent.text for ent in doc.ents if ent.label_ == "GPE"})

def crawl_and_group(limit=50):
    feed = feedparser.parse(RSS_URL)
    unique_entries = list(OrderedDict((entry.link, entry) for entry in feed.entries).values())

    grouped = defaultdict(list)
    processed = 0

    for entry in unique_entries:
        if processed >= limit:
            break

        combined = f"{entry.title} {entry.summary}"
        if not is_incident(combined):
            continue

        published = None
        if hasattr(entry, 'published_parsed'):
            published = datetime(*entry.published_parsed[:6]).isoformat()

        regions = extract_regions(combined) or ["Unknown"]

        item = {
            "title": entry.title,
            "url": entry.link,
            "summary": entry.summary,
            "date": published,
            "source": "BBC",
            "regions": regions
        }

        for region in regions:
            if not any(existing['url'] == item['url'] for existing in grouped[region]):
                grouped[region].append(item)

        processed += 1

    with open("bbc_incident_by_region.json", "w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)

    print(f"Processed {processed} unique incident articles, grouped by region.")

if __name__ == "__main__":
    crawl_and_group(limit=50)

