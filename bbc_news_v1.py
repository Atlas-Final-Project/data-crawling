import feedparser
import json
from datetime import datetime

# BBC World RSS 피드
RSS_URL = "https://feeds.bbci.co.uk/news/world/rss.xml"

# 사건·사고 키워드
incident_keywords = [
    "attack", "killed", "murder", "shooting", "explosion", "crash", "disaster",
    "terror", "riot", "violence", "accident", "earthquake", "fire", "storm",
    "protest", "assault", "hurricane", "drowning", "death", "injured"
]

def is_incident(entry):
    text = (entry.title + " " + entry.summary).lower()
    return any(k in text for k in incident_keywords)

def crawl_bbc_rss():
    feed = feedparser.parse(RSS_URL)
    results = []

    for entry in feed.entries:
        if is_incident(entry):
            # 발행일을 ISO 포맷으로 변환
            published = None
            if hasattr(entry, 'published_parsed'):
                published = datetime(*entry.published_parsed[:6]).isoformat()

            results.append({
                "title": entry.title,
                "url": entry.link,
                "summary": entry.summary,
                "date": published,
                "source": "BBC"
            })

    # JSON 파일로 저장
    with open("bbc_incident_rss_v1.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Found {len(results)} incident articles. Saved to bbc_incident_rss_v1.json")

if __name__ == "__main__":
    crawl_bbc_rss()
