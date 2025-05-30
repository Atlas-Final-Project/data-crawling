import feedparser
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# Google 뉴스 RSS (전 세계)
RSS_URL = "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en"

# 사건·사고 및 질병 관련 키워드
incident_keywords = {
    "attack", "killed", "murder", "shooting", "explosion", "crash", "disaster",
    "terror", "riot", "violence", "accident", "earthquake", "fire", "storm",
    "protest", "assault", "hurricane", "drowning", "death", "injured",
    "outbreak", "virus", "infection", "pandemic", "epidemic", "disease",
    "flu", "covid", "ebola", "zika", "sars", "health", "vaccine", "contagion"
}

def clean_summary(html_summary):
    soup = BeautifulSoup(html_summary, "html.parser")
    return soup.get_text().strip()

def is_relevant(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in incident_keywords)

def crawl_google_news(limit=50, days=7):
    feed = feedparser.parse(RSS_URL)
    results = []
    count = 0

    threshold = datetime.utcnow() - timedelta(days=days)

    for entry in feed.entries:
        # 날짜 파싱
        pub_date = getattr(entry, "published_parsed", None)
        if pub_date:
            published = datetime(*pub_date[:6])
            if published < threshold:
                continue
        else:
            published = None

        full_text = f"{entry.title} {entry.summary}"
        if is_relevant(full_text):
            item = {
                "title": entry.title,
                "url": entry.link,
                "summary": clean_summary(entry.summary),
                "date": published.isoformat() if published else None,
                "source": "Google News"
            }
            results.append(item)
            count += 1
            if count >= limit:
                break

    with open("google_incident_news.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"{len(results)}개의 관련 뉴스를 저장했습니다 → google_incident_news.json")

if __name__ == "__main__":
    crawl_google_news(limit=50, days=7)
