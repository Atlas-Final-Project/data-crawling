# -*- coding: utf-8 -*-
"""
Fox News 크롤러
"""

import feedparser
from typing import List, Dict, Any
from .base_news_crawler import BaseNewsCrawler


class FoxNewsCrawler(BaseNewsCrawler):
    """Fox News 크롤러"""
    
    def __init__(self, config_file: str = "crawler_config.json"):
        super().__init__(config_file)
        self.source_name = "Fox News"
        self.rss_urls = self.config.get("sources", {}).get("fox", {}).get("rss_urls", [])
    
    def crawl_rss_feed(self, rss_url: str, max_articles: int = 10) -> List[Dict[str, Any]]:
        """단일 RSS 피드 크롤링"""
        try:
            feed = feedparser.parse(rss_url)
            articles = []
            
            for entry in feed.entries[:max_articles]:
                article_data = {
                    'title': getattr(entry, 'title', ''),
                    'content': getattr(entry, 'summary', ''),
                    'url': getattr(entry, 'link', ''),
                    'published': getattr(entry, 'published', ''),
                    'source': self.source_name,
                    'rss_url': rss_url
                }
                
                # 분류 정보 추가
                full_text = article_data['title'] + " " + article_data['content']
                article_data['category'] = self.categorize_article(
                    article_data['title'], article_data['content']
                )
                article_data['countries'] = self.extract_countries(full_text)
                article_data['is_incident'] = self.is_incident(full_text)
                
                articles.append(article_data)
            
            return articles
            
        except Exception as e:
            print(f"❌ Fox RSS 크롤링 오류 ({rss_url}): {e}")
            return []
    
    def crawl(self, max_articles: int = 10) -> List[Dict[str, Any]]:
        """Fox News 크롤링 실행"""
        print(f"🔄 {self.source_name} 크롤링 시작...")
        
        all_articles = []
        for rss_url in self.rss_urls:
            print(f"  📡 RSS 피드 처리: {rss_url}")
            articles = self.crawl_rss_feed(rss_url, max_articles // len(self.rss_urls))
            all_articles.extend(articles)
        
        print(f"✅ {self.source_name} 크롤링 완료: {len(all_articles)}개 기사")
        return all_articles
