# -*- coding: utf-8 -*-
"""
BBC 뉴스 크롤러
BBC RSS 피드를 통해 뉴스 기사를 크롤링
"""

import feedparser
from newspaper import Article
from typing import List, Dict, Optional, Any
from .base_news_crawler import BaseNewsCrawler


class BBCNewsCrawler(BaseNewsCrawler):
    """BBC 뉴스 크롤러"""
    
    def __init__(self, config_file: str = "crawler_config.json"):
        super().__init__(config_file)
        self.source_name = "BBC News"
        self.rss_url = self.config.get("sources", {}).get("bbc", {}).get("rss_url", "")
    
    def get_rss_feed(self) -> Optional[feedparser.FeedParserDict]:
        """RSS 피드 가져오기"""
        try:
            feed = feedparser.parse(self.rss_url)
            if feed.bozo:
                print(f"⚠️ RSS 피드 파싱 경고: {feed.bozo_exception}")
            return feed
        except Exception as e:
            print(f"❌ RSS 피드 오류: {e}")
            return None
    
    def extract_article_content(self, url: str) -> Optional[Dict[str, Any]]:
        """기사 내용 추출"""
        try:
            article = Article(url)
            article.download()
            article.parse()
            
            return {
                'title': article.title,
                'content': article.text,
                'authors': ', '.join(article.authors) if article.authors else 'Unknown',
                'publish_date': str(article.publish_date) if article.publish_date else 'Unknown',
                'url': url,
                'summary': article.summary if hasattr(article, 'summary') else ''
            }
        except Exception as e:
            print(f"❌ 기사 추출 오류 ({url}): {e}")
            return None
    
    def crawl(self, max_articles: int = 10) -> List[Dict[str, Any]]:
        """BBC 뉴스 크롤링 실행"""
        print(f"🔄 {self.source_name} 크롤링 시작...")
        
        feed = self.get_rss_feed()
        if not feed:
            return []
        
        articles = []
        for i, entry in enumerate(feed.entries[:max_articles]):
            print(f"  📰 기사 처리 중 {i+1}/{min(max_articles, len(feed.entries))}...")
            
            article_data = self.extract_article_content(entry.link)
            if article_data:
                # RSS 정보 추가
                article_data['rss_title'] = getattr(entry, 'title', '')
                article_data['rss_published'] = getattr(entry, 'published', '')
                article_data['rss_summary'] = getattr(entry, 'summary', '')
                article_data['source'] = self.source_name
                
                # 분류 정보 추가
                article_data['category'] = self.categorize_article(
                    article_data['title'], article_data['content']
                )
                article_data['countries'] = self.extract_countries(
                    article_data['title'] + " " + article_data['content']
                )
                article_data['is_incident'] = self.is_incident(
                    article_data['title'] + " " + article_data['content']
                )
                
                articles.append(article_data)
        
        print(f"✅ {self.source_name} 크롤링 완료: {len(articles)}개 기사")
        return articles
