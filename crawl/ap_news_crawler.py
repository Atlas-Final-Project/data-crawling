# -*- coding: utf-8 -*-
"""
AP News 크롤러
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional
from .base_news_crawler import BaseNewsCrawler


class APNewsCrawler(BaseNewsCrawler):
    """AP News 크롤러"""
    
    def __init__(self, config_file: str = "crawler_config.json"):
        super().__init__(config_file)
        self.source_name = "AP News"
        self.base_url = self.config.get("sources", {}).get("ap", {}).get("base_url", "")
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }        
        self.request_delay = 2  # 요청 간 기본 지연 시간 (초)
        self.max_delay = 30  # 최대 지연 시간 (초)
        self.current_delay = self.request_delay  # 현재 지연 시간
        self.retry_count = 0  # 연속 실패 횟수
    
    def _adjust_delay(self, success: bool = True):
        """요청 성공/실패에 따른 지연 시간 조절 (지수 백오프)"""
        if success:
            # 성공 시 지연 시간을 기본값으로 리셋
            self.current_delay = self.request_delay
            self.retry_count = 0
        else:
            # 실패 시 지연 시간을 지수적으로 증가
            self.retry_count += 1
            self.current_delay = min(self.request_delay * (2 ** self.retry_count), self.max_delay)
            print(f"⚠️ {self.source_name}: 지연 시간 조절 - {self.current_delay}초 대기")
    
    def extract_article_links(self, limit: int = 50) -> List[str]:
        """AP 뉴스에서 기사 링크 추출 (429 에러 처리 포함)"""
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=10)
            
            # 429 에러 체크
            if response.status_code == 429:
                print(f"⚠️ {self.source_name}: 429 Too Many Requests 발생")
                raise requests.exceptions.HTTPError("429 Too Many Requests")
            
            response.raise_for_status()
            
            # 간단한 정규식을 사용하여 링크 추출
            article_links = []
            
            # /article/ 패턴이 포함된 링크 찾기
            pattern = r'href=["\']([^"\']*?/article/[^"\']*?)["\']'
            matches = re.findall(pattern, response.text)
            
            for href in matches:
                if href.startswith('/'):
                    href = urljoin("https://apnews.com", href)
                if href not in article_links:
                    article_links.append(href)
                    if len(article_links) >= limit:
                        break
            
            return article_links
            
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                print(f"❌ {self.source_name}: 요청 제한에 걸렸습니다. 나중에 다시 시도해주세요.")
                raise Exception("AP_NEWS_429_ERROR")
            else:
                print(f"❌ AP HTTP 오류: {e}")
                return []
        except Exception as e:
            if "AP_NEWS_429_ERROR" in str(e):
                raise
            print(f"❌ AP 링크 추출 오류: {e}")
            return []
    def extract_article_data(self, url: str) -> Optional[Dict[str, Any]]:
        """개별 기사 데이터 추출 (429 에러 처리 및 지연 시간 포함)"""
        try:
            # 요청 간 지연 시간 추가
            time.sleep(self.request_delay)
            
            response = requests.get(url, headers=self.headers, timeout=10)
            
            # 429 에러 체크
            if response.status_code == 429:
                print(f"⚠️ {self.source_name}: 429 Too Many Requests 발생 (기사 추출)")
                raise requests.exceptions.HTTPError("429 Too Many Requests")
            
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 제목 추출
            title_elem = soup.find('h1') or soup.find('title')
            title = title_elem.get_text(strip=True) if title_elem else "제목 없음"
            
            # 내용 추출
            content_elems = soup.find_all('p')
            content = ' '.join([p.get_text(strip=True) for p in content_elems[:10]])
            
            return {
                'title': title,
                'content': content,
                'url': url,
                'source': self.source_name
            }
            
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                print(f"❌ {self.source_name}: 요청 제한에 걸렸습니다 ({url})")
                raise Exception("AP_NEWS_429_ERROR")
            else:
                print(f"❌ AP HTTP 오류 ({url}): {e}")
                return None
        except Exception as e:
            if "AP_NEWS_429_ERROR" in str(e):
                raise
            print(f"❌ AP 기사 추출 오류 ({url}): {e}")
            return None
    def crawl(self, max_articles: int = 10) -> List[Dict[str, Any]]:
        """AP News 크롤링 실행 (429 에러 처리 포함)"""
        print(f"🔄 {self.source_name} 크롤링 시작...")
        
        try:
            # 링크 추출
            links = self.extract_article_links(max_articles)
            if not links:
                print(f"❌ {self.source_name}: 기사 링크를 찾을 수 없습니다.")
                return []
            
            articles = []
            for i, url in enumerate(links[:max_articles]):
                print(f"  📰 기사 처리 중 {i+1}/{min(max_articles, len(links))}...")
                
                try:
                    article_data = self.extract_article_data(url)
                    if article_data:
                        # 분류 정보 추가
                        full_text = article_data['title'] + " " + article_data['content']
                        article_data['category'] = self.categorize_article(
                            article_data['title'], article_data['content']
                        )
                        article_data['countries'] = self.extract_countries(full_text)
                        article_data['is_incident'] = self.is_incident(full_text)
                        articles.append(article_data)
                except Exception as e:
                    if "AP_NEWS_429_ERROR" in str(e):
                        print(f"⚠️ {self.source_name}: 429 에러로 인해 크롤링을 중단합니다.")
                        raise Exception("AP_NEWS_429_ERROR")
                    else:
                        print(f"❌ 기사 처리 오류: {e}")
                        continue
            
            print(f"✅ {self.source_name} 크롤링 완료: {len(articles)}개 기사")
            return articles
            
        except Exception as e:
            if "AP_NEWS_429_ERROR" in str(e):
                raise
            print(f"❌ {self.source_name} 크롤링 오류: {e}")
            return []
