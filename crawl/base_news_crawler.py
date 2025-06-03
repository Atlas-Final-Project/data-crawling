# -*- coding: utf-8 -*-
"""
기본 뉴스 크롤러 클래스
모든 뉴스 크롤러의 공통 기능을 제공하는 기본 클래스
"""

import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from dateutil import parser as date_parser


class BaseNewsCrawler:
    """뉴스 크롤러 기본 클래스"""
    
    def __init__(self, config_file: str = "crawler_config.json"):
        """
        설정 파일을 로드하여 초기화
        
        Args:
            config_file (str): 설정 파일 경로
        """
        self.config = self.load_config(config_file)
        self.categories = self.config.get("categories", {})
        self.countries = self.config.get("countries", {})
        self.incident_keywords = set(self.config.get("incident_keywords", []))
        
    def load_config(self, config_file: str) -> Dict[str, Any]:
        """설정 파일 로드"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ 설정 파일 로드 오류: {e}")
            return {}
    
    def extract_countries(self, text: str) -> List[str]:
        """텍스트에서 국가명 추출"""
        if not text:
            return ["Unknown"]
            
        countries = []
        text_lower = text.lower()
        
        for keyword, country in self.countries.items():
            if keyword in text_lower and country not in countries:
                countries.append(country)
        
        return list(set(countries)) if countries else ["Unknown"]
    
    def categorize_article(self, title: str, content: str) -> str:
        """기사 카테고리 분류"""
        text_to_analyze = (title + " " + content).lower()
        
        category_scores = {}
        for category, keywords in self.categories.items():
            score = sum(1 for keyword in keywords if keyword in text_to_analyze)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores.keys(), key=lambda k: category_scores[k])
        else:
            return "General"
    
    def is_incident(self, text: str) -> bool:
        """사건·사고 여부 판단"""
        if not text:
            return False
        return any(keyword in text.lower() for keyword in self.incident_keywords)
    
    def normalize_date(self, date_string: str) -> str:
        """다양한 날짜 형식을 표준 형식(YYYY-MM-DD HH:MM:SS)으로 변환"""
        if not date_string:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # dateutil.parser를 사용하여 다양한 형식의 날짜를 파싱
            parsed_date = date_parser.parse(date_string)
            return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            # 파싱 실패 시 현재 시간 반환
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def save_to_json(self, articles: List[Dict[str, Any]], filename: str) -> Optional[str]:
        """기사를 JSON 파일로 저장 (제목, 발행일, 본문, 소스만)"""
        if not articles:
            print("❌ 저장할 기사가 없습니다.")
            return None
        
        # 필요한 필드만 추출
        simplified_articles = []
        for article in articles:
            # 발행일 필드를 다양한 소스에서 찾기
            raw_published = (article.get('rss_published', '') or 
                        article.get('published', '') or 
                        article.get('pubDate', ''))
            
            # 발행일을 표준 형식으로 변환
            published = self.normalize_date(raw_published)
            simplified_article = {
                "title": article.get('title', ''),
                "published": published,
                "content": article.get('content', ''),
                "source": article.get('source', 'Unknown'),
                "category": article.get('category', 'General'),
                "countries": article.get('countries', ['Unknown'])
            }
            simplified_articles.append(simplified_article)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(simplified_articles, f, ensure_ascii=False, indent=2)
            print(f"✅ {len(simplified_articles)}개 기사를 {filename}에 저장했습니다.")
            return filename
        except Exception as e:
            print(f"❌ JSON 저장 오류: {e}")
            return None
    
    def crawl(self, max_articles: int = 10) -> List[Dict[str, Any]]:
        """크롤링 실행 (하위 클래스에서 구현 필요)"""
        raise NotImplementedError("하위 클래스에서 crawl 메서드를 구현해야 합니다.")
