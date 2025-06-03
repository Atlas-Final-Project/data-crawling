# -*- coding: utf-8 -*-
"""
통합 뉴스 크롤러
"""

from datetime import datetime
from collections import Counter
from typing import List, Dict, Any
from .bbc_news_crawler import BBCNewsCrawler
from .fox_news_crawler import FoxNewsCrawler
from .ap_news_crawler import APNewsCrawler


class UnifiedNewsCrawler:
    """통합 뉴스 크롤러"""
    
    def __init__(self, config_file: str = "crawler_config.json"):
        """
        통합 크롤러 초기화
        
        Args:
            config_file (str): 설정 파일 경로
        """
        self.config_file = config_file
        self.crawlers = {
            'bbc': BBCNewsCrawler(config_file),
            'fox': FoxNewsCrawler(config_file),
            'ap': APNewsCrawler(config_file)
        }
    
    def crawl_all(self, max_articles_per_source: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """모든 소스에서 크롤링 실행"""
        print("🚀 통합 뉴스 크롤링 시작...")
        print("="*60)
        
        results = {}
        all_articles = []
        
        for source_name, crawler in self.crawlers.items():
            try:
                articles = crawler.crawl(max_articles_per_source)
                results[source_name] = articles
                all_articles.extend(articles)
                print()
            except Exception as e:
                print(f"❌ {source_name} 크롤링 오류: {e}")
                results[source_name] = []
        
        # 통합 결과 저장
        if all_articles:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"unified_news_{timestamp}.json"
            
            # 첫 번째 크롤러 인스턴스를 사용해서 저장
            if self.crawlers:
                first_crawler = next(iter(self.crawlers.values()))
                first_crawler.save_to_json(all_articles, filename)
        
        self.print_summary(results, all_articles)
        return results
    
    def crawl_single(self, source: str, max_articles: int = 10) -> List[Dict[str, Any]]:
        """단일 소스 크롤링"""
        if source not in self.crawlers:
            print(f"❌ 지원하지 않는 소스: {source}")
            print(f"지원 소스: {list(self.crawlers.keys())}")
            return []
        
        crawler = self.crawlers[source]
        return crawler.crawl(max_articles)
    
    def print_summary(self, results: Dict[str, List[Dict[str, Any]]], all_articles: List[Dict[str, Any]]):
        """크롤링 결과 요약 출력"""
        print("="*60)
        print("📊 크롤링 결과 요약")
        print("="*60)
        
        # 소스별 통계
        for source, articles in results.items():
            print(f"📰 {source.upper()}: {len(articles)}개 기사")
        
        print(f"📈 총 수집된 기사: {len(all_articles)}개")
        
        if all_articles:
            # 카테고리별 분포
            categories = Counter(article.get('category', 'Unknown') for article in all_articles)
            print(f"\n📂 카테고리별 분포:")
            for category, count in categories.most_common():
                print(f"  • {category}: {count}개")
            
            # 국가별 언급 빈도
            all_countries = []
            for article in all_articles:
                all_countries.extend(article.get('countries', []))
            country_counts = Counter(all_countries)
            
            print(f"\n🌍 주요 언급 국가 (상위 10개):")
            for country, count in country_counts.most_common(10):
                if country != "Unknown":
                    print(f"  • {country}: {count}회")
            
            # 사건사고 기사 수
            incident_count = sum(1 for article in all_articles if article.get('is_incident', False))
            print(f"\n🚨 사건·사고 관련 기사: {incident_count}개")
        
        print("="*60)
