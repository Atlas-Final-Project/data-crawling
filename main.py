"""
통합 뉴스 크롤러 실행 파일 (자동 스케줄링 포함)
crawl 패키지를 사용하여 뉴스를 크롤링하고 MongoDB에 저장합니다.
"""

import os
import time
import logging
import schedule
from datetime import datetime, timedelta
from crawl import UnifiedNewsCrawler
from db import MongoDBManager
from news_classification import NewsLocationExtractor


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# AP News 재시도 관리를 위한 전역 변수
ap_retry_time = None

def save_articles_to_db(articles, db_manager, location_extractor):
    """기사를 MongoDB에 저장 (제목, 발행일, 본문, 소스, 위치 정보 포함)"""
    if not articles:
        logging.warning("저장할 기사가 없습니다.")
        return 0
    
    try:
        # 중복 방지를 위해 제목을 기준으로 upsert
        inserted_count = 0
        updated_count = 0
        
        for article in articles:
            # 발행일 정규화 (BaseNewsCrawler의 normalize_date와 동일한 로직)
            raw_published = (article.get('rss_published', '') or 
                        article.get('published', '') or
                        article.get('publish_date', '')) or article.get('pubDate', '')
            
            if not raw_published or raw_published == 'Unknown':
                published = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                try:
                    from dateutil import parser as date_parser
                    parsed_date = date_parser.parse(str(raw_published))
                    published = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    published = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 위치 정보 추출
            title = article.get('title', '')
            content = article.get('content', '')
            full_text = f"{title} {content}"
            
            locations = []
            if full_text.strip():
                try:
                    extracted_locations = location_extractor.extract_locations(
                        text=full_text, 
                        min_score=0.9, 
                        min_length=2
                    )
                    locations = [loc['word'] for loc in extracted_locations]
                    if locations:
                        logging.info(f"위치 추출 완료: {locations} - {title[:50]}...")
                except Exception as e:
                    logging.warning(f"위치 추출 중 오류: {e} - {title[:50]}...")
                    locations = []
            
            # 필요한 필드만 추출하여 간소화된 기사 데이터 생성 (국가, 카테고리, 위치 포함)
            simplified_article = {
                "title": title,
                "published": published,
                "content": content,
                "source": article.get('source', 'Unknown'),
                "category": article.get('category', 'General'),
                "countries": article.get('countries', ['Unknown']),
                "locations": locations,  # 새로 추가된 위치 정보
                "crawled_at": datetime.now().isoformat()
            }
            # 제목과 소스를 기준으로 중복 확인 후 upsert (더 정확한 중복 처리)
            filter_dict = {
                "title": simplified_article.get("title", ""),
                "source": simplified_article.get("source", "Unknown")
            }
            result = db_manager.upsert("news", filter_dict, simplified_article)
            
            if result == "updated":
                updated_count += 1
            else:
                inserted_count += 1
        
        logging.info(f"DB 저장 완료 - 새로 삽입: {inserted_count}개, 업데이트: {updated_count}개")
        return inserted_count + updated_count
        
    except Exception as e:
        logging.error(f"DB 저장 중 오류 발생: {e}")
        return 0

def crawl_and_save():
    """크롤링 실행 및 DB 저장"""
    global ap_retry_time
    logging.info("🚀 자동 뉴스 크롤링 시작...")
    
    try:
        # 통합 크롤러 초기화
        unified_crawler = UnifiedNewsCrawler()
        
        # 위치 추출기 초기화
        location_extractor = NewsLocationExtractor()
        logging.info("📍 위치 추출기 초기화 완료")
        
        # MongoDB 연결 (환경변수 또는 기본값 사용)
        # MONGO_URI와 DATABASE_NAME을 .env 파일에 설정하거나
        # 직접 지정 (데이터베이스명: Atlas)
        db_manager = MongoDBManager(
            connection_string=os.getenv("MONGO_URI"),
            database_name="Atlas"
        )
        
        # 모든 소스에서 크롤링 (기사를 최대한 많이 수집)
        max_articles_per_source = 50  # 소스당 최대 50개 기사
        
        logging.info("모든 소스에서 크롤링 시작...")
        all_articles = []
        
        # 각 소스별로 개별 크롤링
        sources = ['bbc', 'fox', 'ap']
        for source in sources:
            try:
                # AP News의 경우 재시도 시간 체크
                if source == 'ap' and ap_retry_time:
                    current_time = datetime.now()
                    if current_time < ap_retry_time:
                        remaining_minutes = int((ap_retry_time - current_time).total_seconds() / 60)
                        logging.info(f"AP News 재시도 대기 중... 남은 시간: {remaining_minutes}분")
                        continue
                    else:
                        # 재시도 시간이 지나면 초기화
                        ap_retry_time = None
                        logging.info("AP News 재시도 시간이 되었습니다. 크롤링을 재개합니다.")
                
                logging.info(f"{source.upper()} 크롤링 시작...")
                articles = unified_crawler.crawl_single(source, max_articles_per_source)
                all_articles.extend(articles)
                logging.info(f"{source.upper()} 크롤링 완료: {len(articles)}개 기사")
                
            except Exception as e:
                error_msg = str(e)
                if source == 'ap' and "AP_NEWS_429_ERROR" in error_msg:
                    # AP News 429 에러 처리
                    ap_retry_time = datetime.now() + timedelta(minutes=40)
                    logging.warning(f"⚠️ AP News 429 에러 발생. 40분 후 재시도: {ap_retry_time.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    logging.error(f"{source.upper()} 크롤링 오류: {e}")
                continue        # 수집된 기사를 DB에 저장
        if all_articles:
            saved_count = save_articles_to_db(all_articles, db_manager, location_extractor)
            logging.info(f"✅ 총 {len(all_articles)}개 기사 수집, {saved_count}개 DB 저장 완료")
        else:
            logging.warning("수집된 기사가 없습니다.")
        
        # DB 연결 종료
        db_manager.close_connection()
    except Exception as e:
        logging.error(f"크롤링 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

def main():
    """메인 실행 함수 - 바로 10분마다 자동 크롤링 시작"""
    global ap_retry_time
    print("🗞️ 통합 뉴스 크롤러 - 자동 모드")
    print("=" * 50)
    print("🕒 10분마다 자동 크롤링을 시작합니다...")
    print("데이터베이스: Atlas, 컬렉션: news")
    print("AP News 429 에러 시 40분 후 자동 재시도")
    print("중단하려면 Ctrl+C를 누르세요.")
    print("=" * 50)
    
    try:
        # 첫 실행
        crawl_and_save()
        
        # 10분마다 실행 스케줄 설정
        schedule.every(10).minutes.do(crawl_and_save)
        
        print(f"\n⏰ 다음 크롤링: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 이후 10분마다")
        if ap_retry_time:
            print(f"📍 AP News 재시도 예정: {ap_retry_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("스케줄러가 실행 중입니다...")
        
        # 스케줄 실행
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 스케줄 확인
            
            # AP News 상태 주기적 출력 (5분마다)
            if ap_retry_time and datetime.now().minute % 5 == 0:
                remaining_minutes = int((ap_retry_time - datetime.now()).total_seconds() / 60)
                if remaining_minutes > 0:
                    print(f"📍 AP News 재시도까지 남은 시간: {remaining_minutes}분")
            
    except KeyboardInterrupt:
        print("\n⭕ 자동 크롤링이 중단되었습니다.")
        logging.info("사용자에 의해 크롤링 스케줄러가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        logging.error(f"메인 함수 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
