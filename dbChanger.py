from db import MongoDBManager
from news_classification import NewsLocationExtractor
import os
import logging
from datetime import datetime
from bson import ObjectId

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_changer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def update_news_locations():
    """MongoDB에 저장된 뉴스 기사들의 위치 정보를 news_classification.py를 사용해 업데이트"""
    try:
        # MongoDB 연결
        db_manager = MongoDBManager(
            connection_string=os.getenv("MONGO_URI"), 
            database_name="Atlas"
        )
        
        # 위치 추출기 초기화
        location_extractor = NewsLocationExtractor()
        
        # 모든 뉴스 기사 가져오기
        logging.info("📰 뉴스 기사들을 가져오는 중...")
        all_articles = db_manager.find_many("news")
        
        if not all_articles:
            logging.warning("데이터베이스에서 뉴스 기사를 찾을 수 없습니다.")
            return
            
        logging.info(f"총 {len(all_articles)}개의 뉴스 기사를 찾았습니다.")
        
        updated_count = 0
        error_count = 0
        
        for i, article in enumerate(all_articles, 1):
            try:
                article_id = article.get('_id')
                title = article.get('title', '')
                content = article.get('content', '')
                
                # 제목과 본문을 합쳐서 분석
                full_text = f"{title} {content}"
                
                if not full_text.strip():
                    logging.warning(f"기사 {i}: 제목과 본문이 비어있습니다. ID: {article_id}")
                    continue
                
                # 위치 추출 (최소 신뢰도 0.85, 최소 길이 2로 설정)
                locations = location_extractor.extract_locations(
                    text=full_text, 
                    min_score=0.9, 
                    min_length=2
                )
                
                # 추출된 위치들을 리스트로 변환
                location_names = [loc['word'] for loc in locations]
                
                # 기존 countries 필드와 새로운 locations 필드 업데이트
                update_data = {
                    'locations': location_names
                }
                  # 기사 업데이트
                try:
                    # _id를 ObjectId로 변환
                    object_id = ObjectId(article_id)
                    filter_dict = {'_id': object_id}
                    result = db_manager.update_one("news", filter_dict, update_data)
                    
                    if result > 0:
                        updated_count += 1
                        logging.info(f"기사 {i}/{len(all_articles)} 업데이트 완료: {len(location_names)}개 위치 추출 - {location_names}")
                    else:
                        logging.warning(f"기사 {i}: 업데이트 실패. ID: {article_id}")
                except Exception as id_error:
                    logging.error(f"기사 {i}: ObjectId 변환 또는 업데이트 오류: {id_error}, ID: {article_id}")
                    continue
                    
            except Exception as e:
                error_count += 1
                logging.error(f"기사 {i} 처리 중 오류 발생: {e}")
                continue
        
        logging.info(f"✅ 위치 정보 업데이트 완료!")
        logging.info(f"   - 성공적으로 업데이트된 기사: {updated_count}개")
        logging.info(f"   - 오류 발생한 기사: {error_count}개")
        logging.info(f"   - 전체 기사: {len(all_articles)}개")
        
        # DB 연결 종료
        db_manager.close_connection()
        
    except Exception as e:
        logging.error(f"위치 정보 업데이트 중 오류 발생: {e}")
        raise

def sample_location_extraction():
    """샘플 기사들의 위치 추출 결과를 미리 확인해보는 함수"""
    try:
        # MongoDB 연결
        db_manager = MongoDBManager(
            connection_string=os.getenv("MONGO_URI"), 
            database_name="Atlas"
        )
        
        # 위치 추출기 초기화
        location_extractor = NewsLocationExtractor()
        
        # 샘플로 5개 기사만 가져오기
        sample_articles = db_manager.find_many("news", limit=5)
        
        if not sample_articles:
            logging.warning("샘플 기사를 찾을 수 없습니다.")
            return
            
        logging.info(f"🔍 샘플 {len(sample_articles)}개 기사의 위치 추출 미리보기:")
        print("=" * 80)
        
        for i, article in enumerate(sample_articles, 1):
            title = article.get('title', '')
            content = article.get('content', '')
            current_countries = article.get('countries', [])
            
            # 제목과 본문을 합쳐서 분석
            full_text = f"{title} {content}"
            
            print(f"\n📰 기사 {i}: {title[:50]}...")
            print(f"현재 countries: {current_countries}")
            
            if full_text.strip():
                locations = location_extractor.extract_locations(
                    text=full_text, 
                    min_score=0.85, 
                    min_length=2
                )
                location_names = [loc['word'] for loc in locations]
                print(f"추출된 locations: {location_names}")
                
                # 신뢰도 점수도 표시
                if locations:
                    print("위치별 신뢰도:")
                    for loc in locations:
                        print(f"  - {loc['word']}: {loc['score']:.4f}")
            else:
                print("텍스트 내용이 없습니다.")
            
            print("-" * 80)
        
        # DB 연결 종료
        db_manager.close_connection()
        
    except Exception as e:
        logging.error(f"샘플 위치 추출 중 오류 발생: {e}")
        raise

def main():
    db_manager = MongoDBManager(connection_string=os.getenv("MONGO_URI"), database_name="Atlas")
    
    print("🔧 뉴스 데이터베이스 위치 정보 업데이트 도구")
    print("=" * 60)
    print("1. 샘플 기사 위치 추출 미리보기 (5개)")
    print("2. 전체 기사 위치 정보 업데이트")
    print("3. 종료")
    print("=" * 60)
    
    while True:
        try:
            choice = input("\n선택하세요 (1-3): ").strip()
            
            if choice == '1':
                print("\n🔍 샘플 기사 위치 추출을 시작합니다...")
                sample_location_extraction()
                
            elif choice == '2':
                print("\n⚠️  전체 기사 위치 정보 업데이트를 시작합니다.")
                confirm = input("계속하시겠습니까? (y/N): ").strip().lower()
                if confirm in ['y', 'yes']:
                    update_news_locations()
                else:
                    print("업데이트가 취소되었습니다.")
                    
            elif choice == '3':
                print("프로그램을 종료합니다.")
                break
                
            else:
                print("잘못된 선택입니다. 1-3 사이의 숫자를 입력해주세요.")
                
        except KeyboardInterrupt:
            print("\n\n프로그램이 중단되었습니다.")
            break
        except Exception as e:
            print(f"오류 발생: {e}")
            logging.error(f"메인 함수 오류: {e}")
    
    # DB 연결 종료
    db_manager.close_connection()

if __name__ == "__main__":
    main()