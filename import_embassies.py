from db import MongoDBManager
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

def import_embassies():
    with MongoDBManager(connection_string=MONGO_URI, database_name=DATABASE_NAME) as db_manager:
        print("대사관 데이터 가져오기 시작...")
        try:
            # JSON 파일에서 대사관 데이터 가져오기
            json_file_path = "대사관.json"
            inserted_ids = db_manager.insert_from_json("embassies", json_file_path)
            print(f"✅ JSON에서 {len(inserted_ids)}개 대사관 데이터 삽입 완료")
        except Exception as e:
            print(f"❌ JSON 삽입 실패: {e}")

def check_embassies():
    with MongoDBManager(connection_string=MONGO_URI, database_name=DATABASE_NAME) as db_manager:
        count = db_manager.count_documents("embassies")
        print(f"📊 전체 대사관 수: {count}")
        
        for embassy in db_manager.find_many("embassies", limit=5):
            print(f"📋 대사관: {embassy['mission_name']}, 전화번호: {embassy['phone_number']}, 주소: {embassy['address']}")

if __name__ == "__main__":
    print("대사관 데이터 가져오기 시작...")
    import_embassies()
    print("대사관 데이터 가져오기가 완료되었습니다.\n")

    print("대사관 데이터 확인 중...")
    check_embassies()
    print("대사관 데이터가 성공적으로 확인되었습니다.")
