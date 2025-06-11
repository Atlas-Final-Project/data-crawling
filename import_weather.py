from db import MongoDBManager
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

def import_weather():
    with MongoDBManager(connection_string=MONGO_URI, database_name=DATABASE_NAME) as db_manager:
        print("날씨 데이터 가져오기 시작...")
        try:
            # JSON 파일에서 대사관 데이터 가져오기
            csv_file_path = "rain_data.csv"
            inserted_ids = db_manager.insert_from_csv(collection_name="weather", csv_file_path=csv_file_path, add_timestamp=False)
            print(f"✅ JSON에서 {len(inserted_ids)}개 날씨 데이터 삽입 완료")
        except Exception as e:
            print(f"❌ JSON 삽입 실패: {e}")

def check_weather():
    with MongoDBManager(connection_string=MONGO_URI, database_name=DATABASE_NAME) as db_manager:
        count = db_manager.count_documents("weather")
        print(f"📊 전체 날씨 수: {count}")
        
        for document in db_manager.find_many("weather", limit=5):
            print(f"🌤️ 날씨 데이터: {document}")

if __name__ == "__main__":
    print("날씨 데이터 가져오기 시작...")
    import_weather()
    print("날씨 데이터 가져오기가 완료되었습니다.\n")

    print("날씨 데이터 확인 중...")
    check_weather()
    print("날씨 데이터가 성공적으로 확인되었습니다.")
