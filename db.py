from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.database import Database
from bson import ObjectId
from typing import Dict, List, Optional, Any, Union
import logging
import json
import csv
import os
from datetime import datetime
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class MongoDBManager:
    """MongoDB CRUD 작업을 처리하는 클래스"""
    
    def __init__(self, connection_string: Optional[str] = None, database_name: Optional[str] = None):
        """
        MongoDB 연결 초기화
        
        Args:
            connection_string (str): MongoDB 연결 문자열 (기본값: 환경변수 MONGO_URI)
            database_name (str): 사용할 데이터베이스 이름 (기본값: 환경변수 DATABASE_NAME)
        """
        # 환경변수에서 기본값 가져오기
        self.connection_string = connection_string or os.getenv("MONGO_URI")
        self.database_name = database_name or os.getenv("DATABASE_NAME")
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self._connect()
    
    def _connect(self):
        """MongoDB에 연결"""
        try:
            self.client = MongoClient(self.connection_string)
            # 연결 테스트
            self.client.admin.command('ping')
            if self.database_name is None:
                raise ValueError("데이터베이스 이름이 지정되지 않았습니다.")
            self.db = self.client[self.database_name]
            logging.info(f"MongoDB 연결 성공: {self.database_name}")
        except ConnectionFailure as e:
            logging.error(f"MongoDB 연결 실패: {e}")
            raise
    
    def close_connection(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            logging.info("MongoDB 연결 종료")
    # CREATE 작업
    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> str:
        """
        단일 문서 삽입
        
        Args:
            collection_name (str): 컬렉션 이름
            document (dict): 삽입할 문서
            
        Returns:
            str: 삽입된 문서의 ObjectId
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            logging.info(f"문서 삽입 성공: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            logging.error(f"문서 삽입 실패: {e}")
            raise
    
    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> List[str]:
        """
        여러 문서 삽입
        
        Args:
            collection_name (str): 컬렉션 이름
            documents (list): 삽입할 문서들의 리스트
            
        Returns:
            list: 삽입된 문서들의 ObjectId 리스트
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            logging.info(f"{len(result.inserted_ids)}개 문서 삽입 성공")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logging.error(f"문서 삽입 실패: {e}")
            raise
      # READ 작업
    def find_one(self, collection_name: str, filter_dict: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        단일 문서 조회
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 검색 조건
            
        Returns:
            dict: 조회된 문서 또는 None
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
            
            collection = self.db[collection_name]
            filter_dict = filter_dict or {}
            result = collection.find_one(filter_dict)
            if result:
                result['_id'] = str(result['_id'])  # ObjectId를 문자열로 변환
            return result
        except Exception as e:
            logging.error(f"문서 조회 실패: {e}")
            raise
    
    def find_many(self, collection_name: str, filter_dict: Optional[Dict[str, Any]] = None, 
                  limit: int = 0, sort_field: Optional[str] = None, sort_order: int = 1) -> List[Dict[str, Any]]:
        """
        여러 문서 조회
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 검색 조건
            limit (int): 결과 제한 수 (0이면 제한 없음)
            sort_field (str): 정렬 필드
            sort_order (int): 정렬 순서 (1: 오름차순, -1: 내림차순)
            
        Returns:
            list: 조회된 문서들의 리스트
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            filter_dict = filter_dict or {}
            
            cursor = collection.find(filter_dict)
            
            if sort_field:
                cursor = cursor.sort(sort_field, sort_order)
            
            if limit > 0:
                cursor = cursor.limit(limit)
            
            results = list(cursor)
            
            # ObjectId를 문자열로 변환
            for result in results:
                result['_id'] = str(result['_id'])
            
            return results
        except Exception as e:
            logging.error(f"문서 조회 실패: {e}")
            raise
    
    def count_documents(self, collection_name: str, filter_dict: Optional[Dict[str, Any]] = None) -> int:
        """
        문서 개수 조회
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 검색 조건
            
        Returns:
            int: 문서 개수
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            filter_dict = filter_dict or {}
            count = collection.count_documents(filter_dict)
            return count
        except Exception as e:
            logging.error(f"문서 개수 조회 실패: {e}")
            raise
      # UPDATE 작업
    def update_one(self, collection_name: str, filter_dict: Dict[str, Any], 
                   update_dict: Dict[str, Any]) -> int:
        """
        단일 문서 업데이트
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 업데이트할 문서 조건
            update_dict (dict): 업데이트 내용
            
        Returns:
            int: 업데이트된 문서 수
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.update_one(filter_dict, {"$set": update_dict})
            logging.info(f"문서 업데이트 성공: {result.modified_count}개")
            return result.modified_count
        except Exception as e:
            logging.error(f"문서 업데이트 실패: {e}")
            raise
    
    def update_many(self, collection_name: str, filter_dict: Dict[str, Any], 
                    update_dict: Dict[str, Any]) -> int:
        """
        여러 문서 업데이트
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 업데이트할 문서들의 조건
            update_dict (dict): 업데이트 내용
            
        Returns:
            int: 업데이트된 문서 수
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.update_many(filter_dict, {"$set": update_dict})
            logging.info(f"문서 업데이트 성공: {result.modified_count}개")
            return result.modified_count
        except Exception as e:
            logging.error(f"문서 업데이트 실패: {e}")
            raise
    
    def upsert(self, collection_name: str, filter_dict: Dict[str, Any], 
               update_dict: Dict[str, Any]) -> str:
        """
        문서가 존재하면 업데이트, 없으면 삽입
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 검색/업데이트 조건
            update_dict (dict): 업데이트/삽입 내용
            
        Returns:
            str: 업데이트되거나 삽입된 문서의 ObjectId
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.update_one(
                filter_dict, 
                {"$set": update_dict}, 
                upsert=True
            )
            if result.upserted_id:
                logging.info(f"새 문서 삽입: {result.upserted_id}")
                return str(result.upserted_id)
            else:
                logging.info("기존 문서 업데이트")
                return "updated"
        except Exception as e:
            logging.error(f"Upsert 실패: {e}")
            raise
      # DELETE 작업
    def delete_one(self, collection_name: str, filter_dict: Dict[str, Any]) -> int:
        """
        단일 문서 삭제
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 삭제할 문서 조건
            
        Returns:
            int: 삭제된 문서 수
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.delete_one(filter_dict)
            logging.info(f"문서 삭제 성공: {result.deleted_count}개")
            return result.deleted_count
        except Exception as e:
            logging.error(f"문서 삭제 실패: {e}")
            raise
    
    def delete_many(self, collection_name: str, filter_dict: Dict[str, Any]) -> int:
        """
        여러 문서 삭제
        
        Args:
            collection_name (str): 컬렉션 이름
            filter_dict (dict): 삭제할 문서들의 조건
            
        Returns:
            int: 삭제된 문서 수
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            result = collection.delete_many(filter_dict)
            logging.info(f"문서 삭제 성공: {result.deleted_count}개")
            return result.deleted_count
        except Exception as e:
            logging.error(f"문서 삭제 실패: {e}")
            raise
    
    # 컬렉션 관리
    def drop_collection(self, collection_name: str):
        """
        컬렉션 삭제
        
        Args:
            collection_name (str): 삭제할 컬렉션 이름
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            self.db.drop_collection(collection_name)
            logging.info(f"컬렉션 삭제 성공: {collection_name}")
        except Exception as e:
            logging.error(f"컬렉션 삭제 실패: {e}")
            raise
    def list_collections(self) -> List[str]:
        """
        데이터베이스의 모든 컬렉션 목록 조회
        
        Returns:
            list: 컬렉션 이름 리스트
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collections = self.db.list_collection_names()
            return collections
        except Exception as e:
            logging.error(f"컬렉션 목록 조회 실패: {e}")
            raise
    
    # 파일에서 데이터 가져와서 DB에 추가
    def insert_from_json(self, collection_name: str, json_file_path: str, 
                        add_timestamp: bool = True) -> List[str]:
        """
        JSON 파일에서 데이터를 읽어와서 컬렉션에 삽입
        
        Args:
            collection_name (str): 컬렉션 이름
            json_file_path (str): JSON 파일 경로
            add_timestamp (bool): 삽입 시간 타임스탬프 추가 여부
            
        Returns:
            list: 삽입된 문서들의 ObjectId 리스트
        """
        try:
            if not os.path.exists(json_file_path):
                raise FileNotFoundError(f"JSON 파일을 찾을 수 없습니다: {json_file_path}")
            
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # 데이터가 리스트가 아닌 경우 리스트로 변환
            if not isinstance(data, list):
                data = [data]
            
            # 데이터베이스에 삽입
            inserted_ids = self.insert_many(collection_name, data)
            logging.info(f"JSON 파일에서 {len(inserted_ids)}개 문서 삽입 완료: {json_file_path}")
            
            return inserted_ids
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON 파일 파싱 오류: {e}")
            raise
        except Exception as e:
            logging.error(f"JSON 파일에서 데이터 삽입 실패: {e}")
            raise
    
    def insert_from_csv(self, collection_name: str, csv_file_path: str, 
                       encoding: str = 'utf-8', delimiter: str = ',', 
                       add_timestamp: bool = True) -> List[str]:
        """
        CSV 파일에서 데이터를 읽어와서 컬렉션에 삽입
        
        Args:
            collection_name (str): 컬렉션 이름
            csv_file_path (str): CSV 파일 경로
            encoding (str): 파일 인코딩 (기본값: utf-8)
            delimiter (str): CSV 구분자 (기본값: ,)
            add_timestamp (bool): 삽입 시간 타임스탬프 추가 여부
            
        Returns:
            list: 삽입된 문서들의 ObjectId 리스트
        """
        try:
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_file_path}")
            
            data = []
            timestamp = datetime.now().isoformat() if add_timestamp else None
            
            with open(csv_file_path, 'r', encoding=encoding, newline='') as file:
                csv_reader = csv.DictReader(file, delimiter=delimiter)
                
                for row in csv_reader:
                    # 빈 값들을 None으로 변환 (선택사항)
                    processed_row = {}
                    for key, value in row.items():
                        processed_row[key] = value if value.strip() != '' else None
            
            # 데이터베이스에 삽입
            if not data:
                logging.warning(f"CSV 파일에 데이터가 없습니다: {csv_file_path}")
                return []
            
            inserted_ids = self.insert_many(collection_name, data)
            logging.info(f"CSV 파일에서 {len(inserted_ids)}개 문서 삽입 완료: {csv_file_path}")
            
            return inserted_ids
            
        except Exception as e:
            logging.error(f"CSV 파일에서 데이터 삽입 실패: {e}")
            raise
    
    def upsert_from_json(self, collection_name: str, json_file_path: str, 
                        unique_field: str, add_timestamp: bool = True) -> Dict[str, int]:
        """
        JSON 파일에서 데이터를 읽어와서 upsert (있으면 업데이트, 없으면 삽입)
        
        Args:
            collection_name (str): 컬렉션 이름
            json_file_path (str): JSON 파일 경로
            unique_field (str): 중복 확인할 필드명
            add_timestamp (bool): 삽입/업데이트 시간 타임스탬프 추가 여부
            
        Returns:
            dict: {'inserted': 삽입된 수, 'updated': 업데이트된 수}
        """
        try:
            if not os.path.exists(json_file_path):
                raise FileNotFoundError(f"JSON 파일을 찾을 수 없습니다: {json_file_path}")
            
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # 데이터가 리스트가 아닌 경우 리스트로 변환
            if not isinstance(data, list):
                data = [data]
            
            inserted_count = 0
            updated_count = 0
            timestamp = datetime.now().isoformat() if add_timestamp else None
            
            for item in data:
                if not isinstance(item, dict) or unique_field not in item:
                    logging.warning(f"유효하지 않은 문서 또는 unique_field '{unique_field}' 누락: {item}")
                    continue
                
                # Upsert 실행
                filter_dict = {unique_field: item[unique_field]}
                result = self.upsert(collection_name, filter_dict, item)
                
                if result == "updated":
                    updated_count += 1
                else:
                    inserted_count += 1
            
            result_summary = {'inserted': inserted_count, 'updated': updated_count}
            logging.info(f"JSON Upsert 완료 - 삽입: {inserted_count}, 업데이트: {updated_count}")
            
            return result_summary
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON 파일 파싱 오류: {e}")
            raise
        except Exception as e:
            logging.error(f"JSON 파일에서 Upsert 실패: {e}")
            raise
    
    def upsert_from_csv(self, collection_name: str, csv_file_path: str, 
                       unique_field: str, encoding: str = 'utf-8', 
                       delimiter: str = ',', add_timestamp: bool = True) -> Dict[str, int]:
        """
        CSV 파일에서 데이터를 읽어와서 upsert (있으면 업데이트, 없으면 삽입)
        
        Args:
            collection_name (str): 컬렉션 이름
            csv_file_path (str): CSV 파일 경로
            unique_field (str): 중복 확인할 필드명
            encoding (str): 파일 인코딩 (기본값: utf-8)
            delimiter (str): CSV 구분자 (기본값: ,)
            add_timestamp (bool): 삽입/업데이트 시간 타임스탬프 추가 여부
            
        Returns:
            dict: {'inserted': 삽입된 수, 'updated': 업데이트된 수}
        """
        try:
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_file_path}")
            
            inserted_count = 0
            updated_count = 0
            timestamp = datetime.now().isoformat() if add_timestamp else None
            
            with open(csv_file_path, 'r', encoding=encoding, newline='') as file:
                csv_reader = csv.DictReader(file, delimiter=delimiter)
                
                for row in csv_reader:
                    if unique_field not in row:
                        logging.warning(f"unique_field '{unique_field}'가 CSV 행에 없습니다: {row}")
                        continue
                    
                    # 빈 값들을 None으로 변환
                    processed_row = {}
                    for key, value in row.items():
                        processed_row[key] = value if value.strip() != '' else None

                    # Upsert 실행
                    filter_dict = {unique_field: processed_row[unique_field]}
                    result = self.upsert(collection_name, filter_dict, processed_row)
                    
                    if result == "updated":
                        updated_count += 1
                    else:
                        inserted_count += 1
            
            result_summary = {'inserted': inserted_count, 'updated': updated_count}
            logging.info(f"CSV Upsert 완료 - 삽입: {inserted_count}, 업데이트: {updated_count}")
            
            return result_summary
            
        except Exception as e:
            logging.error(f"CSV 파일에서 Upsert 실패: {e}")
            raise
    
    def backup_collection_to_json(self, collection_name: str, output_file_path: str, 
                                 filter_dict: Optional[Dict[str, Any]] = None) -> int:
        """
        컬렉션 데이터를 JSON 파일로 백업
        
        Args:
            collection_name (str): 컬렉션 이름
            output_file_path (str): 출력 JSON 파일 경로
            filter_dict (dict): 백업할 문서 조건 (None이면 전체)
            
        Returns:
            int: 백업된 문서 수
        """
        try:
            # 데이터 조회
            documents = self.find_many(collection_name, filter_dict)
            
            # JSON 파일로 저장
            with open(output_file_path, 'w', encoding='utf-8') as file:
                json.dump(documents, file, ensure_ascii=False, indent=2, default=str)
            
            logging.info(f"컬렉션 백업 완료: {len(documents)}개 문서 -> {output_file_path}")
            return len(documents)
            
        except Exception as e:
            logging.error(f"컬렉션 백업 실패: {e}")
            raise
    
    def backup_collection_to_csv(self, collection_name: str, output_file_path: str, 
                                filter_dict: Optional[Dict[str, Any]] = None,
                                encoding: str = 'utf-8') -> int:
        """
        컬렉션 데이터를 CSV 파일로 백업
        
        Args:
            collection_name (str): 컬렉션 이름
            output_file_path (str): 출력 CSV 파일 경로
            filter_dict (dict): 백업할 문서 조건 (None이면 전체)
            encoding (str): 파일 인코딩 (기본값: utf-8)
            
        Returns:
            int: 백업된 문서 수
        """
        try:
            # 데이터 조회
            documents = self.find_many(collection_name, filter_dict)
            
            if not documents:
                logging.warning("백업할 문서가 없습니다.")
                return 0
            
            # CSV 헤더 생성 (모든 문서의 모든 키 수집)
            all_keys = set()
            for doc in documents:
                all_keys.update(doc.keys())
            
            fieldnames = sorted(list(all_keys))
            
            # CSV 파일로 저장
            with open(output_file_path, 'w', encoding=encoding, newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                
                for doc in documents:
                    # 복잡한 데이터 타입을 문자열로 변환
                    processed_doc = {}
                    for key, value in doc.items():
                        if isinstance(value, (dict, list)):
                            processed_doc[key] = json.dumps(value, ensure_ascii=False, default=str)
                        else:
                            processed_doc[key] = str(value) if value is not None else ''
                    
                    writer.writerow(processed_doc)
            
            logging.info(f"컬렉션 CSV 백업 완료: {len(documents)}개 문서 -> {output_file_path}")
            return len(documents)
            
        except Exception as e:
            logging.error(f"컬렉션 CSV 백업 실패: {e}")
            raise
    
    # 인덱스 관리
    def create_index(self, collection_name: str, index_fields: List[tuple], unique: bool = False):
        """
        인덱스 생성
        
        Args:
            collection_name (str): 컬렉션 이름
            index_fields (list): 인덱스 필드들 [(field_name, order), ...]
            unique (bool): 유니크 인덱스 여부
        """
        try:
            if self.db is None:
                raise ConnectionError("데이터베이스 연결이 없습니다.")
                
            collection = self.db[collection_name]
            collection.create_index(index_fields, unique=unique)
            logging.info(f"인덱스 생성 성공: {index_fields}")
        except Exception as e:
            logging.error(f"인덱스 생성 실패: {e}")
            raise
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close_connection()


# 사용 예시
if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    
    # MongoDB 매니저 사용 예시
    with MongoDBManager() as db_manager:
        # 기본 CRUD 예시
        news_data = {
            "title": "테스트 뉴스",
            "content": "이것은 테스트 뉴스입니다.",
            "source": "test_source",
            "created_at": "2025-06-02"
        }
        
        # 단일 문서 삽입
        inserted_id = db_manager.insert_one("news", news_data)
        print(f"삽입된 문서 ID: {inserted_id}")
        
        # 문서 조회
        found_news = db_manager.find_one("news", {"title": "테스트 뉴스"})
        print(f"조회된 문서: {found_news}")
        
        # 문서 업데이트
        updated_count = db_manager.update_one(
            "news", 
            {"title": "테스트 뉴스"}, 
            {"content": "업데이트된 내용"}
        )
        print(f"업데이트된 문서 수: {updated_count}")
        
        # 문서 개수 조회
        count = db_manager.count_documents("news")
        print(f"전체 뉴스 문서 수: {count}")
        
        # 파일에서 데이터 가져오기 예시
        try:
            # JSON 파일에서 데이터 삽입
            # json_ids = db_manager.insert_from_json("news", "news_data.json")
            # print(f"JSON에서 삽입된 문서 수: {len(json_ids)}")
            
            # CSV 파일에서 데이터 삽입
            # csv_ids = db_manager.insert_from_csv("news", "news_data.csv")
            # print(f"CSV에서 삽입된 문서 수: {len(csv_ids)}")
            
            # JSON 파일에서 upsert
            # upsert_result = db_manager.upsert_from_json("news", "news_data.json", "title")
            # print(f"Upsert 결과: {upsert_result}")
            
            # 컬렉션을 JSON으로 백업
            # backup_count = db_manager.backup_collection_to_json("news", "backup_news.json")
            # print(f"백업된 문서 수: {backup_count}")
            
            print("파일 작업 예시는 주석 처리되어 있습니다.")
            
        except FileNotFoundError as e:
            print(f"파일을 찾을 수 없습니다: {e}")
        except Exception as e:
            print(f"파일 작업 중 오류 발생: {e}")

