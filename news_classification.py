# 1) 설치해야 할 패키지
# -------------------
# pip install transformers torch

from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline


class NewsLocationExtractor:
    """뉴스 텍스트에서 위치(LOC) 엔티티를 추출하는 클래스"""
    
    def __init__(self, model_name="dslim/bert-base-NER"):
        """
        NER 모델을 초기화합니다.
        
        Args:
            model_name (str): 사용할 모델 이름 (기본값: "dslim/bert-base-NER")
        """
        self.model_name = model_name
        self._load_model()
    
    def _load_model(self):
        """토크나이저와 모델을 로드하고 pipeline을 생성합니다."""
        print(f"모델 로딩 중: {self.model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForTokenClassification.from_pretrained(self.model_name)
        
        # pipeline 객체 생성
        # aggregation_strategy="simple"을 주면 같은 엔티티 단위로 묶어서 결과를 반환합니다.
        self.nlp_ner = pipeline(
            task="ner",
            model=self.model,
            tokenizer=self.tokenizer,
            aggregation_strategy="simple",
        )
        print("모델 로딩 완료!")
    
    def extract_locations(self, text, min_score=0.90, min_length=2):
        """
        텍스트에서 위치 엔티티를 추출합니다.
        
        Args:
            text (str): 분석할 텍스트
            min_score (float): 최소 신뢰도 점수 (기본값: 0.90)
            min_length (int): 최소 단어 길이 (기본값: 2)
            
        Returns:
            list: 필터링된 위치 엔티티 리스트
        """
        # NER 추론
        entities = self.nlp_ner(text)
        
        # LOC만 필터링
        loc_entities = [ent for ent in entities if ent["entity_group"] == "LOC"]
        
        # 중복 단어 제거 + 잡음 필터링
        seen = set()
        filtered = []
        for ent in loc_entities:
            word = ent["word"]
            score = ent["score"]
            # 소문자로 치환해서 중복 제거(대소문자 구분 없이)
            key = word.lower()
            if key in seen:
                continue
            # score가 낮거나 word 길이가 짧은 경우 건너뜀
            if score < min_score or len(word) < min_length:
                continue
            seen.add(key)
            filtered.append(ent)
        
        return filtered
    
    def print_locations(self, locations):
        """추출된 위치 엔티티를 출력합니다."""
        print("── 최종 LOC 엔티티 ──")
        for ent in locations:
            print(f"{ent['word']} (score: {ent['score']:.4f})")