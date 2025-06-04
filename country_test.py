# 1) 설치해야 할 패키지
# -------------------
# pip install transformers torch

from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline

# 2) 토크나이저와 모델 로드
# -------------------------
model_name = "dslim/bert-base-NER"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForTokenClassification.from_pretrained(model_name)

# 3) pipeline 객체 생성
# ----------------------
# aggregation_strategy="simple"을 주면 같은 엔티티 단위로 묶어서 결과를 반환합니다.
nlp_ner = pipeline(
    task="ner",
    model=model,
    tokenizer=tokenizer,
    aggregation_strategy="simple",
)

# 4) 테스트 문장 정의
# -------------------
text = """Ukraine says it hit Crimea bridge with underwater explosives

Ukraine uses underwater explosive on Crimean bridge

However, later in the day, local authorities warned the bridge was temporarily closed again.

Russian media initially said the bridge was briefly closed to traffic but that it had reopened by 10:00 local time (08:00 GMT).

The "first explosive device" was detonated "without any civilian casualties", the SBU added. The information shared by the SBU could not be immediately verified.

The SBU said it had "mined the supports" of the bridge with explosives equivalent to 1,100kg of TNT, which "severely damaged" the bottom level of the supports.

Ukraine's Security Service (SBU) said it targeted the Crimea bridge with underwater explosives on Tuesday following an operation lasting "several months".

Unconfirmed reports on social media said more explosions had occurred around the structure.

The official Telegram channel sharing operational updates about the bridge said: "We ask those on the bridge and in the inspection zone to remain calm and follow the instructions of the transport security officers."

Russia has not yet commented on Tuesday morning's attack but Russian military bloggers speculated that an underwater drone, rather than explosive, had hit a protective barrier.

The SBU said its director, Lieutenant General Vasyl Malyuk, personally supervised the operation and coordinated its planning.

In a Telegram post, it quoted Malyuk as saying Ukraine had hit the Crimea bridge in 2022 and 2023 and was therefore "continuing this tradition under water."

"No illegal Russian facilities have a place on the territory of our state," Malyuk said.

"Therefore, the Crimean Bridge is an absolutely legitimate target, especially considering that the enemy used it as a logistical artery to supply its troops."

The bridge - also known as Kerch Bridge - was built by Russia after it invaded and annexed the Crimean Peninsula in 2014. It was inaugurated by Russian President Vladimir Putin to great fanfare in 2018.

In Ukraine, the bridge is a hated symbol of Russian occupation. Moscow guards it carefully - which makes any attack on it, whether with underwater drones or explosives, a remarkable achievement.

Tuesday's strike on the bridge comes barely 48 hours after Ukraine hit several targets across Russia as part of an operation dubbed 'Spider Web'.

Kyiv said it smuggled more than 100 drones into Russia and managed to get them delivered near air bases by unsuspecting lorry drivers.

The drones then flew out of the lorries and attacked Russia's prized strategic bombers in locations across the country."""

# 5) NER 추론
# ------------
entities = nlp_ner(text)

# 1) LOC만 필터링
loc_entities = [ent for ent in entities if ent["entity_group"] == "LOC"]

# 2) 중복 단어 제거 + 잡음 필터링: 점수 >= 0.90, 길이 >= 2
seen = set()
filtered = []
for ent in loc_entities:
    word = ent["word"]
    score = ent["score"]
    # 소문자로 치환해서 중복 제거(대소문자 구분 없이)
    key = word.lower()
    if key in seen:
        continue
    # score가 낮거나(word 길이 < 2) 잡음으로 보이는 경우 건너뜀
    if score < 0.90 or len(word) < 2:
        continue
    seen.add(key)
    filtered.append(ent)

# 최종 결과 출력
print("── 최종 LOC 엔티티 ──")
for ent in filtered:
    print(f"{ent['word']} (score: {ent['score']:.4f})")