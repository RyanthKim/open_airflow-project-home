# Airflow ETL Pipeline

실 운영 환경에서 검증된 Airflow 기반 ETL 파이프라인 포트폴리오.
Medallion Architecture(Bronze/Silver/Gold)로 데이터를 단계적으로 정제하며,
Airflow 2.10+ Task SDK, Asset 기반 Cross-DAG 의존성, 커스텀 Sensor 등
프로덕션 수준의 패턴을 적용한 프로젝트입니다.

---

## 프로젝트 개요

이 프로젝트는 SaaS 환경의 구독/결제/이벤트 데이터를 수집하고,
단계적으로 정제하여 분석 가능한 형태로 제공하는 데이터 파이프라인입니다.

**핵심 특징:**
- Medallion Architecture (Bronze -> Silver -> Gold) 기반 데이터 정제
- Airflow Task SDK (`@task`, TaskGroup, Asset) 활용
- 커스텀 Sensor를 통한 네트워크/VPN 사전 검증
- 배치 처리 + 메모리 관리로 제한된 리소스에서 안정 운영
- Slack 콜백을 통한 파이프라인 모니터링

---

## 아키텍처

### 전체 데이터 흐름

```
  Data Sources              Bronze               Silver                Gold             Output
 +--------------+     +----------------+    +-----------------+    +-------------+    +----------+
 | Google Sheets|---->| temp_event_logs|    | plan_info       |    |             |    |Dashboard |
 | Redshift raw |---->| temp_sub_      |--->| subscription_   |--->| Gold 집계   |--->|Slack     |
 | API Events   |---->|   periods      |    |   periods       |    | 테이블     |    |Report    |
 +--------------+     +----------------+    +-----------------+    +-------------+    +----------+
                       DROP TABLE IF EXISTS   DELETE + INSERT        Asset trigger
                       CREATE TABLE AS        (idempotent)          Cross-DAG 연계
```

### Medallion Architecture 상세

```
 +---------------------------------------------------------------------+
 |                        Airflow Scheduler                             |
 +---------------------------------------------------------------------+
         |                    |                     |
         v                    v                     v
 +---------------+    +---------------+     +---------------+
 |    Bronze     |    |    Silver     |     |     Gold      |
 |  (Staging)    |    |  (Cleansed)  |     |  (Analytics)  |
 |---------------|    |---------------|     |---------------|
 | - raw 데이터   |    | - 정규화      |     | - 집계 테이블  |
 |   UNION/적재  |--->| - JOIN/보강   |---->| - KPI 산출    |
 | - 임시 테이블   |    | - 품질 검증   |     | - 리포트 뷰   |
 | - 3일 윈도우   |    | - 90일 범위   |     | - Asset 발행  |
 +---------------+    +---------------+     +---------------+
   CTAS (덮어쓰기)      DELETE+INSERT          Asset trigger
                        (멱등성 보장)         (Cross-DAG)
```

**Bronze (Staging)**
- 원천 데이터를 그대로 적재하는 단계
- `DROP TABLE IF EXISTS` + `CREATE TABLE AS`로 매 실행마다 재생성
- 여러 소스(user_events, system_events, api_events)를 UNION하여 통합
- 윈도우 함수로 순서 인덱스 부여 (idx, idx_inverse)

**Silver (Cleansed)**
- Bronze 데이터를 정규화하고 비즈니스 로직을 적용하는 단계
- `DELETE + INSERT` 패턴으로 멱등성 보장 (트랜잭션 내 실행)
- Recursive CTE로 구독 상태 체인 추적
- Plan 정보, 가격, 기능 플래그를 JOIN하여 비정규화 차원 테이블 구성

**Gold (Analytics)**
- 최종 분석/리포팅용 집계 테이블
- Asset(Dataset) trigger를 통해 하위 DAG 자동 실행
- 대시보드/Slack 리포트의 데이터 소스

### Cross-DAG 의존성 (Asset/Dataset)

```
 DAG: bronze_pipeline           DAG: silver_pipeline          DAG: gold_reporting
 +--------------------+         +--------------------+        +------------------+
 | temp_event_logs    |         | plan_info          |        | daily_kpi        |
 | temp_sub_periods   |--Asset->| subscription_      |--Asset>| slack_report     |
 |                    |  trigger|   periods           | trigger|                  |
 +--------------------+         +--------------------+        +------------------+
```

Bronze DAG가 완료되면 Asset을 발행하고,
Silver DAG가 해당 Asset을 감지하여 자동 트리거됩니다.
이 패턴으로 DAG 간 결합도를 낮추면서도 실행 순서를 보장합니다.

---

## 프로젝트 구조

```
.
├── airflow/
│   ├── dags/
│   │   ├── libs/                     # 공통 라이브러리
│   │   │   ├── alerts.py             # Slack 성공/실패 콜백
│   │   │   ├── sensors.py            # RedshiftConnectionSensor (VPN 체크)
│   │   │   └── logging_config.py     # Airflow/로컬 환경 자동 감지 로거
│   │   └── sql/
│   │       ├── bronze/               # Bronze 레이어 SQL
│   │       │   ├── temp_event_logs.sql
│   │       │   ├── temp_subscription_periods.sql
│   │       │   └── drop_temp_tables.sql
│   │       ├── silver/               # Silver 레이어 SQL
│   │       │   ├── plan_info.sql
│   │       │   └── subscription_periods.sql
│   │       └── gold/                 # Gold 레이어 SQL
│   ├── config/                       # Airflow 설정 오버라이드
│   ├── logs/                         # 태스크 실행 로그 (gitignored)
│   └── plugins/                      # 커스텀 Operator/Hook
├── tests/
│   └── unit/                         # 단위 테스트
├── docker-compose.yaml               # 멀티 컨테이너 Airflow 환경
├── requirements.txt                  # 프로덕션 의존성
├── requirements-dev.txt              # 개발/테스트 의존성
├── .env.example                      # 환경 변수 템플릿
└── .gitignore
```

---

## 주요 패턴

### 1. Airflow Task SDK (@task, TaskGroup, Asset)

Airflow 2.10+의 Task SDK를 활용하여 Pythonic하게 DAG를 구성합니다.

```python
from airflow.sdk import Asset, DAG, task, TaskGroup

# Asset 정의: Cross-DAG 의존성의 핵심
bronze_complete = Asset("bronze_complete")

@task
def extract_events() -> dict:
    """Bronze: 이벤트 로그 적재"""
    ...

@task(outlets=[bronze_complete])
def load_subscription_periods(event_summary: dict) -> None:
    """Bronze: 구독 기간 적재 + Asset 발행"""
    ...

with DAG(...) as dag:
    with TaskGroup("bronze") as bronze_group:
        events = extract_events()
        load_subscription_periods(events)
```

**Task SDK 적용 포인트:**
- `@task` 데코레이터로 XCom 직렬화 자동 처리
- TaskGroup으로 Bronze/Silver/Gold 단계 시각적 그룹핑
- Asset(구 Dataset)으로 DAG 간 데이터 의존성 선언

### 2. Medallion Architecture (Bronze/Silver/Gold)

SQL 파일을 레이어별로 분리하여 관리합니다.

```
sql/
├── bronze/     CTAS (CREATE TABLE AS) — 매번 재생성, 원천 그대로
├── silver/     DELETE+INSERT in transaction — 멱등성 보장, 비즈니스 로직
└── gold/       집계/리포팅 뷰 — Asset trigger로 하위 DAG 연계
```

**Bronze 레이어 SQL 패턴:**
```sql
-- CTAS: 임시 테이블을 매번 새로 생성
DROP TABLE IF EXISTS analytics.temp_event_logs;
CREATE TABLE analytics.temp_event_logs
DISTKEY (user_id)
SORTKEY (event_ts, event_source)
AS
WITH union_events AS (
    SELECT ... FROM raw.user_events
    UNION ALL
    SELECT ... FROM raw.system_events
    UNION ALL
    SELECT ... FROM raw.api_events
)
SELECT ... FROM indexed_events;
```

**Silver 레이어 SQL 패턴:**
```sql
-- 트랜잭션 내 DELETE+INSERT: 멱등성 보장
BEGIN;
DELETE FROM analytics.subscription_periods
WHERE period_start >= DATEADD(day, -90, CURRENT_DATE);

INSERT INTO analytics.subscription_periods (...)
WITH RECURSIVE period_chain (...) AS (
    -- Anchor: 첫 구독 이벤트
    SELECT ... WHERE event_seq = 1
    UNION ALL
    -- Recursive: 다음 기간으로 이동
    SELECT ... FROM temp_subscription_periods t
    INNER JOIN period_chain pc ON t.period_start = pc.period_end
)
SELECT ... FROM enriched_periods;
COMMIT;
```

### 3. 커스텀 Sensor (VPN/네트워크 체크)

Airflow worker에서 Redshift로의 네트워크 접근이 VPN에 의존하는 환경에서,
파이프라인 시작 전에 연결 가능 여부를 확인합니다.

```python
class RedshiftConnectionSensor(BaseSensorOperator):
    """VPN/네트워크 상태를 확인하고, 연결 가능할 때까지 대기"""

    def poke(self, context: Context) -> bool:
        try:
            db = get_db_manager()
            db.execute_query("SELECT 1")
            return True
        except Exception:
            return False  # 다음 poke까지 대기
```

이 Sensor를 DAG 최상단에 배치하면, VPN이 끊어진 상태에서
즉시 실패하는 대신 연결이 복구될 때까지 자동으로 재시도합니다.

### 4. DataValidator (데이터 품질 검증)

Silver/Gold 레이어 적재 후 데이터 품질을 검증합니다.

```python
@task
def validate_subscription_periods() -> None:
    """Silver 적재 후 데이터 정합성 검증"""
    checks = [
        ("NULL 체크", "SELECT COUNT(*) FROM analytics.subscription_periods WHERE user_id IS NULL"),
        ("기간 정합성", "SELECT COUNT(*) FROM analytics.subscription_periods WHERE period_start > period_end"),
        ("중복 체크", "SELECT user_id, period_seq, COUNT(*) FROM ... GROUP BY 1,2 HAVING COUNT(*) > 1"),
    ]
    for name, query in checks:
        result = db.execute_query(query)
        if result[0][0] > 0:
            raise ValueError(f"데이터 품질 검증 실패: {name}")
```

### 5. 텍스트 마이닝 (형태소 분석 + 카테고리 분류)

한국어 텍스트를 형태소 분석하여 카테고리를 자동 분류합니다.
kiwipiepy를 활용한 경량 형태소 분석기로, JVM 의존성 없이 동작합니다.

```python
from kiwipiepy import Kiwi

kiwi = Kiwi()

def classify_text(title: str) -> str:
    """텍스트에서 형태소를 추출하고 사전 기반으로 카테고리 분류"""
    tokens = kiwi.tokenize(title)
    nouns = [t.form for t in tokens if t.tag.startswith("NN")]
    # 카테고리 사전과 매칭하여 분류
    return match_category(nouns)
```

### 6. 메모리 관리 (Batch Processing, gc, malloc_trim)

제한된 컨테이너 메모리(scheduler 5GB, 나머지 2GB)에서
대량 데이터를 안정적으로 처리하기 위한 패턴입니다.

```python
import ctypes
import gc

def process_in_batches(query: str, batch_size: int = 5000):
    """배치 단위로 데이터를 처리하여 메모리 피크 억제"""
    offset = 0
    while True:
        batch = db.execute_query(f"{query} LIMIT {batch_size} OFFSET {offset}")
        if not batch:
            break
        process(batch)
        del batch
        gc.collect()
        offset += batch_size

    # glibc에 해제된 메모리를 OS에 반환 요청
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except OSError:
        pass
```

**docker-compose.yaml에서의 메모리 설정:**
```yaml
environment:
  MALLOC_ARENA_MAX: "2"          # glibc arena 수 제한 -> RSS 절감
deploy:
  resources:
    limits:
      memory: 5G                 # scheduler OOM 방지
    reservations:
      memory: 3G
```

### 7. Slack 알림 (성공/실패 콜백)

DAG 레벨 콜백으로 파이프라인 상태를 Slack에 실시간 알림합니다.
Sensor timeout은 네트워크 장애로 인한 예상 가능한 실패이므로
경고(warning) 수준으로 분류합니다.

```python
def on_failure_callback(context):
    task_id = context["task_instance"].task_id
    is_sensor = task_id.startswith("check_") or "sensor" in task_id.lower()
    emoji = ":warning:" if is_sensor else ":x:"
    send_slack({"text": f"{emoji} Task *{task_id}* failed ..."})

with DAG(
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    ...
):
```

---

## 기술 스택

| 영역 | 기술 | 버전/비고 |
|------|------|----------|
| Orchestration | Apache Airflow | 2.10.5 (Task SDK, Asset, DAG Processor) |
| Language | Python | 3.11 |
| Database | Amazon Redshift | Redshift-specific SQL (DISTKEY, SORTKEY, CTAS) |
| Metadata DB | PostgreSQL | 15 (Airflow metadata store) |
| Container | Docker Compose | Multi-service (scheduler, api-server, dag-processor) |
| Data Processing | pandas, numpy | 1.5.3 / 1.26.4 |
| NLP | kiwipiepy | 0.18.0 (한국어 형태소 분석) |
| Google API | gspread, google-auth | Google Sheets 연동 |
| AWS | boto3, redshift-connector | Redshift 직접 연결 |
| Notification | Slack Webhook | 성공/실패 콜백 |
| Testing | pytest, ruff | 단위 테스트 + linting |

---

## 로컬 실행

### 사전 요구사항

- Docker Desktop (Docker Compose v2)
- 4GB 이상 여유 메모리

### 설정 및 실행

```bash
# 1. 저장소 클론
git clone https://github.com/<your-username>/airflow-project-home.git
cd airflow-project-home

# 2. 환경 변수 설정
cp .env.example .env
# .env 파일을 열어 실제 값으로 채우기

# 3. Airflow 서비스 시작
docker compose up -d

# 4. 초기화 완료 확인
docker compose logs airflow-init

# 5. 서비스 상태 확인
docker compose ps
```

### 접속 정보

| 서비스 | URL | 기본 계정 |
|--------|-----|----------|
| Airflow UI | http://localhost:8080 | admin / admin |

### 주요 명령어

```bash
# 로그 모니터링
docker compose logs -f airflow-scheduler

# DAG 목록 확인
docker compose exec airflow-scheduler airflow dags list

# 서비스 중지
docker compose down

# 볼륨 포함 완전 삭제
docker compose down -v
```

---

## 테스트

```bash
# 의존성 설치
pip install -r requirements-dev.txt

# 전체 테스트 실행
pytest tests/ -v

# 커버리지 포함
pytest tests/ --cov=airflow/dags --cov-report=term-missing

# Lint
ruff check airflow/
```

---

## 라이선스

MIT License
