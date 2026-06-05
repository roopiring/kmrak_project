# 데이터 엔드투엔드(End-to-End) ETL 파이프라인
- 경매/공매 물건 정보 및 이기종 데이터를 수집·가공하여 안정적인 데이터 제품(Data Product)으로 제공하는 파이프라인 시스템. 
- 레거시 스크립트 기반의 운영 환경을 Apache Airflow 기반의 분산 아키텍처로 고도화하여 파이프라인의 가용성과 처리 성능을 극대화.

## Tech Stack
- **Orchestration:** Apache Airflow (CeleryExecutor)
- **Data Processing:** Python, Pandas, Pydantic, asyncio
- **Storage & Infra:** PostgreSQL, MS SQL, MinIO, Docker (WSL2)
- **Crawling:** Selenium, BeautifulSoup, requests

## Key Achievements
- **통합 관리:** PyInstaller 기반 파편화된 배치 스크립트를 Airflow DAG로 통합하여 관리 효율성 증대.
- **성능 최적화:** 동기 requests를 asyncio 비동기 처리로 리팩토링하여 데이터 수집 속도 4배 향상.
- **가용성 확보:** 8GB RAM이라는 제한된 온프레미스 자원 환경에서 메모리 누수 제어 및 자동화된 데이터 라이프사이클 관리로 **가동률 99% 달성**.
- **데이터 효율성:** 전건 수집(Full Scan) 대신 **스냅샷 기반 증분 수집(Incremental ETL)**을 도입하여 네트워크 I/O 및 타겟 서버 부하 최소화.

## Pipeline Architecture & Data Tiering
데이터의 신뢰성과 재처리 가능성을 보장하기 위해 3단계 계층 구조를 설계.
1. **Raw Layer (Bronze):** API/크롤링 원천 데이터(JSON/HTML) 백업 및 추적성 확보.
2. **Refined Layer (Silver):** Pydantic 스키마 검증 및 비즈니스 로직 기반 데이터 정제.
3. **Data Mart (Gold):** 서비스 UI 조회 패턴에 최적화된 역정규화 테이블 구성.

## 핵심 엔지니어링 경험 (Troubleshooting)
### 1. DB 병목 해결: Bulk Insert & MERGE 전략
- **Problem:** 건바이건(Row-by-Row) 적재로 인한 과도한 DB 커넥션 오버헤드 및 성능 저하.
- **Solution:** Staging(임시) 테이블에 Bulk Insert 후 운영 테이블에 MERGE 문으로 Upsert 수행하여 DB 부하를 획기적으로 차단.

### 2. 고가용성 파이프라인 구축: 데이터 추적성 및 재처리 환경
- **Problem:** 외부 사이트 개편이나 장애 시 데이터 소실 리스크 및 복구(Backfill)의 어려움.
- **Solution:** 원본 데이터(Raw Data) 백업 정책을 도입하여, 장애 발생 시 원본 소스를 활용한 즉시 재처리(Backfill) 체계 구축.

### 3. 도메인 협업 기반 데이터 모델링
- **Problem:** 사용자 의사결정에 필요한 연계 데이터(학군, 금리 등)의 부재 및 데이터 모델의 경직성.
- **Solution:** 도메인 전문가와 협업하여 요구사항을 구체화하고, UI 조회 패턴을 고려한 화면 중심의 데이터 마트 설계로 서비스 품질 향상.

##  앞으로의 고도화 방향
- 수집 스케일 확장 대비 상용 프록시(Proxy) 및 IP 로테이션 도입.
- 데이터 모델링 효율성을 위한 정기적인 쿼리 튜닝.
