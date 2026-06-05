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

## Trouble Shooting
#### 1. 대법원/온비드 사이트 개편으로 인한 수집기 마비 대응
- **상황:** 외부 사이트의 갑작스러운 UI/UX 개편으로 기존 파서가 동작하지 않음.
- **문제:** 데이터 수집이 중단되어 서비스 대시보드에 신규 데이터 미반영.
- **해결:** 개편된 웹 아키텍처를 분석하여 DOM 구조 및 API 호출 규격을 즉시 리팩토링.
- **결과:** 서비스 중단 시간을 최소화하고 데이터 정합성을 유지함.

#### 2. 데이터 Row-by-Row 적재로 인한 DB 병목 및 속도 저하
- **상황:** 초기 수집 로직이 데이터를 한 건씩 DB에 커넥션을 맺고 처리함.
- **문제:** 빈번한 커넥션 오버헤드로 인한 DB 부하 발생 및 전체 수집 속도 저하.
- **해결:** Staging 테이블에 Bulk Insert 후 운영 테이블에 MERGE 구문을 사용하여 변경분만 병합(Upsert).
- **결과:** 전체 수집 처리 속도를 획기적으로 향상시키고 DB 안정성 확보.

#### 3. 네트워크 불안정 및 외부기관 IP 차단 이슈
- **상황:** 수집 속도 향상 과정에서 타겟 서버 보안 정책에 의해 IP 차단 발생.
- **문제:** 데이터 수집 로직 중단 및 파이프라인 가용성 저하.
- **해결:** 기관 가이드라인에 맞춰 수집 속도를 조절하고, 차단 시 일정 시간 대기 후 재수집하는 로직 적용.
- **결회:** 현재 안정적 수집 중이며, 향후 상용 프록시 및 IP 로테이션 도입 계획.
