# 경매락 프로젝트 (Data Engineer & Backend)
안녕하세요, **부동산 경·공매 데이터 플랫폼 경매락(kmrak.com)**에서 Python 기반 데이터 파이프라인 및 백엔드(PHP) 시스템을 운영·개선해온 엔지니어입니다. 

- **비즈니스 도메인:** 대법원 경매 및 온비드 공매 데이터 기반 유료 부동산 정보 플랫폼 운영.
- **핵심 인프라 및 아키텍처:** 실서비스 환경에서 Apache Airflow, Kafka, Docker 기반의 데이터 수집·처리 파이프라인을 운영, 레거시 PHP/MSSQL 시스템 현대화와 장애 대응을 주도.
- **기술적 강점:** 단순 기능 개발을 넘어 운영 환경의 리소스 병목과 시스템 장애를 주도적으로 해결하는 데 강점이 있습니다. `asyncio` 비동기 처리, `Bulk Insert/MERGE` 기반 DB 적재 최적화 등을 통해 가동률 99%를 달성하고 서비스 안정화
- **역량 확장:** 온프레미스 환경에서 XCP-ng 기반 가상화 인프라 구축, Airflow 분산 워커 운영, 중앙 로그 수집 환경 구성, Kafka 기반 실시간 추천 파이프라을 경험했으며, 현재 LLM 자동화로 엔지니어링 스펙트럼을 확장진행중.

---

### Tech Stack

**[Data Engineering]**
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&omega;Color=white)

**[Backend & Infrastructure]**
![PHP](https://img.shields.io/badge/PHP-777BB4?style=flat-square&logo=php&logoColor=white)
![MSSQL](https://img.shields.io/badge/MSSQL-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white)
![XCP-ng](https://img.shields.io/badge/XCP--ng-0099CC?style=flat-square&logo=xenserver&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

**[Communication & Integration]**
![REST API](https://img.shields.io/badge/REST_API-FF6C37?style=flat-square&logo=openapiinitiative&logoColor=white)
![On-premise](https://img.shields.io/badge/On--premise-666666?style=flat-square&logo=serverfault&logoColor=white)

---

## Project Architecture Map
전체 인프라 시스템은 역할과 목적에 따라 물리적 인프라 분리(3-Tier) 및 클라우드-온프레미스 하이브리드 노드 분산 처리를 채택하여 자원 제약을 극복하고 고가용성을 확보.

1. **데이터 파이프라인:** AWS Airflow Master와 On-Premise Worker 간 Celery 분산 구조로 비정형 법률 데이터 수집/검증.
2. **스트리밍 서비스:** FastAPI(Producer)와 분산 컨슈머 워커 간 Kafka 장애 격리(Retry/DLQ) 실시간 추천 모델.
3. **백엔드 인프라:** 레거시 단일 서버 병목 해소를 위한 Web-Data-File 물리 분리 3-Tier 안정화 구조.

---

## Sub-Project Navigation (상세 아키텍처 및 소스코드 바로가기)
각 서브 디렉토리의 `README.md`에서 상세 아키텍처 다이어그램, 운영 코드 명세, 트러블슈팅 이력을 실시간 확인하실 수 있습니다.

### 1. [Data Pipeline](./PIPELINE/DATA_PIPELINE.md)
- **핵심 역할:** 파편화된 PyInstaller 레거시 스크립트를 Airflow Celery 분산 아키텍처로 고도화하여 중앙 집중식 가시성 확보.
- **주요 성과:** API 원천 건수 대조 자동화, Pydantic v2 기반 결함 데이터 FTP 원격 격리, 임시 테이블 빌드 후 MERGE 기반 대용량 벌크 적재 최적화.

### 2. [Kafka Recommendation Streaming](./Kafka/Recommend.md)
- **핵심 역할:** 유저 실시간 클릭/조회 액션 이벤트 처리 및 실시간 추천 서빙 스트리밍 파이프라인 구현.
- **주요 성과:** 파티션-컨슈머 1:1 매핑 병렬 처리 극대화, 메인 큐 블로킹 방지를 위한 Retry-Topic(2회 재시도) 및 DLQ(Dead Letter Queue) 기반 고도화 예외 처리 라인 구축.

### 💻 3. [Backend Service](./Backend/Backend.md)
- **핵심 역할:** Pure PHP 기반 대외 연동 전용 API 설계 및 온프레미스 인프라 3-Tier 물리 분리 아키텍처 개선.
- **주요 성과:** 단일 서버 내 Web-DB-File 자원 경합 및 병목 제거, MSSQL Recovery Pending 긴급 장애 복구 및 SQL Agent 자동화 스케줄링 안착, 해외 악성 트래픽 대응을 위한 Geo-IP 차단 정책 적용.

---

## 🎯 핵심 기술적 역량 요약 (Technical Competencies)

### [Data Engineering & Processing]
- **Orchestration & Scale-out:** 하이브리드(Cloud/On-Premise) 환경에서 Airflow Celery 구조를 운용하여 인프라 비용을 최적화하고 확장성을 강화했습니다.
- **Incremental Processing:** 전건 수집(Full Scan) 대신 **스냅샷 기반 증분 수집(Incremental ETL)** 모델을 도입하여 네트워크 I/O 및 대상 원천 서버의 부하를 최소화했습니다.
- **Entity Normalization:** 비정형 법률 및 등기부 데이터를 Rule-based 파서와 Validator를 통해 정규화하여 분석 가능한 데이터 모델링(Gold Layer)을 완수하고 서비스 데이터 제품(Data Product)의 신뢰성을 확보했습니다.

### [Backend & Reliability Engineering]
- **System Architecture Separation:** 레거시 단일 서버의 하드웨어 리소스 병목을 타파하기 위해 인프라를 Web, Data, File 레이어로 물리 분리하여 가용성을 극대화했습니다.
- **External Collaboration:** 사수/팀장이 없는 제약 환경 속에서도 **KB부동산 및 한국공인중개사협회(MOU)** 파트너십 요구사항을 주도적으로 분석하고, 회원 동기화 및 매물 통계 전용 API 설계 및 개발 전 과정을 리드했습니다.

---

### ✉️ Contact & Channels
- **Email:** dino7238@naver.com
- **Service Link:** [경매락 서비스 바로가기](https://kmrak.com)
