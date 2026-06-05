# 안녕하세요, Python 기반 데이터 파이프라인 및 백엔드(PHP) 시스템을 운영·개선해온 엔지니어입니다.(유료 플랫폼 서비스)

제한된 인프라 환경에서 고가용성 데이터 파이프라인을 설계하고, 비정형 데이터를 가치 있는 데이터 제품(Data Product)으로 정제합니다. 
데이터 엔지니어링 역량을 바탕으로 레거시 시스템의 현대화와 실시간 이벤트 스트리밍 아키텍처를 구현하는 데 강점이 있습니다.

---

### 🛠 Tech Stack
**[Data Engineering]**
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white)

**[Backend & Infrastructure]**
![PHP](https://img.shields.io/badge/PHP-777BB4?style=flat-square&logo=php&logoColor=white)
![MSSQL](https://img.shields.io/badge/MSSQL-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)
![XCP-ng](https://img.shields.io/badge/XCP--ng-0099CC?style=flat-square&logo=xenserver&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

**[Communication & Integration]**
![REST API](https://img.shields.io/badge/REST_API-FF6C37?style=flat-square&logo=openapiinitiative&logoColor=white)
![On-premise](https://img.shields.io/badge/On--premise-666666?style=flat-square&logo=serverfault&logoColor=white)

---

### Projects

| 프로젝트 | 핵심 역할 및 성과 | 주요 기술 |
| :--- | :--- | :--- |
| **[Hybrid Distributed Pipeline]** | 하이브리드(Cloud/On-Premise) 아키텍처 구축을 통한 99% 가동률 달성 | Airflow, Celery, Docker, Redis, MsSQL(MySQL) |
| **[Real-time Recommendation]** | Kafka 기반 이벤트 스트리밍 파이프라인 설계로 비동기 처리 구현 | Kafka, FastAPI |
| **[Real-estate Normalization]** | Rule-based 파서를 통한 비정형 등기부 데이터의 엔티티 정규화 | Python, MERGE |
| **[Legacy Modernization]** | DB속도개선 및 3-Tier 아키텍처 분리 | PHP, MSSQL |
| **[External API Integration]** | 대외 기관(KB부동산, 공인중개사협회) 연동 API 개발 | REST API |
---

### 핵심 기술적 역량 
**[Data Engineering]**
- **Pipeline Orchestration:** PyInstaller 레거시 스크립트를 Airflow DAG로 마이그레이션하여 **데이터 파이프라인의 가시성 및 중앙 집중식 관리** 체계 확립.
- **Incremental Processing:** 전건 수집(Full Scan)에서 **스냅샷 기반 증분 수집(Incremental ETL)** 모델로 전환하여 네트워크 I/O 및 타겟 서버 부하 감소.
- **Entity Normalization:** 비정형 법률 데이터를 정규화하여 분석 가능한 **Gold Layer 데이터 모델링** 및 서비스 신뢰성 확보.
- **Performance Optimization:** Python `asyncio` 기반 비동기 수집 로직 설계 및 `Bulk Insert/MERGE` 전략으로 수집 처리 속도 **4배 향상**.
- **Infra Engineering:** 온프레미스 8GB RAM 제약 환경에서 Celery 분산 처리 구조를 구축하여 **물리적 리소스 한계를 극복한 확장성(Scalability)** 확보.

**[Backend & System Architecture]**
- **Legacy Modernization:** DB/Web/File 3-Tier 분리를 통한 시스템 병목 해결.
- **Reliability Engineering:** Recovery Pending 장애 복구 및 SQL Agent 자동화 스케줄링을 통한 시스템 안정성 확보.
- **External Collaboration:** KB부동산, 공인중개사협회 등 대외 기관 연동을 위한 전용 API 설계 및 운영.
---

### 부동산 경·공매 데이터 플랫폼 경매락(kmrak.com)
- ** 주요 비즈니스: 대법원 경매 및 온비드 공매 데이터 기반 유료 부동산 정보 플랫폼 운영
- ** 주요 서비스: 부동산 권리분석 시스템, 낙찰가 통계 및 물건 상세 정보 제공
- ** 도메인 특성: 경•공매데이터와 부동산 데이터 간의 데이터 결합, 비정형 데이터 파싱 및 실시간 대외 기관 API 연동 환경

### 📬 Contact
- 📧 Email: dino7238@naver.com
