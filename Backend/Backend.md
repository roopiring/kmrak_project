# 백엔드 서비스 & 시스템
- PHP(Pure) 및 MSSQL 2008 R2라는 레거시 환경을 안정화
- 대외 기관(KB부동산, 공인중개사협회)과의 API 연동을 주도하여 플랫폼의 비즈니스 가치를 극대화한 프로젝트

## Tech Stack
- **Language:** PHP (Pure)
- **Database:** MSSQL 2008 R2
- **Infrastructure:** On-premise (Web/DB/File Separation)
- **Integration:** REST API, Toss Payments, Kakao Maps API

## Key Achievements
- 레거시 운영 안정화: 인수인계 공백 상황에서 Pure PHP 및 MSSQL 기반의 온프레미스 시스템을 주도적으로 파악하여 운영 안정성 달성.
- 대외 협업 플랫폼 구축: KB부동산 앱 및 한국공인중개사협회와 연동되는 전용 API를 구축하여 데이터 기반 의사결정 지원 및 실시간 중개 현황 시스템 마련.
- 시스템 아키텍처 고도화: 단일 서버의 리소스 경합을 해결하기 위해 Web/DB/File 서버를 물리적으로 분리하는 3-Tier 아키텍처를 구현하여 시스템 응답 속도 및 확장성 확보.
- 도메인 특화 알고리즘 최적화: 복잡한 비정형 법률 데이터를 구조화하는 '권리분석 알고리즘'을 고도화하여 서비스 핵심 경쟁력 강화.

## Trouble Shooting 
1. 단일 서버 병목 현상 해소 (3-Tier 아키텍처)
- Problem: Web/DB/File 서버가 단일 노드에서 공존하여 리소스(CPU/RAM/Disk I/O) 경합 및 병목 발생.
- Solution: 물리적 서버 환경을 Web 전용, DB 전용, File 전용으로 완벽히 분리하여 독립적인 인프라 환경 구축.
- Result: 리소스 간섭 제거를 통해 시스템 안정성을 비약적으로 향상시키고 각 서버별 스케일 아웃 기반 마련.

2. MSSQL 'Recovery Pending' 장애 긴급 복구
- Problem: DB 용량 부족 및 트랜잭션 로그 비대화로 인한 DB 복구 보류 장애 발생.
- Solution: 긴급 용량 확보 후 트랜잭션 로그 축소(Shrink) 작업 수행, 매일 자동 정리를 위한 SQL 에이전트 작업(Maintenance Job) 스케줄링.
- Result: 서비스 가용성 회복 및 사후 방지 체계 구축.

3. 글로벌 트래픽 공격 대응
- Problem: 특정 시점 외국 IP의 대규모 유입으로 인한 웹 서버 자원 고갈.
- Solution: IDC 인프라의 Geo-IP 기능을 활용하여 국가별 IP 차단 정책 적용.
- Result: 악성 트래픽 원천 차단으로 가용 자원 확보 및 서비스 안정화.

## 도메인 전문성 및 협업 지향성
- 도메인 내재화: 부동산 전문가와의 밀착 협업을 통해 권리관계 및 입찰 프로세스 등 도메인 지식을 완전히 내재화하여 정확한 데이터 모델링 및 개발 수행.  
- 비즈니스 임팩트: 단순히 기능을 구현하는 것을 넘어, 대외 협업 프로젝트(KB부동산, 중개사협회)에서 요구사항 분석부터 API 설계까지 전체 과정을 리드.
