# 레거시 인프라의 현대화 및 하이브리드 클라우드 인프라 구축 프로젝트
- 외부 공인 IP 환경에서의 보안 강화와 온프레미스-클라우드 간의 하이브리드 아키텍처를 설계하여 시스템 안정성을 확보.
- 제한된 리소스와 공인 IP 노출이라는 기술적 제약을 보안 강화와 분산 아키텍처로 극복하여 99% 이상의 높은 가동률을 달성.
  
## Tech Stack
- **nfrastructure:** AWS EC2, Ubuntu Server 24.04 LTS
- **Backend & DB:** PHP, MSSQL 2008 R2
- **Orchestration & Data:** Apache Airflow (CeleryExecutor), Docker, Docker Compose
- **Security:** SSH Hardening, UFW Firewall, Port Obfuscation, IP Whitelisting

## Key Achievements
- **보안 강화:** 외부 공인 IP 환경에서의 해킹 공격을 방어하기 위해 SSH root 차단, 포트 난독화, UFW 방화벽 및 IP 화이트리스트 접근 제어 도입.  
- **하이브리드 아키텍처:** 마스터 노드(AWS EC2)와 워커 노드(온프레미스)를 분리하여 시스템 가용성을 확보하고, 필요 시 수평적으로 확장 가능한 구조 설계.  
- **레거시 호환성 확보:** 최신 Docker 환경과 구형 MSSQL(TLS) 간의 통신 오류를 보안 설정 튜닝으로 해결하여 레거시 데이터의 안전한 현대화 파이프라인 구축.  
- **인프라 자동화:** Git 기반 CI/CD를 구축하여 5분 단위의 코드 동기화 및 자동 배포 체계를 수립, 운영 공수를 획기적으로 절감.

## Trouble Shooting
1. 인프라 보안
- Problem: XCP-ng 노출 시 해킹 봇의 무차별 공격 위험.
- Solution: DMZ 즉시 해제, SSH root 외부 로그인 원천 차단, 강력한 패스워드 정책 적용.
- Result: 외부 공격 벡터를 원천 차단하여 인프라 보안성 확보.

2. 물리적 장애 대응을 위한 하이브리드 마이그레이션
- Problem: 온프레미스 마스터 PC 물리적 다운 시 관제 마비 및 원격 제어 불가.
- Solution: 마스터 노드를 AWS EC2로 이전하여 24/7 가동 환경을 구축하고, 온프레미스 자원을 Celery Worker로 재편성.
- Result: 안정적인 관제 환경과 함께 필요 시 즉각 확장이 가능한 수평적 확장 구조 완성.

3. 레거시 DB(MSSQL 2008 R2) 호환성 문제 해결
- Problem: 최신 Docker 컨테이너의 보안 정책으로 인해 구형 MSSQL TLS 연결 거부.
- Solution: Dockerfile 내 openssl.cnf 수정을 통해 보안 레벨(SECLEVEL) 하향 조정.
- Result: 안정적으로 레거시 데이터를 추출하여 데이터 파이프라인으로 적재 성공.

## System Architecture (Hybrid Cloud-OnPremise)
- 데이터 파이프라인과 관제 시스템은 마스터-워커 분리 구조를 통해 연산 자원을 유연하게 확장할 수 있도록 설계.
- 하이브리드 시스템 구성  :
  1. Master Node (AWS EC2): Web, Scheduler, PostgreSQL(Metadata), Redis(Broker), MinIO(원격 로그)).
  2. Worker Node (On-Premise): Celery Worker들을 통해 물리적 위치에 구애받지 않고 유연한 수평적 연산 수행.  
