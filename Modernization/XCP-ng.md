# 카프카 구성 on XCP-ng
- XCP-ng 기반의 가상화 인프라 환경에서 Kafka Broker와 Consumer를 구축
- 공인 IP 환경에서의 보안 강화 및 네트워크 제약을 극복한 기록.

## Tech Stack
- **Virtualization:** XCP-ng (XenServer) 8.2.1
- **Middleware:** Apache Kafka
- **OS:** Ubuntu Server 24.04 LTS
- **Security:** UFW, SSH Hardening, IP Whitelisting
- **Containerization:** DockerTech Stack

## 인프라 보안 강화 (Hardening)
상용망(공인 IP) 환경에서 해킹 공격을 방어하기 위해 인프라 수준의 보안 설계를 적용. 
- 네트워크 보안: 공유기 DMZ 설정을 해제하여 불필요한 포트 개방을 차단하고, 리눅스 자체 방화벽(UFW)과 IP 화이트리스트를 활용하여 외부 접근을 통제.  
- SSH 보안: PermitRootLogin no 설정을 통해 root 계정의 외부 직접 로그인을 원천 차단하고, 12자리 이상의 복합 비밀번호 정책을 적용.  
- 포트 난독화: 주요 서비스 포트를 표준 포트에서 변경하여 공격 대상 노출을 최소화했습니다.

## ISO 마운트 및 가상화 관리 트러블슈팅
- Problem: SMB/NFS 연동 불가 및 root 로그인 차단으로 인한 파일 전송 제약 발생(공인 IP 환경의 서버와 사설 IP 환경의 PC 간의 네트워크 제약)
- Result:
  - 일반 사용자 계정 생성 후 wheel 그룹에 할당하여 관리자 권한 확보.
  - SFTP를 통한 파일 전송 체계 구축 및 서버 내 전용 로컬 디렉토리(iso/) 소유권 최적화.
  - 젠서버 CLI(xe sr-create)를 활용하여 로컬 폴더를 직접 ISO Repository(SR)로 등록.

## Kafka Broker & Consumer 구축 상세
- 리소스의 효율적 관리와 안정적인 데이터 스트리밍 환경을 구축.
- Broker VM: 4 vCPU, 16GB RAM, 100GB Disk 구성.
- Consumer VM: 6 vCPU, 32GB RAM, 100GB Disk 구성.  
- 운영 최적화:
  - Docker Logging 설정을 통해 로그 보관 주기를 최대 7일로 제한하여 스토리지 관리 효율성 확보.
  - Kafka Log Retention Policy를 7일로 최적화.
  - 리스너(Listener) 설정을 분리하여 Broker-PC 간의 통신 안정성 확보 및 토픽 데이터 전송 확인 완료.
