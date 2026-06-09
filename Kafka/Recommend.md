# 카프카 기반 실시간 유사물건추천 시스템
- 사용자 실시간 클릭/조회 이벤트를 Kafka를 통해 비동기 처리하여, '유사 물건' 추천 결과를 서비스 DB에 즉시 반영하는 추천 시스템 구축.
- 백엔드와 분리하여 메인 서비스의 가용성을 높임
- 실 서비스 DB 데이터 기반으로 피크 트래픽을 측정하고 그 수치를 근거로 파티션/워커 수를 결정

## Tech Stack
- Kafka, Python(FastAPI), Consumer Worker, MSSQL

## Architecture
<img width="500" height="800" alt="image" src="https://github.com/roopiring/kmrak_project/blob/main/Kafka/Recommend.PNG" />


## Key Achievements
- **실시간 추천 파이프라인 설계:** 사용자 이벤트 생성 시 즉시 Kafka로 비동기 전송하여 서비스 응답 속도 최적화.
- **비동기 처리 구조 도입:** 추천 로직 처리를 메인 서버(Backend)와 분리된 Consumer Worker로 독립시켜 서비스 부하를 획기적으로 제거.
- **데이터 안정성 확보**
  - recommend-topic 및 retry-topic(DLQ)을 분리 설계하여 장애 시 재처리 로직 구축.
  - 파티션 키(item_id+user_id) 전략을 통해 동일 이벤트 처리 효율 및 순서 보장.
- **성능 및 확장성 지표**
  - 분산 처리 성능: DB에 저장된 데이터 근거하여 피크 트래픽 20TPS 미만 클릭 확인, 파티션 2개, 컨슈머 워커 2개를 구성하여 24 TPS 이상의 이벤트 처리 성능을 확보함.(피크 대비 충분한 처리 여유를 확보)
  - 수평 확장(Scale-out) 설계: Kafka의 파티션 전략과 컨슈머 그룹을 활용하여, 향후 트래픽 증가 시 파티션과 워커 노드를 추가하는 것만으로 처리량을 즉시 확장할 수 있는 구조로 만듬
  - 성능 검증: 분산 환경에서 부하 테스트를 통해 데이터 병목 없는 안정적인 스트리밍 파이프라인 검증 완료.


## Trouble Shooting
1. 메시지 처리 안정성 및 장애 대응
- Problem: 처리 실패 이벤트 발생 시 데이터 유실 위험 및 파이프라인 중단.
- Solution: `retry-topic` 및 `DLQ(Dead Letter Queue)` 설계를 통해 실패 이벤트를 분리하여 추적성 확보.
- Result: 안정적인 재처리 체계를 구축하여 데이터 손실 없는 스트리밍 환경 구현.

2. DB 병목 발생 시 메인 파이프라인 보호
- Problem: DB 응답 지연 시 Kafka 컨슈머가 블로킹되어 메인 파이프라인 전체 중단 위험 존재.
- Solution: 처리 시간 300ms 임계값 모니터링을 통해 지연 감지 시 retry-topic으로 우회 처리, 파티션 2개·워커 2대 병렬 구성으로 단일 처리 병목 제거.
- Result: Kafka 메시지 발행 시각과 DB 저장 완료 시각을 직접 기록하여 측정한 결과, 정상 구간 기준 1초 이내 처리 확인. DB 지연 감지 시 Slack 알림으로 즉시 인지 체계 구축.

