# 업비트 웹소켓을 이용한 비트코인 체결 프로젝트(개인)
## 1일차
1. 업비트 웹소켓사용 사용해서 kafka 토픽으로 전송(현재 비트코인체결관련만)

2. 그라파나,프로메테우스를 사용해서 대시보드 출력확인 및 프로듀서에 데이터가 쌓이고 분당,초당 몇건씩 쌓이는지 확인
   - 토픽에 데이터(메시지)가 쌓이는 걸 보여주는 차트
     - 그라파나 화면에서 "Message in..." 또는 "per second/minute"라고 적힌 차트들이 바로 프로듀서가 카프카 토픽에 데이터를 밀어 넣고 있는(쌓이고 있는) 속도와 양을 보여주는 곳입니다.
     - Message in per second (초당 메시지 유입량)
     - 지금 실시간으로 초당 데이터가 몇 개씩 토픽에 인입되고 있는지 보여줍니다. 아까 형 화면에서 upbit_test 토픽에 1.35라고 떴던 게 바로 이 차트예요! (초당 1.35개씩 쌓이는 중)
     - Message in per minute (분당 메시지 유입량)
     - 최근 1분 동안 데이터가 총 몇 개 쌓였는지 보여줍니다. 아까 85.2개라고 찍혀있던 차트

   - Lag by Consumer Group
     - 컨슈머 래그(카프카 토픽에 쌓인 데이터 개수와 컨슈머가 읽어간 데이터 개수의 "차이(남은 작업량)"를 뜻)
   - Message consume per minute(처리 속도)
     - 컨슈머가 분당 데이터를 몇 개씩 처리하고 있는지(가져가고 있는지) 보여주는 속도계
     
   * 솔직히 아직 잘 모르겠다 사용방법에 대해서 ㅠㅠ 그래서 다 완성하고나서 많이 알아봐야할거 같다
   * 
3. 컨슈머 및 DB(Mysql) 생성 후 데이터 전송시도 그리고 100건씩 호출 후 데이터 저장하는 구조로 만들려고함
  - 오류발생 : DB에 unique Key가 설정되어있어서 같은시각에 체결된 건들이 있어서 중복저장에 대한 오류가 뜸(db에 원본 이벤트 저장소 역할까지 하려고 해서 생기는 문제)
  - 생각한방법:
     - 1. flink를 도입한다는것을 알게 되었고 flink를 사용해 DB에는 최적화시킨 데이터를 지속적으로 (1초마다) 빠르게 업데이트 시켜줘야함, S3원본데이터를 호출 시켜서 지속적으로 S3에 데이터를 올리는 방법을 생각
     - 2. 스노우플레이크를 사용해서 S3에 있는 데이터를 지속적으로 업데이트 할 수 있는 스노우파이프 구축해야함

## 2일차
1. S3에 파일 업로그 기능 구현
   - 고려사항 및 적용사항 :
      - 1. 업로드 성공여부 및 재시도 횟수(3회)
      - 2. 몇건씩 업로드 시킬지(10만건), 10만건이 안되었어도 1시간에 한번씩 업로드하기
      - 3. 성공 및 실패에 대한 로그기록 남기고 슬랙 알림
   - 추후 진행 예정사항 :
      - 하루에 한번씩 배치처리로 spark를 사용해 데이터 마트화 시켜보기(코인별 평균거래가, 총 거래량, 총 거래금액등)
      
2. Flink를 사용해서 집계데이터를 만들어 DB(Mysql)에 저장하기
   - 트러블슈팅 발생
   ### PyFlink 1.19.1 설치 시 Python 버전 및 setuptools 호환성 문제 해결
   #### 문제 상황
   - Flink 1.19.1 환경에서 Kafka Streaming 테스트를 위해 PyFlink를 설치하던 중 Python 의존성 오류가 발생했다.   
   구성 환경:
      OS: Ubuntu 24.04
      Java: OpenJDK 17
      Apache Flink: 1.19.1
      PyFlink: 1.19.1
      Kafka Connector: flink-sql-connector-kafka 3.2.0-1.19
      Python: 3.12.3 (초기 환경)
      1차 문제: Python 3.12 환경에서 PyFlink 설치 실패
      증상
      pip install apache-flink==1.19.1 실행 시 Apache Beam 빌드 과정에서 오류 발생.

   주요 오류:
      ModuleNotFoundError: No module named 'pkg_resources'
   원인: 
      PyFlink 1.19.1은 내부적으로 Apache Beam 2.48.0을 의존한다.
      해당 버전의 Apache Beam은 Python 최신 버전 및 setuptools 최신 버전과 완전한 호환이 되지 않는다.      
   초기 환경:      
      Python 3.12
          |
      PyFlink 1.19.1
          |
      Apache Beam 2.48.0
      
      구조에서 의존성 충돌 발생.

   해결 방법 1: Python 버전 변경      
      PyFlink 1.19.1 권장 환경에 맞춰 Python 3.11 환경으로 변경했다.      
      pyenv 설치      
      기존 Ubuntu 기본 Python 환경을 유지하기 위해 pyenv를 사용했다.      
      curl https://pyenv.run | bash

   환경 변수 설정:
      export PYENV_ROOT="$HOME/.pyenv"
      export PATH="$PYENV_ROOT/bin:$PATH"
      eval "$(pyenv init - bash)"
      eval "$(pyenv virtualenv-init -)"
      Python 3.11 설치
      pyenv install 3.11.9

   PyFlink 전용 가상환경 생성: pyenv virtualenv 3.11.9 flink-env      
   활성화: pyenv activate flink-env      
   확인: python --version      
   결과: Python 3.11.9

   2차 문제: setuptools 버전 호환성 오류
      PyFlink 실행 시:python flink_upbit.py
      오류 발생: ModuleNotFoundError: No module named 'pkg_resources'
      확인: python -c "import pkg_resources"
      결과: ModuleNotFoundError: No module named 'pkg_resources'
      원인
      - 설치된 setuptools 버전: setuptools 83.0.0
        최신 setuptools에서는 기존 pkg_resources 모듈이 제거되었다.
        하지만 Apache Beam 2.48.0은 해당 모듈을 필요로 한다.
      의존성 구조:
      PyFlink 1.19.1
              |
      Apache Beam 2.48.0
              |
      pkg_resources 필요
              |
      setuptools >= 81
              |
      pkg_resources 제거

      해결 방법 2: setuptools 버전 고정      
      setuptools 버전을 하위 버전으로 변경했다.      
      pip install setuptools==70.3.0
      확인:python -c "import pkg_resources; print('ok')"
      결과: ok
      최종 Python 환경
      Python 3.11.9
              |
      pyenv virtualenv
              |
      PyFlink 1.19.1
              |
      Apache Beam 2.48.0
              |
      setuptools 70.3.0
   
      결과: 최종적으로 PyFlink Streaming Job 실행 성공.
      
      Kafka Connector를 추가한 뒤:
      
      Upbit WebSocket
              |
      Kafka Producer
              |
      Kafka Topic(upbit_test)
              |
      Flink SQL Kafka Source
              |
      PyFlink Streaming 처리
              |
      Console Sink
      
      실시간 데이터 처리 파이프라인 검증 완료.
      
      교훈
      데이터 플랫폼 구성 시 라이브러리 버전 호환성이 중요하다.
      최신 버전이 항상 최선이 아니며, 프레임워크가 요구하는 의존성 버전을 확인해야 한다.
      Python 기반 데이터 처리 환경에서는 pyenv 등을 활용해 프로젝트별 Python 버전을 분리하는 것이 안정적이다.

   일단 잘나오는것 확인...
   - <img width="1186" height="218" alt="image" src="https://github.com/user-attachments/assets/35df2e6d-715a-427b-b582-d917118859e6" />

## 3일차
 pyflink로 1분동안 데이터 합계에 대해서 추출하는 방법 완료
 t_env.execute_sql("""
   CREATE TABLE candle_print (
       code STRING,
       window_start TIMESTAMP(3),
       window_end TIMESTAMP(3),
       trade_count BIGINT,
       high_price DOUBLE,
       low_price DOUBLE,
       volume DOUBLE
   ) WITH (
       'connector' = 'print'
   )
   """)

   t_env.execute_sql("""
INSERT INTO candle_print
SELECT
    code,
    window_start,
    window_end,
    COUNT(*) AS trade_count,
    MAX(trade_price) AS high_price,
    MIN(trade_price) AS low_price,
    SUM(trade_volume) AS volume
FROM TABLE(
    TUMBLE(TABLE upbit_ticker, 
    DESCRIPTOR(event_time), 
    INTERVAL '1' MINIUTE)
)
GROUP BY 
    code, 
    window_start, 
    window_end
""")

<img width="670" height="139" alt="image" src="https://github.com/user-attachments/assets/0f80e0a7-8034-4cf5-9a40-6c7fbaf451ca" />

- 트러블슈팅 발생

# Flink 기반 실시간 체결 데이터 처리 트러블슈팅

## 1. 문제 상황

Upbit WebSocket을 통해 수집한 실시간 체결 데이터를 Kafka Topic으로 전달하고, Flink를 이용해 1분봉 데이터를 생성하는 스트리밍 파이프라인을 구축하던 중 Window Aggregation 결과가 정상적으로 출력되지 않는 문제가 발생했다.

구성 환경:

```
Upbit WebSocket
      |
      v
Kafka Topic
      |
      v
Flink SQL
      |
      v
Window Aggregation
```

목표:

* Kafka 체결 이벤트 수신
* Event Time 기반 Window 처리
* 체결 데이터를 시간 단위로 집계
* 추후 MySQL 및 Redis 저장 구조 확장

---

# 2. Kafka Source 연동 확인

먼저 Flink에서 Kafka 데이터를 정상적으로 읽는지 확인했다.

Kafka Source 테이블 생성:

```sql
CREATE TABLE upbit_ticker (
    type STRING,
    code STRING,
    trade_price DOUBLE,
    trade_volume DOUBLE,
    trade_timestamp BIGINT
)
WITH (
    'connector' = 'kafka',
    'topic' = '',
    'properties.bootstrap.servers' = '',
    'format' = 'json'
)
```

테스트 결과:

```
+I ticker KRW-BTC 93695000 0.001
```

정상적으로 Kafka 메시지를 읽는 것을 확인했다.

---

# 3. Event Time 및 Watermark 추가

실시간 데이터 처리에서는 Processing Time이 아닌 실제 체결 발생 시간을 기준으로 처리하기 위해 Event Time을 추가했다.

변경:

```sql
trade_timestamp BIGINT,

event_time AS TO_TIMESTAMP_LTZ(trade_timestamp, 3),

WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

확인 결과:

```
event_time

2026-07-13 04:18:29.265
```

정상적으로 변환됨을 확인했다.

---

# 4. Window Aggregation 결과 미출력 문제

## 문제

아래 SQL 실행 시 결과가 출력되지 않았다.

```sql
SELECT
    code,
    window_start,
    window_end,
    COUNT(*) AS trade_count
FROM TABLE(
    TUMBLE(
        TABLE upbit_ticker,
        DESCRIPTOR(event_time),
        INTERVAL '5' SECOND
    )
)
GROUP BY
    code,
    window_start,
    window_end
```

원인 분석 결과:

Flink Table API의 Streaming Query는 Batch Query처럼 즉시 결과를 반환하지 않는다.

Window가 종료되고 Watermark 기준으로 완료되어야 결과가 생성된다.

즉:

```
데이터 입력
      |
      |
Window 진행
      |
      |
Watermark 도달
      |
      |
결과 출력
```

구조임을 확인했다.

---

# 5. Print Sink 방식으로 변경

단순 SELECT 출력 방식 대신 Sink Table을 생성하여 Streaming 결과를 확인했다.

```sql
CREATE TABLE candle_print (
    code STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    trade_count BIGINT,
    high_price DOUBLE
)
WITH (
    'connector' = 'print'
)
```

이후 INSERT INTO 방식으로 변경:

```sql
INSERT INTO candle_print
SELECT
    code,
    window_start,
    window_end,
    COUNT(*),
    MAX(trade_price)
FROM TABLE(
    TUMBLE(
        TABLE upbit_ticker,
        DESCRIPTOR(event_time),
        INTERVAL '10' SECOND
    )
)
GROUP BY
    code,
    window_start,
    window_end
```

결과:

```
+I KRW-BTC
window_start : 04:18:30
window_end   : 04:18:40
count        : 18
high_price   : 93606000
```

정상 출력 확인.

---

# 6. Sink Schema 불일치 문제

## 오류

```
Column types of query result and sink do not match.

Different number of columns.
```

발생.

원인:

INSERT SELECT 결과 컬럼 개수와 Sink Table 컬럼 개수가 달랐다.

Query 결과:

```
code
window_start
window_end
COUNT(*)
MAX(price)
MAX(price)
MIN(price)
SUM(volume)
```

총 8개 컬럼 생성

Sink:

```
code
window_start
window_end
trade_count
high_price
low_price
volume
```

7개 컬럼 정의

불일치 발생.

---

# 해결

SELECT 결과와 Sink Schema를 동일하게 맞춤.

```sql
SELECT
    code,
    window_start,
    window_end,
    COUNT(*) AS trade_count,
    MAX(trade_price) AS high_price,
    MIN(trade_price) AS low_price,
    SUM(trade_volume) AS volume
```

정상 실행 확인.

---

# 7. 학습 및 개선 방향

이렇게하면 1분동안 최대값,최소값,합계체결량을 알 수 있는데 
1분동안 혹은 특정시간동안 최초의 체결된 데이터, 마지막 체결된 데이터에 대한 값을 구할 수 없어서 이 부분을 어떻게 하면 구현할 수 있는지 해야한다.

현재 구현:

```
Kafka
 |
Flink SQL
 |
Window Aggregation
 |
Print Sink
```
---

# 배운 점

1. Flink Streaming Query는 즉시 결과를 반환하지 않는다.
2. Window 결과는 Event Time과 Watermark 진행에 따라 생성된다.
3. Sink는 단순 출력 방식이 아니라 실제 데이터 저장 대상이다.
4. Flink SQL Aggregate도 내부적으로 State를 사용한다.
5. 직접적인 상태 제어가 필요한 경우 DataStream API와 State 처리가 필요하다.
6. connector' = 'print'와 'connector' = 'kafka'의 차이
   - print는 Flink가 만든 결과 (처리한)데이터를 어디로 보낼지 지정하는 설정
   - kafka는 데이터를 가져오는곳



# 4일차
sql형태의 데이터가 아닌 Dataframe형태로 1분봉데이터 만들기 완성(1분동안의 OHLCV(시가, 고가, 저가, 종가, 거래량))

<img width="1668" height="228" alt="image" src="https://github.com/user-attachments/assets/58aa3bc3-8404-4fbd-aa79-4ad47cbcb7c9" />

## 알게된부분들 
### 1. Keyby
   ```
   keyed_stream = json_stream.key_by(
        lambda x: x.code 
   )
   ```
   이부분이 들어오는 데이터들의 키가되어서 Flink 자체적으로 분산 처리하게 만들어줌

### 2. def on_timer(self, timestamp, ctx) 함수
   이 함수는 def process_element(self, value, ctx) 안에서 내가 원하는 1분봉일때마다 실행되도록 호출함
   ```
   window_end = ((event_time // 60000) +1) * 60000
   ctx.timer_service().register_event_time_timer(window_end)
   ```
   이 부분을 통해서 on_timer를 실행시키도록 만듬
   1분동안 5000번의 process_element가 실행되도 on_timer은 1번만 실행됨
   그리고 반드시 데이터 저장 후에는 clear()를 해줘야함
### 3. OHLCV(시가, 고가, 저가, 종가, 거래량) 구하는 방법
   특히 최종 체결가는 마지막까지 데이터를 업데이트 해야함 마지막 체결량이기때문에
   


## 트러블 슈팅
### 문제:
   - KeyedProcessFunction의 on_timer 실행 후 TypeError: 'NoneType' object is not iterable 발생

### 원인:
   - PyFlink의 on_timer는 결과 데이터를 Stream 형태로 반환해야 하는데, 아무 값도 반환하지 않아 None이 반환됨.

### 해결:
   - yield를 사용하여 generator 형태로 결과를 반환하도록 수정.

### 결과:
   - Timer 기반 집계 데이터를 downstream operator로 전달 가능한 구조 완성.

### 알게된 점 :
   - yield을 사용하는 이유 Flink downstream으로 전달할 결과 데이터를 생성
   - yield를 사용하면 generator(Iterator) 형태가 되어
   - Flink가 timer 결과를 순회하며 처리할 수 있음
   - 반환값이 None이 되는 문제를 방지


문제발생 : env.set_parallelism(1)를 해제하니까 워터마크가 -가 됨 또한 처리량이 1개인 경우는 테스트용이라서 실무에서 사용 가능한 수준으로 만들어야함

# 5일차 
4일차 마지막에 발생한 문제에 대해서 해결하기 위한 작업 


# 5일차 — Idle Subtask로 인한 Watermark 정체 해결 정체 문제 해결
## 트러블 슈팅
4일차 마지막 테스트에서 `env.set_parallelism(1)`을 제거하자 Watermark가 `Long.MIN_VALUE`에 머무르고, Event Time Timer가 실행되지 않는 문제가 발생했다.
처리 병렬도를 1로 고정하는 방식은 단일 파티션 기반 테스트에서는 동작하지만, 실제 운영 환경의 병렬 처리와 확장성을 검증하기에는 적합하지 않았다. 따라서 병렬도를 강제로 1로 제한하지 않고 문제를 해결했다.

### 문제
`env.set_parallelism(1)`을 제거하면 현재 Watermark가 다음 초기값에서 진행되지 않았다.
```text
-9223372036854775808
```
이 값은 Java의 `Long.MIN_VALUE`이며, 아직 유효한 Watermark가 생성되지 않았다는 의미다.
Watermark가 진행되지 않아 다음과 같이 등록한 Event Time Timer도 실행되지 않았다.
```python
ctx.timer_service().register_event_time_timer(window_end)
```

### 원인
Kafka 파티션 수보다 Flink의 병렬도가 크게 설정되면서 일부 Subtask가 데이터를 전달받지 못했다.
예를 들어 Kafka 파티션이 1개이고 Flink 병렬도가 여러 개라면 다음과 같은 상태가 발생할 수 있다.
```text
Subtask 1 → 데이터 수신 및 Watermark 생성
Subtask 2 → 데이터 없음
Subtask 3 → 데이터 없음
Subtask 4 → 데이터 없음
```
Flink의 downstream Watermark는 입력 Subtask들의 Watermark 중 가장 작은 값을 기준으로 결정된다.
데이터를 받지 못한 Subtask는 Watermark가 초기값인 `Long.MIN_VALUE`에 머무르기 때문에 전체 Watermark의 진행을 막았다.
즉, Watermark가 시간 정보를 받지 못한 것이 아니라, **데이터가 없는 유휴 Subtask의 Watermark가 전체 Watermark 진행을 막은 것**이 원인이었다.

### 해결

WatermarkStrategy에 `with_idleness()`를 추가해 일정 시간 동안 데이터를 전달하지 않는 Subtask를 idle 상태로 처리했다.
```python
from pyflink.common import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy

watermark_strategy = (
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TradeTimestampAssigner())
        .with_idleness(Duration.of_seconds(10))
)
```

```python
timed_stream = stream.assign_timestamps_and_watermarks(
    watermark_strategy
)
```

* `with_idleness(Duration.of_seconds(10))`은 특정 Subtask가 10초 동안 데이터를 받지 못하면 해당 Subtask를 idle 상태로 표시한다.
* idle 처리된 Subtask는 전체 Watermark의 최솟값 계산에서 일시적으로 제외된다.
* with_idleness()는 데이터를 버리는 기능이 아니라 유휴 Source를 Watermark 계산에서 제외하는 기능이다.

```text
데이터가 없는 Subtask
        ↓
10초 동안 입력 없음
        ↓
Idle 상태로 전환
        ↓
Watermark 계산에서 제외
        ↓
활성 Subtask 기준으로 Watermark 진행
```

### 결과
`env.set_parallelism(1)`을 설정하지 않아도 데이터가 없는 Subtask가 10초 후 idle 상태로 전환되면서 Watermark가 정상적으로 진행됐다.
Watermark가 Window 종료 시각을 통과하자 Event Time Timer도 정상적으로 실행됐으며, 코인별 OHLCV 집계 결과를 출력할 수 있었다.
초기 10초 동안의 데이터를 제외하는 방식은 아니다. 초기에는 idle timeout이 지나지 않아 Watermark가 잠시 `Long.MIN_VALUE`로 보일 수 있지만, 데이터가 없는 Subtask가 idle 처리된 이후에는 활성 Subtask가 받은 데이터의 Event Time을 기준으로 Watermark가 진행된다.

## 알게 된 점
* `set_parallelism(1)`은 Watermark를 사용하기 위한 필수 설정이 아니다.
* Kafka 파티션 수보다 Flink 병렬도가 크면 데이터를 받지 못하는 Subtask가 생길 수 있다.
* Flink의 downstream Watermark는 여러 입력 Watermark 중 가장 느린 값을 기준으로 진행된다.
* 입력이 없는 Subtask는 전체 Watermark의 진행을 막을 수 있다.
* `with_idleness()`를 사용하면 장시간 데이터가 없는 Subtask를 Watermark 계산에서 제외할 수 있다.
* 병렬도를 1로 고정하는 것은 테스트 단계에서는 간단하지만, 운영 환경에서는 파티션 수·병렬도·Idle 처리 전략을 함께 고려해야 한다.
* Event Time Timer가 실행되지 않을 때는 Timer 코드뿐 아니라 Watermark가 실제로 진행 중인지 먼저 확인해야 한다.
* Operator가 시작될 때는 아직 유효한 Watermark가 전파되지 않았기 때문에 current_watermark()는 초기값인 Long.MIN_VALUE를 반환할 수 있다.
* 첫 Watermark가 생성되어 전파되면 current_watermark()는 정상적인 Event Time 기반 Watermark로 갱신된다.
* 실행 직후의 Long.MIN_VALUE는 정상적인 초기 상태이며, 장시간 지속될 경우에는 Idle Subtask, 병렬도, Timestamp Assigner 등을 점검해야 한다.
* Watermark는 현재 처리 중인 레코드의 Event Time이 아니라, Operator에 전파된 최신 Watermark를 반환한다.

## Late Event 처리 정책
### 1. 문제 정의
네트워크 지연이나 파티션별 처리 속도 차이로 인해 Event Time이 현재 Watermark보다 이전인 Late Event가 발생할 수 있다.
Late Event가 발생했을 때는 해당 이벤트가 속한 1분봉의 마감 여부를 기준으로 처리 방식을 구분한다.

### 2. 1분봉 마감 전
이벤트의 Event Time이 현재 Watermark보다 이전이더라도, 해당 이벤트가 속한 1분 윈도우의 종료 시각이 아직 Watermark를 지나지 않았다면 1분봉은 확정되지 않은 상태다.
따라서 해당 이벤트를 현재 OHLCV State에 반영한다.

```text
event_time < current_watermark
window_end > current_watermark

→ Late Event이지만 1분봉은 아직 마감 전
→ OHLCV State에 반영
```

단, Late Event가 들어오더라도 Open과 Close는 데이터의 도착 순서가 아니라 Event Time을 기준으로 계산해야 한다.

* Open: 가장 이른 Event Time의 체결 가격
* Close: 가장 늦은 Event Time의 체결 가격
* High: 가장 높은 체결 가격
* Low: 가장 낮은 체결 가격
* Volume: 전체 체결량 합계

### 3. 1분봉 마감 후

현재 Watermark가 해당 이벤트의 `window_end`를 이미 지났다면 1분봉 집계와 저장이 완료된 상태로 판단한다.

```text
window_end <= current_watermark

→ 이미 마감된 1분봉에 도착한 Too Late Event
```

실시간 처리가 중요한 Flink 스트리밍 파이프라인에서는 이미 확정된 1분봉 State를 다시 생성하거나 수정하지 않는다. 따라서 해당 이벤트는 현재 집계에서 제외하고 `return` 처리한다.

```python
if window_end <= current_watermark:
    print(
        f"[TOO_LATE_EVENT] "
        f"code={code}, "
        f"event_time={event_time}, "
        f"window_end={window_end}, "
        f"watermark={current_watermark}"
    )
    return
```

### 4. Too Late Event 보정 방법

정확성이 매우 중요한 데이터라면 Too Late Event를 별도의 Kafka 토픽에 저장할 수 있다.

```text
Flink 실시간 집계
        │
        ├─ 정상 및 마감 전 Late Event
        │       → 실시간 1분봉 집계
        │
        └─ Too Late Event
                → late-trade-events 토픽
                → 별도 보정 Consumer 또는 배치 작업
                → 기존 1분봉 재계산 및 업데이트
```

Kafka 토픽 자체가 1분봉을 수정하는 것은 아니다. 토픽에는 보정이 필요한 이벤트를 저장하고, 별도의 Consumer나 배치 작업이 해당 데이터를 읽어 기존 1분봉을 재계산한다.

### 5. 현재 프로젝트 적용 정책

현재 프로젝트에서는 실시간 조회용 1분봉의 즉시 정합성보다 스트리밍 처리 흐름과 안정성을 우선한다.
따라서 1분봉이 마감된 후 도착한 Too Late Event는 실시간 1분봉에 반영하지 않고 제외한다.
대신 S3에 저장된 원본 체결 데이터를 이용해 배치 방식으로 1분봉을 다시 계산하고, 실시간으로 생성된 1분봉과 비교한다. 값이 다르면 배치 계산 결과를 기준으로 기존 1분봉 데이터를 보정한다.

```text
실시간 경로

Upbit → Kafka → Flink → 실시간 1분봉


정합성 보정 경로

S3 원본 체결 데이터
→ 배치 집계
→ 1분봉 재계산
→ 실시간 1분봉과 비교
→ 차이가 있는 데이터만 업데이트
```

### 6. 최종 처리 기준

```text
1. event_time >= current_watermark
   → 정상 이벤트
   → OHLCV State에 반영

2. event_time < current_watermark
   AND window_end > current_watermark
   → 마감 전 Late Event
   → OHLCV State에 반영

3. window_end <= current_watermark
   → 마감 후 Too Late Event
   → 실시간 State에 반영하지 않음
   → 향후 S3 원본 데이터를 이용한 배치 집계로 보정
```

이 구조를 통해 실시간 파이프라인의 처리 지연과 복잡도를 낮추면서도, 원본 데이터를 이용한 배치 보정으로 최종 데이터 정합성을 확보한다.


# 6일차
- 체크포인트관련해서 작업을 진행
- 체크포인트 관련해서 현상태에서 가능한 복구 테스트 같은걸 진행
- 체크포인트에 대해서 확인해보는 로직을 만들었더니 python3로 동작하는것이여서 프로세스가 죽어 job을 더이상 실행시킬수 없음, 즉 재시작할 대상 자체가 없어졌음
- 그래서 도커에 플링크를 설치하는 방식을 취하기로 함
- 화면구축 및 체크포인트 생성까지 확인
<img width="1328" height="219" alt="image" src="https://github.com/user-attachments/assets/c040e948-f342-416b-a29b-df01f6f159e5" />
## 진행 및 한것
Checkpoint 설정 및 UI를 통해 Flink 작동 확인
Checkpoint Interval : 10초
Checkpoint Mode : Exactly Once
State Backend : HashMapStateBackend
Checkpoint 저장소 설정

## 내일 진행예정인것
1. 로그를 확인해야함, UI상 리스토어만 확인하는것이 아님(현재 총체결량 1.5개인데 체크포인트때는 1인경우 1.5개가 로그가 나오면서 리스토어 되었을때 1개로 나와야함)
2. 도커를 stop & start로 검증예정
3. PostgreSQL 구축 후 1분봉 데이터 전송 테스트
 
## 구축 완료 후 진행해봐야하는것
1. 추후에 Savepoint를 만들어보기(플링크 버전업 같은것, 현재 프로세스 동작하면서 자연스럽게 시프트 되도록 테스트)

## 앞으로 진행해야하는것
- PostgreSQL 구축 후 sink를 통해 Excactly once에 대해서 학습하기
- redis는 sink하지 않을것임, redis는 실시간 처리된 데이터를 가져가는것으로 만들 예정인데 백업된 데이터는 redis를 사용하는 의미가 없기 때문
- 즉 Redis를 영구 저장 Sink로 사용하지 않고, 실시간 조회용 최신 상태 저장소로 사용.
  



# 용어 및 개념
### Window의 가장 큰 역할은?
   시간이나 개수 등의 기준으로 데이터를 그룹화(Grouping)해서 집계(Aggregation)할 수 있도록 하는 것.
   Window는 스트림 데이터를 일정 기준(시간, 개수 등)으로 그룹화하여 집계하기 위한 기능. 
   이 프로젝트에서는 1분 Tumbling Window를 사용하여 1분 동안 들어온 체결 데이터를 하나의 그룹으로 묶고, 
   OHLCV를 계산한 후 MySQL과 Redis에 저장했습니다. Window를 사용하지 않으면 데이터가 계속 누적되어 1분봉과 같은 시간 단위 집계를 수행하기 어려움.

### env.set_parallelism(4)는 무슨 의미일까?? 
   env.set_parallelism(4)는 Flink 실행 환경의 기본 병렬도를 4로 설정하는 것
   각 Operator가 기본적으로 4개의 Subtask로 실행. 
   다만 실제 병렬성은 데이터 소스의 특성에도 영향을 받음. 
   예를 들어 Kafka Source는 Topic의 Partition 수보다 더 많은 Subtask가 동시에 데이터를 읽을 수는 없음.
   Partition 1개이면 env.set_parallelism(4)여도 3개는 실행안됨
   
### keyBy(code)를 사용하는 이유는 무엇일까??
   keyBy(code)를 사용하면 동일한 코인(code)의 이벤트가 항상 같은 Subtask로 전달.
   Flink의 State는 Key 단위로 관리되므로 같은 코인의 OHLC와 거래량을 하나의 State에서 일관되게 집계가능.
   만약 keyBy를 사용하지 않으면 동일한 코인의 이벤트가 여러 Subtask로 분산될 수 있어 State가 분리되고 정확한 집계가 어려워짐.

### Watermark
   Watermark는 Event Time 기준으로 늦게 도착하는 데이터를 어느 시점까지 정상 데이터로 인정할 것인지를 결정하는 기준. 
   업비트 체결 데이터에서 실시간성과 데이터 정확성 사이의 트레이드오프를 고려하여 5초를 선택.
   5초 이내에 도착한 지연 데이터는 집계에 포함하고, 그보다 늦은 데이터는 Late Event로 처리하도록 설계.
   
### WatermarkStrategy란? 
   WatermarkStrategy는 Flink가 이벤트의 Event Time을 어떻게 추출하고 
   Watermark를 어떻게 생성할지를 정의.
   Event Time 기반 윈도우나 Timer는 이 Watermark를 기준으로 동작.
   
### Event Time, Processing Time, Watermark
   Event Time은 거래소에서 실제 체결이 발생한 시각이고, Processing Time은 해당 데이터를 Flink가 처리한 시각.
   Watermark는 현재까지 수신한 Event Time을 기준으로 이벤트 시간이 어디까지 진행됐는지를 나타내는 논리적 기준 시각.
   Window 종료와 Event Time Timer 실행 여부를 판단하는 데 사용.
   
### Kafka Partition과 Flink Parallelism은 어떻게 맞출까?
   Source Operator의 병렬도는 일반적으로 Kafka 파티션 수와 맞추는 것이 효율적.
   파티션보다 병렬도가 작으면 하나의 Subtask가 여러 파티션을 읽게 되고, 반대로 병렬도가 크면 
   일부 Subtask는 데이터를 받지 못해 Idle 상태가 됨. Event Time을 사용하는 경우에는 
   Idle Subtask가 Watermark 진행을 막을 수 있으므로 with_idleness() 설정도 함께 고려

### State란 무엇인가요?
   State는 스트림 데이터를 처리하는 동안 Flink가 작업 중간 결과를 저장하는 공간. 
   예를 들어 코인별 누적 거래량이나 최근 가격 같은 값을 메모리에 유지. 
   장애가 발생하면 Checkpoint에 저장된 State를 복구하여 이어서 처리.

### Flink는 왜 State를 메모리에 저장할까?
   Flink는 실시간 스트림 데이터를 처리하는 시스템이기 때문에 빠른 연산이 중요. 
   그래서 코인별 거래량이나 집계값 같은 State를 디스크가 아닌 RAM에 저장하여 매우 빠르게 읽고 쓸 수 있게함.
   다만 RAM은 휘발성이므로 장애가 발생하면 State가 사라질 수 있기 때문에 
   Checkpoint를 이용하여 주기적으로 디스크에 저장하고 복구 가능하게 만듬.

### 왜 굳이 Checkpoint를 만들까?
   State는 메모리에 저장되어 있기 때문에 장애가 발생하면 모두 사라짐.
   그래서 Flink는 일정 주기마다 State를 Checkpoint로 디스크에 저장.
   장애가 발생하면 마지막 Checkpoint에 저장된 State를 복구한 후, 해당 시점부터 스트림 처리를 이어서 수행.

### Checkpoint에는 무엇이 저장되나?
   Checkpoint에는 연산 중인 State와 Source의 처리 위치(예: Kafka Offset)가 함께 저장.
   장애가 발생하면 State와 Offset을 함께 복구하여 중복 처리나 데이터 유실 없이 이어서 처리 가능.
   
### Savepoint?
   Savepoint는 운영자가 직접 생성하는 State 스냅샷.
   새로운 기능을 배포하거나 Flink Job을 업그레이드할 때 기존 State를 유지한 채 
   새로운 Job에서 이어서 처리하기 위해 사용합니다. 따라서 서비스를 중단하지 않고 안정적으로 버전을 변경.

### Checkpoint와 Savepoint의 차이
   Checkpoint는 장애 복구를 위해 시스템이 자동으로 생성하는 State 스냅샷.
   Savepoint는 운영자가 직접 생성하며, 애플리케이션 업그레이드나 새로운 버전 배포,
   다른 클러스터로 Job을 이전할 때 기존 State를 이어받기 위해 사용.

### Exactly Once Sink
   Flink는 Checkpoint가 성공한 순간
   Kafka Offset + State + Sink(MySQL)를 하나의 시점으로 맞춤.
   이걸 일관된 상태(Consistent State)라고 함

# 실험해보고 싶은것
### 1. 카프카 리밸런싱이 발생했을때
### 2. 처리될때마다 레이텐시가 얼마나 되는지 확인해보고 더욱 빠르게 할 수 있는 방법 연구하기(웹소켓 -> 카프카브로커 전달, 카프카브로커 -> 1분봉 플링크, 카프카브로커 -> 컨슈머 등)

# 트레이드 오프에 대해서
## 1. Flink와 Spark Structured Streaming 비교

실시간 체결 데이터를 처리하기 위해 Apache Flink와 Spark Structured Streaming을 알아봤다.

Spark Structured Streaming은 기본적으로 마이크로배치 방식으로 데이터를 일정 주기마다 묶어서 처리한다. 반면 Flink는 이벤트를 레코드 단위로 지속 처리하는 스트리밍 중심 구조를 제공한다.

이번 프로젝트는 업비트 체결 데이터를 수신한 뒤 1분봉과 실시간 체결량을 빠르게 갱신하는 것이 목적이었기 때문에, 처리 지연을 최소화하고 Event Time, Watermark, State, Timer를 세밀하게 제어할 수 있는 Flink를 선택했다.

Spark Structured Streaming
- 마이크로배치 중심
- 배치와 스트리밍을 하나의 처리 모델로 구성하기 쉬움
- Spark 기반 분석 환경과 연계하기 유리
- 마이크로배치 주기에 따라 추가 지연이 발생할 수 있음

Apache Flink
- 레코드 단위 스트리밍 처리
- 낮은 지연시간에 유리
- Event Time, Watermark, State, Timer 제어가 세밀함
- 실시간 처리 구조와 장애 복구 개념을 더 깊게 설계해야 함

따라서 이번 프로젝트에서는 개발 및 운영 복잡도가 높아지는 비용을 감수하고, 낮은 지연시간과 세밀한 스트림 제어가 가능한 Flink를 채택했다.

## 2. Watermark 5초 설정 이유

실시간 체결 데이터는 네트워크 지연, Kafka 전송 지연, 파티션별 처리 속도 차이로 인해 이벤트가 순서대로 도착하지 않을 수 있다.

Watermark 허용 지연시간을 짧게 설정하면 1분봉 결과를 빠르게 확정할 수 있지만, 정상적으로 처리할 수 있는 지연 이벤트까지 Too Late Event로 분류될 가능성이 높아진다.

반대로 허용 시간을 길게 설정하면 Late Event를 더 많이 집계에 포함할 수 있지만, 1분봉 확정 시점과 사용자 화면 반영이 늦어진다.

3초
- 결과 확정이 빠름
- 3초 이상 지연된 이벤트가 누락될 가능성이 높음

5초
- 실시간성과 지연 이벤트 수용 범위의 균형
- 사용자 화면 반영 지연을 허용 가능한 수준으로 유지

10초
- 더 많은 Late Event를 수용할 수 있음
- 1분봉 확정과 화면 반영이 지나치게 늦어질 수 있음

이번 프로젝트에서는 실시간 화면에서 수 초 수준의 지연은 허용할 수 있지만, 10초 이상의 지연은 사용자 체감에 영향을 줄 수 있다고 판단했다.

따라서 초기 설정값으로 5초를 선택했다.

다만 5초는 고정된 정답이 아니라 초기 가설이다. 운영 단계에서는 실제 이벤트 도착 지연 분포를 측정하고, Late Event 비율과 1분봉 확정 지연시간을 비교해 Watermark 값을 조정할 예정이다.

최종 판단 기준
실시간성 우선
→ Watermark를 짧게 설정
→ 결과는 빠르지만 Late Event 증가

정확성 우선
→ Watermark를 길게 설정
→ Late Event는 줄지만 결과 확정이 늦어짐

이번 프로젝트에서는 실시간 조회 성능을 우선하면서도 일시적인 네트워크 지연을 일부 허용하기 위해 5초 Watermark를 적용했다.

실시간 집계에서 제외된 Too Late Event는 즉시 보정하지 않고, 이후 S3에 저장된 원본 데이터를 기준으로 배치 재집계를 수행해 최종 데이터 정합성을 맞추는 구조를 선택했다.

# 추가적인 트레이드오프
## Flink vs Spark Structured Streaming
Flink 선택
이유: 낮은 지연시간, Event Time, Watermark, State 제어

## Watermark 3초 vs 5초 vs 10초
5초 선택
이유: 실시간성과 Late Event 수용의 균형

## 실시간 정확성 vs 응답속도
Too Late Event는 실시간 집계에 반영하지 않음
대신 배치에서 보정

## Redis 캐시 vs DB 조회
Redis 선택
이유: 최근 1분봉은 조회가 많고 빠른 응답이 중요

## Checkpoint 주기
10초 선택
너무 짧으면 I/O 증가
너무 길면 장애 시 재처리 범위 증가

## Kafka Partition 개수
너무 적으면 병렬성 부족
너무 많으면 관리 비용과 리밸런싱 부담 증가

## Exactly Once vs At Least Once
Flink 내부는 Exactly Once
DB는 UPSERT로 멱등성 확보

## State 저장 vs Stateless 처리
OHLCV 계산은 State 사용
메모리 사용량 증가라는 비용 존재

## 1분봉 실시간 생성 vs 배치 생성
실시간 생성으로 사용자 응답성 확보
배치로 최종 정합성 보장

## Kafka 보존기간(Retention)
길게 설정하면 재처리 가능
하지만 디스크 사용량 증가

## Redis를 Sink로 쓰지 않음
Flink 처리 결과의 영구 저장은 PostgreSQL이 담당하도록 설계하고, Redis는 최신 시세와 최근 집계 결과를 빠르게 제공하기 위한 서빙 캐시로 제한.
과거 데이터의 재조회와 복구는 PostgreSQL 및 Kafka를 통해 수행하므로 Redis에는 전체 이력을 저장하지 않을 예정.
