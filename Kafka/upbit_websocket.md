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

다음 단계:

```
Kafka
 |
Flink DataStream API
 |
KeyBy(code)
 |
ValueState
 |
OHLC 생성
 |
JDBC Sink
 |
MySQL
 |
Redis
```

State 기반 처리를 추가하여:

* Open Price
* Close Price
* Candle 상태 관리
* Timer 기반 Window 종료 처리

를 구현할 예정이다.

---

# 배운 점

1. Flink Streaming Query는 즉시 결과를 반환하지 않는다.
2. Window 결과는 Event Time과 Watermark 진행에 따라 생성된다.
3. Sink는 단순 출력 방식이 아니라 실제 데이터 저장 대상이다.
4. Flink SQL Aggregate도 내부적으로 State를 사용한다.
5. 직접적인 상태 제어가 필요한 경우 DataStream API와 State 처리가 필요하다.


그리고  'connector' = 'print'와 'connector' = 'kafka'의 차이에 대해서도 알게 되었다 print는 Flink가 만든 결과 (처리한)데이터를 어디로 보낼지 지정하는 설정
kafka는 데이터를 가져오는곳
이렇게하면 1분동안 최대값,최소값,합계체결량을 알 수 있는데 
1분동안 혹은 특정시간동안 최초의 체결된 데이터, 마지막 체결된 데이터에 대한 값을 구할 수 없어서 이 부분을 어떻게 하면 구할지 공부해봐야한다.
 
