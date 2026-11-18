# Lab 05: các tình huống "oái oăm" (Spring Kafka)

Mỗi tình huống là một **profile**: cùng một code nền, đổi profile là đổi tình huống. File `application-<profile>.yml` chỉ chứa property khi tình huống đổi được bằng property (`s6`, `s6fix`, `s7`, `s10`). Các profile còn lại cấu hình trong Java (bean `@Profile`, ví dụ error handler + DLT của `s8dlt`, vì Spring Boot không có property cho chúng) hoặc cố ý để mặc định (`s8`); file yaml của chúng chỉ có chú thích chỉ ra cấu hình nằm ở đâu. Sổ tay đầy đủ (triệu chứng, nguyên nhân, cách xử lý, nguồn) ở `reference/0003-tricky-scenarios.html`.

## Chuẩn bị

- Cụm 3 broker từ `labs/03-cluster` đang chạy (`localhost:29092,39092,49092`). Topic tạo tự động bởi `TopicsConfig` (RF=3, `min.insync.replicas=2`).
- Maven phải chạy bằng JDK 17+ (máy bạn đặt `JAVA_HOME` là JDK 8):
  ```bash
  export JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-25.0.4.7-hotspot"
  export JAVA_TOOL_OPTIONS="-Dstdout.encoding=UTF-8"   # để console Windows hiển thị tiếng Việt
  ```
- Chạy một tình huống: `mvn -q spring-boot:run -Dspring-boot.run.profiles=s8`
  Truyền tham số: `-Dspring-boot.run.arguments="--lab.phase=resume"`
  (Chưa kiểm chứng trên máy bạn: nếu `spring-boot.run.*` không nhận, dùng `mvn -q -DskipTests package` rồi `java -jar target/scenarios-0.0.1-SNAPSHOT.jar --spring.profiles.active=s8 --lab.phase=resume`.)
- Dữ liệu của mỗi lần chạy gắn mã riêng (`out/run-id.txt`), group.id cũng riêng, nên chạy lại không bị nhiễu bởi lần trước.

## Các tình huống

| Profile | Tình huống | Cách chạy | Đọc kết quả |
|---|---|---|---|
| `s6` / `s6fix` | Auto-commit: mất và trùng message khi xử lý bất đồng bộ + crash | chạy `--lab.phase=crash`, rồi `--lab.phase=resume` (cùng profile) | In "MẤT" và "TRÙNG". `s6` mất; `s6fix` (ack thủ công) không mất, chỉ trùng |
| `s7` | Xử lý 12 giây > `max.poll.interval.ms` 10 giây: rebalance, trùng | chạy một lần (~100 giây) | Cùng offset xử lý nhiều lần. Thử sửa: `--spring.kafka.consumer.max-poll-records=1 --spring.kafka.consumer.max-poll-interval=30s` |
| `s8` / `s8dlt` | Poison pill: mặc định bị bỏ qua im lặng; có DLT thì còn dấu vết | chạy một lần (~25 giây) mỗi profile | `s8`: POISON không ở đâu cả. `s8dlt`: POISON nằm trong `s8-poison-dlt` |
| `s8retry` | Non-blocking retry `@RetryableTopic` (attempts 3, 1 giây) + `@DltHandler`: thứ tự theo key bị phá | chạy một lần (~30 giây) | Thứ tự `[m0, m1, m2, m4, FLAKY, m6, m7]`; POISON giao 3 lần rồi vào `s8r-orders-dlt`; topic tự tạo `s8r-orders-retry`, `s8r-orders-dlt`. Đã chạy trên cụm 3 broker 2026-11-18 |
| `s9` / `s9dedup` | Trùng dù producer idempotent (ứng dụng gửi lại); chống trùng ở consumer | chạy một lần (~15 giây) mỗi profile | `s9`: điểm gấp đôi. `s9dedup`: điểm đúng |
| `s10` | Transaction consume-transform-produce, `read_committed` vs `read_uncommitted` | chạy một lần (~15 giây) | `read_uncommitted` thấy BOOM 2 lần (1 bản aborted); `read_committed` chỉ thấy 1 lần |
| `s11` | Message 2 MB: lỗi ở từng tầng cấu hình | chạy một lần | 4 dòng kết quả, mỗi dòng một exception hoặc thành công |
| `s12` | Thêm partition làm key đổi partition | `--lab.phase=before`; tăng partition; `--lab.phase=after` | Danh sách key đổi partition |
| `s13` | Compaction: topic "bảng giá mới nhất" | chạy một lần (~110 giây) | Số record theo key trước và sau compaction |

Tăng partition cho `s12` (giữa hai lần chạy):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --alter --topic s12-keys --partitions 6
```

Mô phỏng crash thật cho `s6`: mặc định process tự `halt` (như `kill -9`). Cờ `--lab.crash=close` đóng êm, dùng cho harness.

## Tự kiểm tra không cần Docker (broker Kafka nhúng)

Code được chạy thử với broker KRaft nhúng 1 node (`src/test/java/lab/EmbeddedHarness.java`). **Trạng thái 2026-11-11:** đã chạy và có kết quả cho s7, s8, s8dlt, s9, s9dedup, s11, s12, s6fix; **s6 và s10 đã sửa lỗi sau lần chạy thử nhưng chưa biên dịch/chạy lại; s13 chưa chạy** (harness bị dừng khi máy thiếu bộ nhớ). Chi tiết ở `reference/0003-tricky-scenarios.html`. Cách này kiểm tra được logic consumer, lỗi, transaction, nhưng **không** kiểm tra được độ bền đa broker (đó là việc của lab 04 trên cụm 3 broker).

```bash
mvn -q -DskipTests test-compile
mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -Dstdout.encoding=UTF-8 -cp "target/classes;target/test-classes;$(cat target/cp.txt)" lab.EmbeddedHarness s8 s8dlt s9 s9dedup
```

Kết quả quan sát được ghi ở sổ tay (cột "Trạng thái"). Những tình huống chưa có kết quả quan sát thì ghi rõ là chưa chạy.

## Đọc code theo thứ tự

1. `TopicsConfig.java`: bạn tạo topic, đặt RF và `min.insync.replicas` ở đây (có comment về DLT cần đủ partition).
2. `S8Poison.java`: cấu hình `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` (blocking retry). `S8Retry.java`: `@RetryableTopic` (non-blocking retry).
3. `S6AutoCommit.java`, `S7PollInterval.java`: hai nguồn gây trùng/mất message phía consumer.
4. `S9IdempotentConsumer.java`, `S10Transactions.java`: chống trùng và giới hạn của transaction.
5. `S11TooLarge.java`, `S12PartitionMap.java`, `S13Compaction.java`: thiết kế topic.

## Nguồn

`research/0001` (tình huống T1-T16), `research/0002` (API và mặc định Spring Kafka 4.1.2), docs Apache Kafka 4.4.
