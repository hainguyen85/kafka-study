# Lab 03b: producer và consumer Spring Boot tối giản

Dùng cho các bước thực hành của Bài 3 (gửi/nhận), Bài 5 (lag, reset offset), Bài 5b (rebalance), Bài 8b (đo thông lượng, lag) và Bài 8c (đọc lại theo thời gian). Spring Boot 4.1.2, Spring Kafka 4.1.2, kafka-clients 4.2.2 (toạ độ từ `research/0002-spring-kafka.md`). Cần cụm 3 broker ở `labs/03-cluster`.

**Trạng thái (2026-11-17):** đã chạy với cụm 3 broker của `labs/03-cluster` (Windows gọi vào cụm trong WSL): profile `producer`, `consumer` (lag), `perf` và `replay` cho kết quả ghi trong Bài 8b, 8c. Profile `consumer,static` mới chỉ khởi động được, chưa chạy kịch bản rebalance trên cụm.

## Build

```bash
export JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-25.0.4.7-hotspot"   # JDK 17+; máy đặt JAVA_HOME là JDK 8
mvn -q -DskipTests package
J="java -jar target/spring-clients-0.0.1-SNAPSHOT.jar"
```

## Chạy

| Lệnh | Làm gì |
|---|---|
| `$J --spring.profiles.active=producer --lab.count=9` | Gửi 9 message xoay vòng 3 key (`--lab.keys=...` để đổi), in partition/offset broker trả về |
| `$J --spring.profiles.active=consumer --lab.instance=c1 --lab.group=g1` | Đọc topic, in partition/offset/key của từng record và mỗi lần rebalance |
| thêm `--lab.process-ms=2000` | Giả lập xử lý chậm |
| `--spring.profiles.active=consumer,static` | Thêm static membership (`group.instance.id` = `lab.instance`) và `CooperativeStickyAssignor` |
| `--spring.kafka.listener.ack-mode=record` | Đổi thời điểm commit (mặc định `batch`; xem Bài 5) |
| `$J --spring.profiles.active=perf --lab.topic=lab-perf` | Bài 8b: gửi 200.000 message 200 byte, in message/giây, MB/giây, độ trễ gửi p50/p99/max. Đổi cấu hình bằng `--spring.kafka.producer.properties[linger.ms]=0`, `--spring.kafka.producer.compression-type=lz4`, `--spring.kafka.producer.batch-size=131072`; `--lab.count`, `--lab.size` |
| `$J --spring.profiles.active=replay --lab.group=g-replay-1 --lab.replay-minutes=2` | Bài 8c: lần đầu được giao partition thì `seekToTimestamp` về N phút trước, in timestamp theo giờ Việt Nam |

## Đọc code

- `application.yml`: bootstrap, producer `acks=all` + idempotence tường minh, consumer `auto-offset-reset: earliest`, `ack-mode: batch`.
- `ProducerRunner.java`: `KafkaTemplate.send` + đọc `CompletableFuture`.
- `ConsumerListener.java`: `@KafkaListener` và bean `ConsumerAwareRebalanceListener` (Boot tự gắn vào container).
- `application-static.yml`: hai cấu hình Boot không có property riêng, đi qua `properties[...]`.
- `PerfRunner.java`: đo độ trễ từ lúc gọi `send()` tới lúc ack; khi quá tải, phần lớn là thời gian xếp hàng trong `buffer.memory`.
- `ReplayListener.java`: `AbstractConsumerSeekAware` + `seekToTimestamp`, chỉ seek một lần vì `onPartitionsAssigned` chạy lại sau mỗi rebalance.
- `TopicsConfig.java`: tạo `lab-orders` (3 partition) và `lab-perf` (6 partition), RF=3, `min.insync.replicas=2`.
