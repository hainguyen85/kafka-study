# Lab 03b: producer và consumer Spring Boot tối giản

Dùng cho các bước thực hành của Bài 3 (gửi/nhận), Bài 5 (lag, reset offset), Bài 5b (rebalance). Spring Boot 4.1.2, Spring Kafka 4.1.2, kafka-clients 4.2.2 (toạ độ từ `research/0002-spring-kafka.md`). Cần cụm 3 broker ở `labs/03-cluster`.

**Trạng thái:** đã biên dịch và khởi động được (profile `consumer,static`, không có cụm). **Chưa chạy với cụm thật.**

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

## Đọc code

- `application.yml`: bootstrap, producer `acks=all` + idempotence tường minh, consumer `auto-offset-reset: earliest`, `ack-mode: batch`.
- `ProducerRunner.java`: `KafkaTemplate.send` + đọc `CompletableFuture`.
- `ConsumerListener.java`: `@KafkaListener` và bean `ConsumerAwareRebalanceListener` (Boot tự gắn vào container).
- `application-static.yml`: hai cấu hình Boot không có property riêng, đi qua `properties[...]`.
