# Lab 04: producer và độ bền ("không mất message")

Spring Boot 4.1.2 + Spring Kafka 4.1.2 (toạ độ và hành vi lấy từ `research/0002-spring-kafka.md`). Chạy với cụm ở `labs/03-cluster`.

**Trạng thái kiểm tra:**
- Đã làm: biên dịch và đóng gói được; ứng dụng nạp đúng cấu hình profile và thử kết nối cả 3 broker; hai bộ cấu hình producer (`risky`, `safe`) được `kafka-clients` chấp nhận.
- **Chưa làm: chạy với cụm thật** (máy chưa có Docker). Phần "Kết quả dự kiến" bên dưới là dự đoán từ tài liệu, không phải kết quả đã quan sát. Khi bạn chạy, so sánh rồi báo lại.

## Chuẩn bị

- Cụm đang chạy (`labs/03-cluster`, xem README ở đó).
- Maven phải chạy bằng **JDK 17 trở lên**. Máy bạn đang đặt `JAVA_HOME` là JDK 8, nên đặt lại cho phiên làm việc:
  ```bash
  export JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-25.0.4.7-hotspot"   # Git Bash
  ```
- Kết nối từ Windows tới `localhost:29092,39092,49092`. Nếu Docker chạy trong WSL2 mà Windows không thấy cổng, chạy lab trong WSL.

## Ba profile gửi và một profile đối chiếu

| Profile | Producer | Topic (RF=3) | Ý nghĩa |
|---|---|---|---|
| `risky` | `acks=1`, `enable.idempotence=false` | `lab-orders-risky` (`min.insync.replicas=1`) | Leader ghi xong là báo thành công |
| `safe` | `acks=all`, `enable.idempotence=true` | `lab-orders-safe` (`min.insync.replicas=2`) | Cấu hình hướng "không mất" |
| `strict` | như `safe` | `lab-orders-strict` (`min.insync.replicas=3`) | Mất 1 broker là ngừng nhận ghi |
| `verify` | (không gửi) | chỉ định bằng `--lab.topic=` | Đọc cả topic, đối chiếu với danh sách đã ack |

Chạy (mỗi lệnh một terminal):

```bash
cd labs/04-producer-durability
mvn -q spring-boot:run -Dspring-boot.run.profiles=risky
mvn -q spring-boot:run -Dspring-boot.run.profiles=verify -Dspring-boot.run.arguments="--lab.topic=lab-orders-risky"
```

Đổi tốc độ và số lượng: `-Dspring-boot.run.arguments="--lab.count=200000 --lab.per-second=10000"`.
Nếu `spring-boot:run` không nhận profile, dùng: `mvn -q -DskipTests package` rồi
`java -jar target/producer-durability-0.0.1-SNAPSHOT.jar --spring.profiles.active=risky` (cách này đã được chạy thử để kiểm tra nạp cấu hình).

## Thí nghiệm 1: mất message sau khi đã báo thành công (`risky`)

1. Xem broker nào là leader: `docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic lab-orders-risky`
2. Chạy profile `risky`. Khi log in `[sender] đã gửi ...`, **kill đột ngột** một leader: `docker kill kafka-2` (SIGKILL, không tắt êm).
3. Chờ gửi xong, bật lại: `docker start kafka-2`, chờ vài giây cho ISR đủ trở lại.
4. Chạy `verify` với `--lab.topic=lab-orders-risky`.

Kết quả dự kiến: có thể thấy `ACKED NHƯNG MẤT` > 0. Lý do theo tài liệu: với `acks=1` chỉ leader xác nhận; leader chết trước khi follower kịp sao chép thì bản ghi mất ([producer configs](https://kafka.apache.org/44/configuration/producer-configs/)). Không tất định: lặp lại vài lần, tăng `--lab.per-second` để cửa sổ chưa sao chép lớn hơn.

## Thí nghiệm 2: cùng việc đó với `safe`

Làm lại các bước trên với profile `safe` và `--lab.topic=lab-orders-safe`.
Kết quả dự kiến: `ACKED NHƯNG MẤT` = 0, vì một ghi chỉ được ack khi mọi replica trong ISR đã nhận ([design](https://kafka.apache.org/44/design/design/)) và ISR tối thiểu là 2. Gửi có thể chậm lại trong lúc bầu leader mới.

## Thí nghiệm 3: đánh đổi sẵn sàng lấy bền (`strict`)

1. Chạy profile `strict`. Kill **một** broker: `docker kill kafka-3`.
2. Quan sát phần "KẾT QUẢ GỬI": số lỗi theo từng loại.

Kết quả dự kiến (suy từ tài liệu, chưa chạy thử): lúc đầu ghi bị treo vì leader còn chờ follower đã chết; sau khi follower bị loại khỏi ISR (`replica.lag.time.max.ms` mặc định 30000), ISR còn 2 < 3 nên ghi bị từ chối bằng `NotEnoughReplicasException`, loại lỗi có thể thử lại cho tới hết `delivery.timeout.ms`. Ghi lại các loại lỗi bạn thật sự thấy.

**Vì sao không kill 2 broker:** cụm lab chạy "combined mode", mỗi node vừa là broker vừa là controller. Kill 2 node là mất đa số của quorum controller (3 controller chỉ chịu được 1 lỗi, [KRaft](https://kafka.apache.org/44/operations/kraft/)), nên cụm không bầu được leader mới và hành vi sẽ khác thí nghiệm này. Đó là một bài học riêng cho bài 3.

## Đọc code theo thứ tự

1. `TopicsConfig.java`: bạn là người tạo topic, quyết định RF và `min.insync.replicas` ở đây.
2. `application-*.yml`: ba bộ cấu hình producer. Boot không có property riêng cho `enable.idempotence` và `delivery.timeout.ms`, nên đi qua `properties[...]`.
3. `SenderRunner.java`: gửi bất đồng bộ và đọc kết quả qua `whenComplete`. Nếu bỏ qua future, lỗi gửi chỉ nằm trong log.
4. `VerifyRunner.java`: cách đo "mất" và "trùng" bằng đối chiếu, không bằng cảm giác.

## Nguồn

`research/0001-curriculum-coverage.md` (tình huống T1, T2), `research/0002-spring-kafka.md` (mục 2.1, 2.6). Chưa kiểm chứng: lệnh `spring-boot.run.profiles` và `spring-boot.run.arguments` (cơ chế chuẩn của Spring Boot Maven plugin nhưng chưa tra docs).
