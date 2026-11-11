<!-- Tự sinh từ 0004-producer-durability.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / [Bài 3](0003-build-cluster.md) / Bài 4 · [Bảng tra](../reference/0001-core-concepts.md) · [Sổ tay tình huống](../reference/0003-tricky-scenarios.md)

# Bài 4: "Đã báo thành công" không có nghĩa là "không mất"

Yêu cầu chính của bạn là *không mất message*. Bài này tách yêu cầu đó thành bốn núm vặn: `acks`, `min.insync.replicas`, retry/timeout, idempotence.

> **Liên quan tới mission** Câu hỏi bạn sẽ nghe nhiều nhất trong buổi họp thiết kế: "topic này cấu hình thế nào để không mất?". Sau bài này bạn trả lời được bằng bốn giá trị cụ thể, nói được từng giá trị đổi lấy gì, và biết chỗ nào Spring Kafka có thể làm bạn mất message mà không báo lỗi.

**Thắng lợi của bài này:** với một cấu hình producer + topic bất kỳ, bạn dự đoán đúng "kill leader lúc này thì message nào mất" (bằng mô phỏng ở mục 2), và có sẵn lab chạy trên cụm 3 broker để kiểm chứng (mục 3).

## 1. Kiến thức tối thiểu (có nguồn)

1. **Client 4.x đã mặc định an toàn.** `acks=all`, `enable.idempotence=true`, `retries=2147483647`, `delivery.timeout.ms=120000`, `request.timeout.ms=30000`, `linger.ms=5` (đổi từ 0 ở 4.0), `max.in.flight.requests.per.connection=5`. Muốn thấy mất message, bạn phải *tự hạ* cấu hình. [[Producer configs]](https://kafka.apache.org/44/configuration/producer-configs/) [[Upgrade notes]](https://kafka.apache.org/44/getting-started/upgrade/)
2. **Thứ nguy hiểm là mặc định phía topic/broker.** `min.insync.replicas=1`, `default.replication.factor=1`, `num.partitions=1`. Vì bạn là người tạo topic, hai giá trị đầu là việc của bạn. [[Topic configs]](https://kafka.apache.org/44/configuration/topic-configs/) [[Broker configs]](https://kafka.apache.org/44/configuration/broker-configs/)
3. **"Committed" nghĩa là mọi replica trong ISR đã nhận.** Một message đã committed không mất "as long as there is at least one in sync replica alive". Chữ then chốt là *ISR*, không phải *mọi replica*: ISR có thể co lại chỉ còn leader. [[Design: replication]](https://kafka.apache.org/44/design/design/)
4. **`min.insync.replicas` chỉ có tác dụng với `acks=all`:** ISR nhỏ hơn giá trị này thì broker từ chối ghi bằng `NotEnoughReplicasException`, lỗi loại *retriable* nên producer thử lại tới hết `delivery.timeout.ms`. [[Javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/NotEnoughReplicasException.html)
5. **Biến thể gây trùng:** `NotEnoughReplicasAfterAppendException` nghĩa là ISR thấp được phát hiện *sau khi* bản ghi đã được append; javadoc ghi "Producer retries will cause duplicates". [[Javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/NotEnoughReplicasAfterAppendException.html)
6. **Idempotence có phạm vi hẹp:** chống trùng do retry trong *một session* của producer. Ứng dụng của bạn tự gửi lại sau khi restart thì broker thấy hai bản ghi khác nhau. [[KafkaProducer javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html) (xử lý bằng consumer ở Bài 7)
7. **Quan hệ timeout:** `delivery.timeout.ms` phải ≥ `request.timeout.ms + linger.ms`. Docs khuyên dùng `delivery.timeout.ms` để giới hạn thời gian thử lại thay vì chỉnh `retries`. Đặt tường minh `enable.idempotence=true` cùng `acks≠all`, `retries=0` hoặc `max.in.flight>5` sẽ ném `ConfigException`; nếu không đặt tường minh, idempotence bị tắt âm thầm. [[Producer configs]](https://kafka.apache.org/44/configuration/producer-configs/)
8. **Spring Kafka:** `KafkaTemplate.send` trả `CompletableFuture`; nếu bạn bỏ qua future, lỗi chỉ nằm trong log (mặc định `LoggingProducerListener`). `TopicBuilder` *không có* `minInsyncReplicas()`, dùng `.config("min.insync.replicas","2")`. Topic đã tồn tại thì config không bị sửa trừ khi `spring.kafka.admin.modify-topic-configs=true`. [[Sending messages]](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html) [[research/0002, mục 2.1 và 2.6]](../research/0002-spring-kafka.md)

## 2. Mô phỏng: tự làm mất message

Mô phỏng này theo đúng mô hình ở mục 1 (không phải broker thật). Việc sao chép follower là thủ công để bạn thấy cửa sổ "đã ack nhưng chưa sao chép". Thử lần lượt:

### Thử 1. `acks=1` mất sau khi đã báo thành công

Chọn `acks=1`. Bấm "Gửi message" 3 lần, *không* bấm sao chép, rồi "Kill B1" (leader). Đọc dòng tổng: bao nhiêu message "ACK NHƯNG MẤT"? Bấm "Start B1": B1 quay lại có lấy được chúng không?

Mong đợi: cả 3 message đã ack nhưng mất, vì leader mới (B2) chưa sao chép chúng, và B1 sống lại phải cắt log theo leader mới.

### Thử 2. `acks=all`, minISR=2: cùng cú kill

Làm lại với `acks=all`. Gửi 3 message (chúng ở trạng thái "chờ ack"), kill B1 trước khi sao chép. Message nào mất? Message nào đã được ack?

Mong đợi: không có message nào "ACK NHƯNG MẤT". Ba message chưa từng được ack nên producer thử lại; không có gì đã hứa mà bị phá.

### Thử 3. Cái bẫy "acks=all mà vẫn yếu" (minISR=1)

Đặt `acks=all`, `min.insync.replicas=1`. Kill B2 và B3, bấm "Chờ 30 giây" (ISR co còn {1}). Gửi 2 message: chúng được ack ngay. Kill B1, rồi Start B2. Partition ở trạng thái nào? Bây giờ bật ô `unclean.leader.election.enable` và làm lại từ đầu.

Mong đợi: unclean=false thì partition **offline** (không mất dữ liệu, mất sẵn sàng); unclean=true thì B2 lên làm leader và 2 message đã ack mất. Đây là lý do hỏi platform team giá trị `unclean.leader.election.enable` (xem [bảng managed](../reference/0004-managed-kafka-msk-vs-confluent.md): MSK Standard mặc định true).

### Thử 4. Đánh đổi sẵn sàng lấy bền (minISR=3) và trùng sau append

`acks=all`, minISR=3. Kill B3 rồi gửi ngay: lúc ISR còn {1,2,3} ghi được nhưng chờ ack. Bấm "Chờ 30 giây". Quan sát thông báo, rồi gửi thêm một message.

Mong đợi: sau khi ISR co còn 2 < 3, message chờ ack chuyển thành `NotEnoughReplicasAfterAppend` (retry sẽ trùng), và message mới bị từ chối. Một broker chết là ngừng ghi: vì thế AWS khuyến nghị minISR ≤ RF-1.

## 3. Lab thật trên cụm 3 broker

Mô phỏng cho bạn trực giác; cụm thật cho bạn bằng chứng. Mã ở `labs/04-producer-durability` (Spring Boot 4.1.2 + Spring Kafka 4.1.2); README có lệnh đầy đủ. Ba topic cùng RF=3 chỉ khác `min.insync.replicas` (1, 2, 3); profile `risky` hạ `acks=1` và tắt idempotence; `safe` dùng `acks=all`; `verify` đọc lại toàn bộ topic và đối chiếu với danh sách seq đã được ack, nên "mất" là con số đo được, không phải cảm giác.

**Trạng thái trung thực:** lab 04 đã biên dịch và đã chạy thử nạp cấu hình, nhưng **chưa được chạy với cụm 3 broker** (máy chưa có Docker). Các "mong đợi" dưới đây suy ra từ tài liệu. Mất message do kill leader *không tất định*: có thể phải chạy vài lần hoặc tăng `--lab.per-second`. Ghi lại điều bạn thật sự thấy; nếu khác mong đợi, đó là thông tin quý, hãy báo cho giáo viên.

### Bước 1. Chạy `risky` và kill leader

```
cd labs/04-producer-durability
# describe để biết leader
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic lab-orders-risky
mvn -q spring-boot:run -Dspring-boot.run.profiles=risky     # terminal A
docker kill kafka-2                                          # terminal B, khi thấy "[sender] đã gửi ..."
docker start kafka-2
mvn -q spring-boot:run -Dspring-boot.run.profiles=verify -Dspring-boot.run.arguments="--lab.topic=lab-orders-risky"
```

Mong đợi (chưa kiểm chứng): `ACKED NHƯNG MẤT` > 0 có thể xuất hiện. Nhớ đặt `JAVA_HOME` là JDK 17+ (xem README).

Tôi đã chạy risky + verify và ghi lại số "mất"

### Bước 2. Cùng cú kill với `safe`

Lặp lại với profile `safe` và `--lab.topic=lab-orders-safe`.

Mong đợi: `ACKED NHƯNG MẤT` = 0. Gửi có thể khựng một lúc trong lúc bầu leader mới.

Tôi đã chạy safe + verify

### Bước 3. `strict`: kill một broker

Chạy profile `strict` (topic minISR=3), `docker kill kafka-3` và đọc phần "KẾT QUẢ GỬI" theo từng loại lỗi.

Mong đợi (suy ra): ban đầu ghi treo; sau khoảng 30 giây ISR co lại còn 2 < 3 và ghi bị từ chối `NotEnoughReplicasException` cho tới hết `delivery.timeout.ms`. Không kill 2 broker: cụm combined mode mất đa số controller (xem Bài 3).

Tôi đã chạy strict và ghi lại các loại lỗi

## 4. Usecase oái oăm (mỗi cái một miền)

### (a) Flash sale: "đơn đã nhận" rồi biến mất

Giao diện báo đặt hàng thành công vì producer nhận ack. Sau sự cố một broker, kho đếm thiếu đơn. Nguyên nhân thường gặp: `acks=1`, hoặc `acks=all` nhưng topic `min.insync.replicas=1` và ISR đã co còn leader (Thử 3). Cách xử lý: `acks=all` + RF=3 + minISR=2 *ở cấp topic*, kiểm bằng `--describe`. Từ Kafka 4.1 docs nói giá trị đặt ở cấp broker "sẽ bị gỡ" và ELR đổi nghĩa của config này, nên đừng dựa vào default broker. [[Upgrade notes]](https://kafka.apache.org/44/getting-started/upgrade/)

### (b) Telemetry kho lạnh: cụm sống mà không ghi được

Mất 2/3 broker: ISR < minISR, mọi ghi bị từ chối. Producer treo tới 2 phút (`delivery.timeout.ms` mặc định) rồi lỗi. Đây là *đúng thiết kế*: Kafka đổi sẵn sàng lấy bền. Quyết định theo domain: telemetry nhiệt độ chịu mất vài điểm có thể chọn minISR=1; sổ cái thì không. Bạn sẽ phải nói được lựa chọn này trong họp, kèm hệ quả.

### (c) Spring: lỗi gửi nằm im trong log

Service gọi `template.send(...)` không đọc future. Topic bị từ chối (ISR thấp), ứng dụng vẫn trả 200 cho người dùng. Đây là mất message *phía ứng dụng*, không phải phía Kafka. Xử lý: `whenComplete` hoặc `.get(timeout)` khi cần chắc chắn, và quyết định ứng dụng làm gì khi lỗi (trả lỗi cho người dùng, hoặc ghi outbox, Bài 7). Xem cách `SenderRunner` làm.

### (d) Nếu là ngân hàng

Một dòng cho buổi họp: "Topic giao dịch: RF=3, minISR=2, `acks=all`, idempotence bật, `unclean.leader.election.enable=false`, và producer đọc kết quả gửi". Đó là câu trả lời chuẩn cho "không mất"; phần còn lại (trùng) giải ở consumer.

## 5. Tự nói thành lời

Không nhìn tài liệu, trong 45 giây: *"Để một message đã báo thành công không mất khi một broker chết, mình cần ... vì ...; cái giá phải trả là ..."* Dùng đủ các từ: ISR, `acks=all`, `min.insync.replicas`, leader, retry.

## 6. Kiểm tra

**1. Topic RF=3, min.insync.replicas=1, producer acks=all. ISR đã co còn mình leader, leader chết. Điều nào đúng?**

- **A.** Bản ghi vẫn an toàn nhờ acks bằng all
- **B.** Bản ghi chỉ có một bản sao đáng tin
- **C.** Producer luôn nhận lỗi trước khi ghi
- **D.** Follower tự giữ đủ mọi bản ghi mới

<details><summary>Đáp án</summary>

**B. Bản ghi chỉ có một bản sao đáng tin**

acks=all chỉ đợi các replica TRONG ISR. Khi ISR chỉ còn leader, ack nghĩa là một bản sao; min.insync.replicas=1 không chặn điều đó.

</details>

**2. Cấu hình nào làm broker TỪ CHỐI ghi khi ISR quá nhỏ, thay vì ghi vào một bản sao rồi báo thành công?**

- **A.** acks=1 ở phía producer
- **B.** retries=0 ở phía producer
- **C.** linger.ms=0 ở phía producer
- **D.** min.insync.replicas=2 ở topic cùng acks=all

<details><summary>Đáp án</summary>

**D. min.insync.replicas=2 ở topic cùng acks=all**

min.insync.replicas chỉ có tác dụng cùng acks=all: ISR nhỏ hơn giá trị này thì broker trả NotEnoughReplicasException.

</details>

**3. Producer nhận NotEnoughReplicasAfterAppendException rồi tự thử lại. Rủi ro gì?**

- **A.** Message bị trùng vì đã được append
- **B.** Message bị mất vì leader đã đổi
- **C.** Message bị đảo thứ tự giữa partition
- **D.** Message bị từ chối vĩnh viễn ở broker

<details><summary>Đáp án</summary>

**A. Message bị trùng vì đã được append**

Javadoc: ISR thấp được phát hiện sau khi đã append; thử lại có thể tạo bản trùng. Idempotence trong một session giảm rủi ro này; resend của ứng dụng thì không.

</details>

**4. Service gọi template.send(...) và không đọc kết quả. Broker từ chối ghi. Chuyện gì xảy ra phía ứng dụng?**

- **A.** Spring tự ném exception ngay tại dòng gọi
- **B.** Spring tự gửi lại vô hạn cho tới khi được
- **C.** Lỗi chỉ hiện trong log, ứng dụng vẫn chạy
- **D.** Spring tự chuyển message sang topic dự phòng

<details><summary>Đáp án</summary>

**C. Lỗi chỉ hiện trong log, ứng dụng vẫn chạy**

send trả CompletableFuture; nếu không đọc, mặc định chỉ LoggingProducerListener ghi log. Cần whenComplete hoặc get(timeout).

</details>

**5. Bạn đặt enable.idempotence=true tường minh cùng acks=1. Client 4.x làm gì?**

- **A.** Tự đổi acks thành all rồi chạy tiếp
- **B.** Âm thầm tắt idempotence rồi chạy tiếp
- **C.** Ném ConfigException khi tạo producer
- **D.** Chạy bình thường và chỉ cảnh báo log

<details><summary>Đáp án</summary>

**C. Ném ConfigException khi tạo producer**

Docs: đặt tường minh enable.idempotence=true cùng cấu hình xung đột thì ném ConfigException; chỉ khi không đặt tường minh thì idempotence mới bị tắt âm thầm.

</details>

## Nguồn nên đọc

> **Chính:** mục "Replication" và "Message Delivery Semantics" trong [Design](https://kafka.apache.org/44/design/design/) (khoảng 20 phút), rồi [Producer configs](https://kafka.apache.org/44/configuration/producer-configs/) (chỉ các mục `acks`, `delivery.timeout.ms`, `enable.idempotence`).
>
> **Mã nguồn bài này:** `labs/04-producer-durability` (README có kết quả dự kiến và cách đọc code).
>
> **Tra nhanh khi họp:** [Sổ tay tình huống](../reference/0003-tricky-scenarios.md), nhóm A.

**Bài tiếp theo:** [Bài 5: consumer, offset, commit](0005-consumer-offset-commit.md): phía nhận cũng có thể làm mất message dù producer hoàn hảo.

> **Còn chỗ nào mơ hồ?** Hỏi thẳng giáo viên (agent), ví dụ "vì sao acks=all mà minISR=1 vẫn yếu?", "retry của producer khác retry của ứng dụng chỗ nào?". Dán dòng kết quả quiz và những gì bạn thấy ở lab để bài sau điều chỉnh đúng mức.
