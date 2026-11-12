<!-- Tự sinh từ 0005-consumer-offset-commit.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / [Bài 4](0004-producer-durability.md) / Bài 5 · [Bảng tra](../reference/0001-core-concepts.md) · [Sổ tay tình huống](../reference/0003-tricky-scenarios.md)

# Bài 5: Producer hoàn hảo vẫn có thể bị consumer làm mất message

Offset commit là lời hứa "tôi đã xử lý tới đây". Lời hứa đó có thể được đưa ra sớm hơn sự thật.

> **Liên quan tới mission** "Không mất" là một chuỗi: producer ghi bền (Bài 4), broker giữ, consumer xử lý rồi mới ghi nhận. Bài này lo mắt xích cuối. Câu bạn sẽ gặp: "service này restart thì có mất hoặc xử lý lại message không?". Bạn trả lời được bằng chế độ commit và vị trí của lệnh commit trong code.

**Thắng lợi của bài này:** với một consumer cho trước, bạn chỉ ra được khi nào commit xảy ra, "crash ở đây thì mất hay trùng", và chọn cấu hình Spring Kafka đảm bảo at-least-once. Bạn cũng gỡ rối được một group bị rebalance liên tục bằng CLI.

## 1. Kiến thức tối thiểu (có nguồn)

1. **Vị trí và commit.** Vị trí của consumer ở mỗi partition là một số nguyên: offset của message *kế tiếp* sẽ đọc. Commit lưu số đó (vào topic nội bộ `__consumer_offsets`); sau restart consumer chạy tiếp từ offset đã commit. [[Design: consumer position]](https://kafka.apache.org/44/design/design/) [[KafkaConsumer javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
2. **Mặc định của Kafka client (4.4):** `enable.auto.commit=true`, `auto.commit.interval.ms=5000`, `auto.offset.reset=latest`, `max.poll.records=500`, `max.poll.interval.ms=300000`, `session.timeout.ms=45000`, `heartbeat.interval.ms=3000`, `group.protocol=classic`, `isolation.level=read_uncommitted`. [[Consumer configs]](https://kafka.apache.org/44/configuration/consumer-configs/)
3. **Auto-commit chạy bên trong `poll()`.** Nếu bạn chưa xử lý xong mọi record của lần `poll` trước khi gọi `poll` kế tiếp (hoặc `close`), "committed offset can move ahead of consumption": offset đi trước việc xử lý, crash là mất. [[KafkaConsumer javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
4. **Spring Kafka đổi mặc định:** container có `AckMode=BATCH` và đặt `enable.auto.commit=false` "unless explicitly set". Tức là với Boot không cấu hình gì, commit do container làm sau khi xử lý xong lô, an toàn hơn Kafka thuần. Muốn kiểm soát từng record: `ack-mode=manual_immediate` và nhận `Acknowledgment`. [[Message listener container]](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html)
5. **Bẫy đã gặp khi chạy thật:** khai báo tham số `Acknowledgment` ở chế độ *không* manual làm Spring ném `IllegalStateException` (không phải đưa `null` như research ban đầu ghi); đã đính chính ở [research/0002](../research/0002-spring-kafka.md).
6. **Sống chết của consumer có hai đồng hồ.** Heartbeat/session (`session.timeout.ms`) và *khoảng cách giữa hai lần `poll`* (`max.poll.interval.ms`). Quá hạn thứ hai thì consumer rời group, rebalance xảy ra, `commitSync` ném `CommitFailedException`. [[KafkaConsumer javadoc]](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) [[Consumer configs]](https://kafka.apache.org/44/configuration/consumer-configs/)
7. **Giảm rebalance khi deploy:** static membership (`group.instance.id` khác nhau mỗi instance: restart không rebalance, đổi lại partition được giữ tới hết session timeout); assignor `CooperativeStickyAssignor`; hoặc giao thức mới `group.protocol=consumer` (KIP-848, GA từ Kafka 4.0; khi đó `session.timeout.ms` và `heartbeat.interval.ms` phía client không áp dụng). [[Design]](https://kafka.apache.org/44/design/design/) [[KIP-848]](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
8. **Hết hạn offset.** Offset đã commit bị xoá sau `offsets.retention.minutes` (mặc định 10080 phút, 7 ngày) kể từ khi group rỗng; khi không còn offset hợp lệ, `auto.offset.reset` quyết định (mặc định `latest`). Docs cảnh báo thêm partition khi dùng `latest` "may cause silent message loss". [[Broker configs]](https://kafka.apache.org/44/configuration/broker-configs/) [[Consumer configs]](https://kafka.apache.org/44/configuration/consumer-configs/)
9. **CLI cứu hộ:** `kafka-consumer-groups.sh --describe` hiện CURRENT-OFFSET, LOG-END-OFFSET, LAG; `--reset-offsets` (`--to-earliest`, `--to-datetime`, `--shift-by`, `--by-duration`, `--to-offset`) mặc định chỉ xem trước, và group phải không có member đang chạy. [[Basic operations]](https://kafka.apache.org/44/operations/basic-kafka-operations/)

## 2. Mô phỏng: chỗ nào của dòng thời gian thì crash gây mất?

Partition có 9 record, mỗi `poll()` trả 3 (như `max.poll.records=3`). Mô phỏng theo mô hình ở mục 1 (không phải client thật).

### Thử 1. Chế độ A (auto-commit, xử lý trong thread consumer)

`poll()`, xử lý 2 trong 3 record, bấm "Crash". Rồi chạy lại: `poll()` và xử lý hết. Đọc dòng tổng.

Mong đợi: không mất, có *trùng* (2 record đầu xử lý hai lần). Đó là at-least-once.

### Thử 2. Chế độ B (auto-commit, giao việc cho worker rồi return)

`poll()` hai lần (lần hai commit lô đầu), mỗi lần giao cho worker; xử lý đúng 1 record; bấm "Crash". Offset nào bị đánh dấu MẤT?

Mong đợi: các offset của lô đầu mà worker chưa làm xong: đã commit nhưng chưa từng được xử lý. Đây là C1 trong sổ tay: đúng kiểu "email không bao giờ gửi".

### Thử 3. Chế độ C (ack thủ công sau xử lý)

Làm lại mọi kịch bản crash ở chế độ C. Có lúc nào mất không? Trùng thì sao?

Mong đợi: không bao giờ mất; trùng nhiều nhất một record (record đang xử lý lúc crash). Muốn hết trùng phải chống trùng ở phía xử lý (Bài 7).

## 3. Lab thật: `labs/05-scenarios`

Cùng code nền, mỗi tình huống là một profile. Dữ liệu và `group.id` của mỗi lần chạy gắn mã riêng nên chạy lại không bị nhiễu. Chạy trên cụm 3 broker của Bài 3 (Maven phải dùng JDK 17+, xem README).

**Trạng thái trung thực:** `s7` và `s6fix` đã chạy trên broker nhúng 1 node: s7 cho thấy cả 3 offset đầu được xử lý **3 lần**; s6fix cho MẤT 0, TRÙNG 0 (crash giả lập đóng êm). `s6` (profile gây mất) đã sửa lỗi sau lần chạy thử nhưng **chưa chạy lại**, nên kết quả dưới đây là dự đoán. Cả hai chưa chạy với cụm 3 broker. Lệnh `spring-boot.run.*` chưa kiểm chứng trên máy bạn; README có cách dự phòng bằng `java -jar`.

### Bước 1. S6: auto-commit làm mất message (dự đoán trước khi chạy)

Mở `S6AutoCommit.java`, đọc listener `Unsafe` và `Fixed`. Viết ra giấy: "crash lúc này thì seq nào mất?". Rồi chạy hai pha cùng profile:

```
cd labs/05-scenarios
mvn -q spring-boot:run -Dspring-boot.run.profiles=s6                                   # pha crash: process tự halt như kill -9
mvn -q spring-boot:run -Dspring-boot.run.profiles=s6 -Dspring-boot.run.arguments="--lab.phase=resume"   # pha chạy tiếp, in báo cáo
```

Mong đợi (dự đoán, chưa chạy lại): báo cáo in `MẤT` > 0 vì offset auto-commit trước khi thread pool xử lý xong. Rồi chạy `s6fix` với cùng hai pha: kỳ vọng MẤT = 0, và có thể TRÙNG > 0 nếu crash cứng.

Tôi đã dự đoán, chạy s6 và s6fix, ghi lại MẤT/TRÙNG

### Bước 2. S7: xử lý chậm hơn `max.poll.interval.ms`

```
mvn -q spring-boot:run -Dspring-boot.run.profiles=s7     # ~100 giây
```

Profile `s7` đặt `max-poll-records=3`, `max-poll-interval=10s`, mỗi record xử lý 12 giây. Quan sát log rebalance và số lần mỗi offset được xử lý. Rồi sửa: `--spring.kafka.consumer.max-poll-records=1 --spring.kafka.consumer.max-poll-interval=30s` và so sánh.

Đã quan sát trên broker nhúng: cả 3 offset đầu xử lý 3 lần. Với cụm của bạn ghi lại số bạn thấy.

Tôi đã chạy s7 và thử cấu hình sửa

### Bước 3. CLI: nhìn lag và reset offset

Dùng topic `lab-orders` từ Bài 3: chạy một console consumer có group, gửi vài message, rồi:

```
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group g1
# dừng consumer, rồi xem trước và áp dụng reset
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group g1 --topic lab-orders --reset-offsets --to-earliest --dry-run
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group g1 --topic lab-orders --reset-offsets --to-earliest --execute
```

Mong đợi: `--describe` hiện CURRENT-OFFSET, LOG-END-OFFSET, LAG; `--reset-offsets` báo lỗi nếu group còn member đang chạy (theo docs). Ghi lại bạn thấy gì: đây là thao tác bạn sẽ cần khi "message cũ phải xử lý lại".

Tôi đã xem lag và reset offset bằng CLI

## 4. Usecase oái oăm (mỗi cái một miền)

### (a) Gửi email/SMS thông báo: vừa mất vừa trùng

Listener đẩy việc gửi email sang thread pool rồi return để "nhanh". Auto-commit đẩy offset đi; crash giữa chừng làm vài email không bao giờ đi (mất), và nếu bạn commit cuối lô thì vài cái khác đi hai lần. Xử lý: ack sau khi xong (Thử 3), nhưng nếu việc gửi cần song song thì phải *đợi* worker xong trước khi ack (hoặc quản lý offset theo worker). Đánh đổi: tốc độ lấy an toàn.

### (b) OCR chứng từ: bị đá khỏi group

Mỗi tài liệu 12 giây, `max.poll.records=500` mặc định, `max.poll.interval.ms` 5 phút: một lô 500 record cần 100 phút. Consumer bị coi là chết, rebalance, và cả lô bị xử lý lại bởi người khác: "xử lý lặp liên hồi". Xử lý: hạ `max.poll.records` cho vừa với thời gian xử lý, hoặc tăng `max.poll.interval.ms` (đánh đổi: phát hiện consumer thật sự chết chậm hơn).

### (c) ETL báo cáo tuần: nghỉ lễ quay lại thì "không có gì để đọc"

Group rỗng quá 7 ngày: offset đã commit bị xoá; `auto.offset.reset=latest` làm job nhảy tới cuối và bỏ qua mọi thứ đã tích. Xử lý: `earliest` (đọc lại hết) hoặc `none` (ném lỗi cho bạn biết); tăng `offsets.retention.minutes` theo chu kỳ job. Đừng quên `retention.ms` của chính topic cũng có thể đã xoá dữ liệu.

### (d) Gọi xe: deploy làm cả đội điều phối đứng hình

Rolling restart 10 instance, mỗi lần một instance rời/vào là một lần rebalance; giao thức classic có rào chắn toàn group. Xử lý theo mức: static membership (instance về lại đúng chỗ cũ, đừng để trùng `group.instance.id`), cooperative assignor, `group.protocol=consumer` (cần broker hỗ trợ; Kafka Connect chưa dùng được). Trên MSK, AWS khuyên luôn đặt `group.instance.id`.

### (e) Nếu là ngân hàng

"Service nhận sự kiện chuyển tiền: tắt auto-commit, ack sau khi ghi DB xong, chống trùng bằng mã sự kiện": ba cụm từ này là câu trả lời đầy đủ cho "restart có sao không".

## 5. Tự nói thành lời

Trong 45 giây: *"Consumer crash giữa chừng thì ... vì offset được commit ... Để không mất, mình ... và chấp nhận ... Để hết trùng thì ..."* Dùng: poll, commit, at-least-once, rebalance, `max.poll.interval.ms`.

**Ôn nhanh:** [tóm tắt Bài 5](../summaries/0005-consumer-offset-commit.md) (đọc sau khi đã tự nói).

## 6. Kiểm tra

**1. Consumer Kafka thuần đang auto-commit, và hàm xử lý chỉ giao việc cho thread pool rồi trả về. Crash xảy ra. Điều gì đúng?**

- **A.** Việc chưa làm xong có thể bị mất vĩnh viễn
- **B.** Mọi việc chưa xong sẽ được nhận lại
- **C.** Broker tự đợi worker rồi mới commit
- **D.** Offset chỉ commit sau khi worker xong

<details><summary>Đáp án</summary>

**A. Việc chưa làm xong có thể bị mất vĩnh viễn**

Auto-commit chạy trong poll(): offset có thể đi trước việc xử lý thực sự, nên việc đã commit mà worker chưa làm xong thì mất.

</details>

**2. Với Boot + Spring Kafka không cấu hình gì thêm, commit offset do ai làm và khi nào?**

- **A.** Kafka client, mỗi năm giây một lần
- **B.** Ứng dụng, khi bạn gọi commitSync
- **C.** Container, sau khi xử lý hết lô poll
- **D.** Broker, khi nhận đủ số ack cấu hình

<details><summary>Đáp án</summary>

**C. Container, sau khi xử lý hết lô poll**

Spring đặt enable.auto.commit=false trừ khi bạn đặt tường minh, và AckMode mặc định là BATCH: container commit sau khi xử lý xong lô.

</details>

**3. Listener của bạn khai báo tham số Acknowledgment nhưng ack-mode là mặc định. Spring Kafka 4.1.2 làm gì?**

- **A.** Đưa null vào tham số rồi chạy tiếp
- **B.** Tự đổi ack-mode sang manual cho bạn
- **C.** Bỏ qua tham số và commit như thường
- **D.** Ném IllegalStateException khi gọi listener

<details><summary>Đáp án</summary>

**D. Ném IllegalStateException khi gọi listener**

Đã gặp khi chạy lab s6: cần AckMode MANUAL để Acknowledgment có mặt; nếu không, record bị xử lý như lỗi listener.

</details>

**4. Mỗi record xử lý 12 giây, max.poll.records=3, max.poll.interval.ms=10 giây. Triệu chứng nào hợp lý nhất?**

- **A.** Lag giảm đều và không có lỗi nào
- **B.** Producer nhận NotEnoughReplicasException
- **C.** Broker xoá offset đã commit của group
- **D.** Consumer rời group, record bị xử lý lặp

<details><summary>Đáp án</summary>

**D. Consumer rời group, record bị xử lý lặp**

Khoảng giữa hai lần poll vượt max.poll.interval.ms nên consumer bị coi là chết, rebalance xảy ra và record được giao lại (đã quan sát ở s7: mỗi offset xử lý 3 lần).

</details>

**5. Job ETL chạy lại sau 10 ngày nghỉ, group rỗng suốt thời gian đó, auto.offset.reset mặc định. Chuyện gì?**

- **A.** Job nhảy tới cuối, bỏ qua dữ liệu tích
- **B.** Job đọc lại topic từ offset đầu tiên
- **C.** Job báo lỗi và không khởi động được
- **D.** Job tiếp tục đúng chỗ đã dừng trước đó

<details><summary>Đáp án</summary>

**A. Job nhảy tới cuối, bỏ qua dữ liệu tích**

Offset commit hết hạn sau 7 ngày kể từ khi group rỗng; không còn offset hợp lệ thì auto.offset.reset=latest nhảy tới cuối. Dùng earliest hoặc none, và tăng offsets.retention.minutes.

</details>

## Nguồn nên đọc

> **Chính:** phần mở đầu của [KafkaConsumer javadoc](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) (mục offsets and consumer position, mục "Detecting Consumer Failures", "Manual Offset Control"), khoảng 25 phút.
>
> **Spring:** [Message listener container](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html) (AckMode, static membership).
>
> **Tra nhanh:** [Sổ tay](../reference/0003-tricky-scenarios.md), nhóm B.

**Bài tiếp theo:** [Bài 6: poison pill, retry, DLT](0006-error-handling-dlt.md): khi chính message gây lỗi, và mặc định của Spring Kafka là bỏ nó đi.

> **Còn chỗ nào mơ hồ?** Hỏi thẳng giáo viên, ví dụ "vì sao commit là offset+1?", "static membership khác cooperative ở đâu?". Dán kết quả quiz và số bạn thấy ở s6/s7.
