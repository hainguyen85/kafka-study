<!-- Tự sinh từ 0008-topic-design.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / [Bài 7](0007-idempotent-consumer-transactions.md) / Bài 8 · [Sổ tay tình huống](../reference/0003-tricky-scenarios.md) · [Managed trên AWS](../reference/0004-managed-kafka-msk-vs-confluent.md)

# Bài 8: Bạn là người tạo topic. Bảy quyết định không sửa lại được dễ dàng

Bài 4-7 là cấu hình để chữa. Bài này là cấu hình để khỏi phải chữa: những gì bạn chọn lúc tạo topic.

> **Liên quan tới mission** Bạn tự tạo và cấu hình topic ở nơi mới. Đây là phần thiết kế ảnh hưởng lớn nhất và khó đảo ngược nhất. Bài kiểm tra cuối (Bài 9) đòi bạn đưa ra các quyết định này trong 30 phút cho một bài toán lạ.

**Thắng lợi của bài này:** với một bài toán, bạn điền được "thẻ thiết kế topic" (key, số partition, RF/minISR, retention hay compaction, kích thước message, tên, schema) kèm lý do và đánh đổi, và biết quyết định nào gần như không đổi lại được.

## 1. Kiến thức tối thiểu (có nguồn)

1. **Số partition chỉ tăng, không giảm; tăng làm key đổi partition.** Docs: sau khi mở rộng "messages with the same key may be routed to different partitions"; Kafka không phân phối lại dữ liệu cũ. Với consumer `auto.offset.reset=latest`, thêm partition "may cause silent message loss". [[Basic operations]](https://kafka.apache.org/44/operations/basic-kafka-operations/) [[Consumer configs]](https://kafka.apache.org/44/configuration/consumer-configs/)
2. **Song song hoá bị chặn bởi số partition:** trong một group, mỗi partition chỉ có một consumer; thêm consumer vượt số partition là consumer nhàn rỗi. [[Design]](https://kafka.apache.org/44/design/design/)
3. **Retention mặc định:** `cleanup.policy=delete`, `retention.ms`=604800000 (7 ngày), `retention.bytes`=-1. [[Topic configs]](https://kafka.apache.org/44/configuration/topic-configs/)
4. **Compaction:** `cleanup.policy=compact` giữ giá trị mới nhất mỗi key; thứ tự và offset không đổi. Compaction chạy ngầm, hạn chót "is not a hard guarantee"; `min.cleanable.dirty.ratio`=0.5, `min.compaction.lag.ms`=0, segment đang ghi không bị compact. Tombstone (value `null`) bị xoá sau `delete.retention.ms`=86400000; consumer chậm hơn thế có thể bỏ lỡ lệnh xoá. [[Topic configs]](https://kafka.apache.org/44/configuration/topic-configs/) [[Design: log compaction]](https://kafka.apache.org/44/design/design/)
5. **Kích thước message có bốn tầng giới hạn độc lập:** producer `max.request.size`=1048576; broker/topic `message.max.bytes`/`max.message.bytes` (kích thước batch sau nén); follower `replica.fetch.max.bytes`=1048576; consumer `max.partition.fetch.bytes`=1048576 và `fetch.max.bytes`=52428800, hai giá trị này *không phải giới hạn cứng* (batch đầu quá lớn vẫn được trả). Lỗi: `RecordTooLargeException`. [[Producer]](https://kafka.apache.org/44/configuration/producer-configs/) [[Topic]](https://kafka.apache.org/44/configuration/topic-configs/) [[Consumer]](https://kafka.apache.org/44/configuration/consumer-configs/)
6. **Độ bền là cấu hình của *topic*** (Bài 4): RF và `min.insync.replicas` đặt khi tạo, kiểm bằng `--describe`. Với Spring: `TopicBuilder` không có `minInsyncReplicas()`, dùng `.config(...)`; topic đã tồn tại thì không bị sửa trừ khi bật `modify-topic-configs`. [[research/0002, mục 2.6]](../research/0002-spring-kafka.md)
7. **Tên và tạo topic:** tắt auto-create (Apache mặc định bật, MSK và Confluent Cloud tắt) để lỗi gõ tên không đẻ ra "topic ma" RF=1; tạo bằng code (`NewTopic`) hay IaC đều cần quyền. [[Bảng managed]](../reference/0004-managed-kafka-msk-vs-confluent.md)

## 2. Quyết định số 1: key, và vì sao thêm partition là chuyện lớn

Đây là bộ mô phỏng của Bài 2, dùng lại để nhớ lại bằng tay. Gửi vài message với cùng key ở 3 partition, đổi sang 6 và gửi tiếp, quan sát key nằm ở nhiều partition. (Hàm băm trong mô phỏng không phải của Kafka thật.)

**Số liệu thật từ lab:** `labs/05-scenarios` profile `s12` ghi 20 key vào topic 3 partition, tăng lên 6, ghi lại: **9/20 key đổi partition** (quan sát trên broker nhúng). Lab còn lại cho bạn chạy trên cụm: `--lab.phase=before`, tăng partition bằng `kafka-topics.sh --alter --partitions 6`, rồi `--lab.phase=after` (lệnh đầy đủ ở README).

## 3. Thẻ thiết kế topic

| # | Quyết định | Câu hỏi để chọn | Khó đổi? |
|---|---|---|---|
| 1 | Key | Thứ tự phải giữ theo đối tượng nào (đơn, tài khoản, thiết bị)? Key có phân bố đều không (key "nóng" làm một partition quá tải)? | Rất khó: đổi key là đổi cách đọc dữ liệu cũ |
| 2 | Số partition | Cần tối đa bao nhiêu consumer song song? Có chấp nhận key đổi partition sau này không? Trần số partition của dịch vụ managed? | Chỉ tăng; tăng làm đổi mapping |
| 3 | RF và `min.insync.replicas` | Mất vài message có chấp nhận không? Mất 1 broker có phải tiếp tục ghi không? | Sửa được, nhưng phải biết để kiểm |
| 4 | Retention hay compaction | Cần lịch sử hay chỉ cần trạng thái mới nhất? Bao lâu thì nhập lại dữ liệu cũ? | Sửa được; dữ liệu đã xoá thì mất |
| 5 | Kích thước message | Payload lớn nhất là bao nhiêu? Có thể giữ file ngoài rồi gửi tham chiếu? | Sửa 4 tầng đồng bộ |
| 6 | Tên, topic phụ (DLT, retry) | Quy ước tên? DLT có đủ partition, đúng minISR? | Đổi tên = topic mới |
| 7 | Schema (định dạng) | Ai sở hữu schema, thay đổi thế nào mà không phá consumer cũ? | Phá vỡ tương thích là khó nhất |

**Schema: ranh giới của bài này.** Mission chỉ dành một bài ngắn cho Schema Registry, nhưng **chưa có nguồn chính nào về Schema Registry/Avro/Protobuf trong workspace** (xem mục Gaps của `RESOURCES.md`), và Schema Registry không thuộc Apache Kafka lõi. Mình không dạy chi tiết từ trí nhớ. Mức thiết kế duy nhất an toàn lúc này: *coi định dạng message là hợp đồng giữa các service*, đặt `eventId` và một trường phiên bản ngay từ đầu (Bài 7 cần `eventId`), và hỏi platform team về registry và quy tắc tương thích. Bước tiếp theo nếu bạn muốn: mình chạy một vòng research riêng cho Schema Registry/tương thích schema rồi mới viết bài.

## 4. Thực hành: bốn thẻ thiết kế

Điền từng thẻ *trước*, rồi mở đáp án. Không có đáp án duy nhất: chấm theo việc bạn nêu đúng đánh đổi hay chưa.

### Thẻ A. Vị trí tài xế (gọi xe)

200.000 tài xế gửi toạ độ mỗi 3 giây. Hệ thống điều phối chỉ cần vị trí mới nhất; lịch sử chỉ để phân tích 1 ngày. Mất vài điểm không sao.

Mở đáp án gợi ý

**Key:** `driverId` (thứ tự theo tài xế). **Partition:** theo số consumer cần chạy song song cộng dư địa, vì không giảm được. **RF/minISR:** có thể RF=3 minISR=1 và `acks=1`, chấp nhận mất điểm đổi lấy độ trễ và thông lượng (đúng ngược với dòng tiền). **Retention:** `delete` ngắn (ví dụ 1-2 ngày) vì vị trí cũ vô giá trị; không cần compaction cho điều phối vì consumer luôn bám đuôi. **Cảnh báo:** 200.000/3 ≈ 67.000 message/giây: tính thông lượng và quota partition của dịch vụ managed trước khi chốt.

### Thẻ B. Sự kiện thanh toán

Mỗi giao dịch có thể bị gửi lại; không được mất; thứ tự theo tài khoản; cần truy vết 90 ngày.

Mở đáp án gợi ý

**Key:** `accountId` (thứ tự theo tài khoản; chú ý tài khoản "nóng"). **RF=3, minISR=2**, producer `acks=all` + idempotence + đọc kết quả gửi (Bài 4); `unclean.leader.election.enable=false` và kiểm giá trị thực tế trên dịch vụ managed. **Consumer:** tắt auto-commit, ack sau khi ghi DB, `UNIQUE(eventId)` (Bài 5, 7). **Lỗi:** `DefaultErrorHandler` + DLT cùng số partition, minISR=2, cảnh báo (Bài 6). **Retention:** `retention.ms` ≥ 90 ngày (nhớ `offsets.retention.minutes` mặc định 7 ngày nếu có consumer chạy thưa). **Phía phát:** outbox. **Bẫy:** blocking retry giữ thứ tự theo tài khoản nhưng làm cả partition đứng khi lỗi kéo dài.

### Thẻ C. Bảng giá sản phẩm

Mỗi sản phẩm có giá thay đổi thường xuyên; dịch vụ mới khởi động phải nạp được giá hiện tại của mọi sản phẩm; có sản phẩm bị gỡ.

Mở đáp án gợi ý

**Compaction:** `cleanup.policy=compact`, key = `productId`, value = giá; gỡ sản phẩm bằng tombstone (value `null`). Dịch vụ mới đọc từ `earliest` sẽ thấy giá mới nhất mỗi key (sau khi compaction chạy; trước đó vẫn có bản cũ, và nó không phải đảm bảo cứng). **Bẫy:** consumer chậm hơn `delete.retention.ms` (1 ngày mặc định) có thể bỏ lỡ tombstone nên giữ sản phẩm đã gỡ trong bộ nhớ. Trên MSK Express và Serverless có giới hạn về compaction (xem bảng managed). Code: `S13Compaction` (chưa chạy, xem README).

### Thẻ D. Tài liệu quét (ảnh 2-5 MB)

Mở đáp án gợi ý

Lựa chọn ưu tiên: **không đưa file vào Kafka**; lưu file ở object storage và gửi tham chiếu (pattern, không phải tính năng Kafka). Nếu bắt buộc đưa vào: nâng đồng bộ bốn tầng (producer `max.request.size`, topic `max.message.bytes`, `replica.fetch.max.bytes`, và kiểm consumer), cân nhắc nén; trên dịch vụ managed có thể có trần cứng (Confluent: 8 MB Basic/Standard, 20 MB Enterprise/Dedicated; MSK Serverless: 8 MiB) theo [bảng managed](../reference/0004-managed-kafka-msk-vs-confluent.md). S11 đã chạy: mặc định lỗi ở producer (2000089 > 1048576), nâng producer thì lỗi ở broker, nâng topic `max.message.bytes=3000000` thì thành công.

### Chạy ba lab trên cụm

```
cd labs/05-scenarios
mvn -q spring-boot:run -Dspring-boot.run.profiles=s11        # 4 dòng kết quả
mvn -q spring-boot:run -Dspring-boot.run.profiles=s12 -Dspring-boot.run.arguments="--lab.phase=before"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --alter --topic s12-keys --partitions 6
mvn -q spring-boot:run -Dspring-boot.run.profiles=s12 -Dspring-boot.run.arguments="--lab.phase=after"
mvn -q spring-boot:run -Dspring-boot.run.profiles=s13        # ~110 giây
```

**Trạng thái trung thực:** s11 và s12 đã chạy trên broker nhúng (kết quả ở trên là quan sát được). **s13 (compaction) chưa từng được chạy**: code biên dịch được, kết quả "số record theo key trước và sau compaction" là điều bạn sẽ là người đầu tiên thấy. Compaction không tất định về thời điểm, nên nếu thấy chưa gọn, đó là hành vi bình thường chứ chưa chắc là lỗi.

Tôi đã chạy s11, s12 trên cụm

Tôi đã chạy s13 và ghi lại số record trước/sau

## 5. Điều khác đi khi nơi mới là dịch vụ managed

Cụm tự dựng của bạn là chỗ để *học*; nơi làm việc là dịch vụ do devops lo. Bốn thứ cần hỏi trước khi chốt thẻ thiết kế (chi tiết, so sánh MSK/Confluent và nguồn ở [bảng managed](../reference/0004-managed-kafka-msk-vs-confluent.md)): **(1)** RF/minISR có bị khoá không (MSK Express khoá, Serverless không cho sửa, Confluent nhận minISR 1 hoặc 2); **(2)** `unclean.leader.election.enable` hiệu lực là gì (MSK Standard mặc định **true**); **(3)** bạn có quyền tạo topic và trần partition là bao nhiêu; **(4)** trần kích thước message. Những khái niệm của bài 4-8 dùng được ở mọi nơi; chỉ giá trị và quyền thay đổi.

## 6. Tự nói thành lời

Trong 60 giây, cho "bảng giá sản phẩm": *"Mình tạo topic với key ..., ... partition vì ..., chính sách dọn dẹp ... vì ..., độ bền ... Chỗ không đổi lại được là ..."*

**Ôn nhanh:** [tóm tắt Bài 8](../summaries/0008-topic-design.md) (đọc sau khi đã tự nói).

## 7. Kiểm tra

<!-- quiz:quiz -->

**1. Topic 3 partition cần tăng lên 6 vì lượng tải tăng. Điều nào đúng về dữ liệu và key?**

- **A.** Dữ liệu cũ được chia lại theo partition mới
- **B.** Key mới có thể đi partition khác key cũ
- **C.** Mọi key giữ nguyên partition ban đầu
- **D.** Chỉ các key chưa từng xuất hiện bị đổi

<details><summary>Đáp án</summary>

**B. Key mới có thể đi partition khác key cũ**

Kafka không phân phối lại dữ liệu cũ; sau khi tăng, message cùng key có thể đi tới partition khác (s12: 9/20 key đổi). Thứ tự theo key bị vỡ qua điểm tăng.

</details>

**2. Bảng giá cần "giá mới nhất mỗi sản phẩm" cho service khởi động lại. Chọn cấu hình nào?**

- **A.** cleanup.policy=compact với key là productId
- **B.** cleanup.policy=delete và retention ngắn
- **C.** min.insync.replicas bằng ba cho topic
- **D.** Số partition bằng số sản phẩm đang bán

<details><summary>Đáp án</summary>

**A. cleanup.policy=compact với key là productId**

Compaction giữ giá trị mới nhất mỗi key. Xoá sản phẩm bằng tombstone (value null). Retention theo thời gian sẽ xoá cả giá còn hiệu lực.

</details>

**3. Message 2 MB bị RecordTooLargeException. Bạn nâng max.request.size ở producer lên 3 MB. Điều gì rất có thể xảy ra tiếp?**

- **A.** Gửi thành công và consumer đọc được
- **B.** Producer tự chia nhỏ message cho vừa
- **C.** Consumer báo lỗi vì fetch size thấp
- **D.** Broker vẫn từ chối vì giới hạn của topic

<details><summary>Đáp án</summary>

**D. Broker vẫn từ chối vì giới hạn của topic**

Bốn tầng giới hạn độc lập (s11): sau producer tới topic max.message.bytes. Consumer mặc định vẫn đọc được record 2 MB vì fetch size không phải giới hạn cứng.

</details>

**4. Topic dòng tiền cần "không mất", 3 broker chịu mất 1 mà vẫn ghi được. Cặp RF và min.insync.replicas hợp lý?**

- **A.** RF=3 và min.insync.replicas=2
- **B.** RF=3 và min.insync.replicas=3
- **C.** RF=2 và min.insync.replicas=2
- **D.** RF=1 và min.insync.replicas=1

<details><summary>Đáp án</summary>

**A. RF=3 và min.insync.replicas=2**

RF=3, minISR=2 cho phép mất một broker mà vẫn ghi an toàn; minISR=3 làm mất một broker là ngừng ghi (thí nghiệm strict ở Bài 4).

</details>

**5. Nơi làm việc dùng MSK Standard. Giá trị mặc định nào có thể làm bạn mất dữ liệu đã ack khi partition sống lại?**

- **A.** num.partitions của mỗi topic
- **B.** auto.create.topics.enable
- **C.** unclean.leader.election.enable
- **D.** delete.retention.ms của compaction

<details><summary>Đáp án</summary>

**C. unclean.leader.election.enable**

Theo bảng managed: MSK Standard mặc định unclean.leader.election.enable=true (non-tiered), trong khi Apache là false. Bật nghĩa là chấp nhận mất dữ liệu để partition sống lại.

</details>

<!-- /quiz -->

## Nguồn nên đọc

> **Chính:** [Topic configs](https://kafka.apache.org/44/configuration/topic-configs/) (đọc `cleanup.policy`, `retention.ms`, `min.insync.replicas`, `max.message.bytes`, các mục compaction) và mục "Log compaction" trong [Design](https://kafka.apache.org/44/design/design/) (khoảng 30 phút).
>
> **Bổ trợ:** [Basic operations](https://kafka.apache.org/44/operations/basic-kafka-operations/), mục thêm partition.
>
> **Mã nguồn bài này:** `labs/05-scenarios` (`TopicsConfig`, `S11`, `S12`, `S13`). **Tra nhanh:** [Sổ tay](../reference/0003-tricky-scenarios.md), nhóm D.

**Bài tiếp theo:** [Bài 9: bài kiểm tra cuối](0009-final-design-test.md).

> **Còn chỗ nào mơ hồ?** Hỏi thẳng giáo viên, ví dụ "chọn bao nhiêu partition cho topic này?", "key nóng xử lý thế nào?". Dán kết quả quiz và thẻ thiết kế bạn tự điền để mình chấm.
