# 0001 - Rà soát giáo trình: khoảng trống, tình huống oái oăm, chỉnh lộ trình

Ngày: 2026-11-10. Phiên bản đối chiếu: Apache Kafka 4.4 (docs `kafka.apache.org/44/`). Chỉ dùng nguồn chính (docs Apache, Javadoc, KIP trên cwiki). Link dạng `[tên]` trỏ tới mục "Sources" ở cuối file. Chỗ nào là suy luận của tôi (không có câu nguyên văn trong docs) được đánh dấu **(suy ra)**.

## Tóm tắt

1. Lộ trình hiện tại bao phủ đúng xương sống (key/offset, acks/ISR, commit, DLQ, EOS). Chỗ cần chỉnh nhất là bài 4: ở 4.x **client mặc định đã là `acks=all` + `enable.idempotence=true`** ([producer-cfg]); thứ nguy hiểm thật là default phía **broker/topic**: `min.insync.replicas=1`, `default.replication.factor=1`, `num.partitions=1`, `auto.create.topics.enable=true` ([broker-cfg], [topic-cfg]).
2. Từ 4.1, cụm KRaft mới bật **ELR** mặc định và giá trị `min.insync.replicas` đặt ở mức broker "sẽ bị gỡ" ([upgrade]) -> bẫy khi bài 3 cấu hình cụm Docker bằng biến môi trường: nên đặt `min.insync.replicas` **ở cấp topic** và kiểm chứng bằng `--describe`.
3. Thiếu hẳn trong lộ trình: kích thước message (4 tầng config), `auto.offset.reset`/hết hạn offset, reset offset bằng CLI, static membership + giao thức rebalance mới (KIP-848, GA từ 4.0), hanging transaction / `read_committed`, Share Groups (GA từ 4.2), compaction làm "bảng trạng thái mới nhất".
4. Docs Apache chỉ mô tả DLQ cho Kafka Connect và Share Groups (4.4); retry-topic/DLQ cho consumer thường là **pattern ứng dụng, không có docs chính thức** -> bài 6 phải ghi rõ.
5. Đề xuất: tách bài 6 làm hai (poison pill/DLQ; rebalance + liveness), thêm một bài ngắn "Share Groups so với Consumer Groups", đưa phần client-side security lên trước schema (ngày đầu ở ngân hàng sẽ cần kết nối cụm có SASL/SSL), và hỏi người học 8 câu về môi trường ngân hàng (mục 4).
6. 16 tình huống ở mục 2 đều có nguồn; mỗi tình huống kèm một thí nghiệm CLI/Java trên cụm 3 broker.
7. Mục "Unverified" liệt kê những gì chưa kiểm chứng được (tên config quota, config SASL/SSL, Schema Registry, hành vi console-consumer, Share Groups có bật sẵn ở 4.4 hay không...).

---

## 1. Khoảng trống phủ sóng và điểm cần sửa

### 1.1 Điều chỉnh/sửa cho các bài đã lên kế hoạch

| Bài | Điều cần sửa/thêm | Bằng chứng |
|---|---|---|
| 3 (cụm 3 broker) | Topic nội bộ `__consumer_offsets` (`offsets.topic.replication.factor` mặc định **3**, `offsets.topic.num.partitions` 50), `__transaction_state` (`transaction.state.log.replication.factor` 3, `transaction.state.log.min.isr` **2**), nên cụm 1 broker mặc định dễ lỗi khi dùng group/transaction. Dùng 3 broker là đúng yêu cầu: docs nói transactions "require a cluster of at least three brokers". | [broker-cfg], [producer-cfg] |
| 3 | Tắt `auto.create.topics.enable` (mặc định **true**) và tạo topic tường minh. Topic tự tạo lấy `num.partitions` mặc định **1**, `default.replication.factor` mặc định **1**. | [broker-cfg], [consumer-cfg] (`allow.auto.create.topics`=true) |
| 3 | Học `kafka-topics.sh --describe` (Leader, Isr), `kafka-consumer-groups.sh --describe` (CURRENT-OFFSET, LOG-END-OFFSET, LAG), `kafka-leader-election.sh --election-type preferred`. `auto.leader.rebalance.enable=true` mặc định: sau khi broker bị kill rồi sống lại, leader tự quay về preferred replica. | [ops-basic], [broker-cfg] |
| 3 | KRaft: `process.roles` (`broker`, `controller`, hoặc cả hai), `controller.quorum.voters` là "cách cũ", dynamic quorum dùng `controller.quorum.bootstrap.servers`. 4.4: `broker.id` deprecated, dùng `node.id`; thư mục format bằng `kafka-storage` không tương thích ngược. | [broker-cfg], [upgrade] |
| 4 | Default 4.4: `acks=all`, `enable.idempotence=true`, `retries=2147483647`, `delivery.timeout.ms=120000`, `request.timeout.ms=30000`, `linger.ms=5` (đổi từ 0 ở 4.0), `batch.size=16384`, `max.in.flight.requests.per.connection=5`. Muốn thấy mất message phải **tự hạ** `acks=1`/`enable.idempotence=false` trong lab. | [producer-cfg], [upgrade] |
| 4 | Đặt tường minh `enable.idempotence=true` cùng `max.in.flight>5`/`acks!=all`/`retries=0` sẽ **ném lỗi**; nếu không đặt tường minh thì idempotence bị tắt âm thầm. Từ 4.0 không còn "tự fallback" khi `max.in.flight>5`. | [producer-cfg], [upgrade] |
| 4 | Thêm quan hệ timeout: `delivery.timeout.ms >= request.timeout.ms + linger.ms`; `request.timeout.ms` nên lớn hơn `replica.lag.time.max.ms` (broker, mặc định 30000). | [producer-cfg], [broker-cfg] |
| 4 | `min.insync.replicas` chỉ có tác dụng khi `acks=all`; ISR < min -> `NotEnoughReplicasException` (retriable). Biến thể `NotEnoughReplicasAfterAppendException`: phát hiện ISR thấp **sau khi** đã append, "Producer retries will cause duplicates". | [design], [exc-nerep], [exc-nerep-after] |
| 4 | Giải thích idempotence đúng phạm vi: chỉ trong **một session** của producer, và resend ở tầng ứng dụng không dedup được. | [producer-javadoc] |
| 5 | `enable.auto.commit=true` (mặc định) + `auto.commit.interval.ms=5000`; `auto.offset.reset=latest` (mặc định); `max.poll.records=500`; `max.poll.interval.ms=300000`; `session.timeout.ms=45000`; `heartbeat.interval.ms=3000`. Commit "offset của record kế tiếp cần đọc", không phải record vừa xử lý. | [consumer-cfg], [consumer-javadoc] |
| 5 | Thêm: `KafkaConsumer` **không thread-safe**; ngoại lệ duy nhất là `wakeup()` (mẫu shutdown). `poll(long)` đã bị xoá ở 4.0, chỉ còn `poll(Duration)`. | [consumer-javadoc], [upgrade] |
| 5 | Thêm "nằm ngoài code": reset offset bằng `kafka-consumer-groups.sh --reset-offsets` (`--to-earliest/--to-datetime/--shift-by/--by-duration/--to-offset`, mặc định `--dry-run`, nhóm phải **không có member active**). | [ops-basic] |
| 6 | Docs chỉ có DLQ cho **Connect** (`errors.tolerance=none` mặc định, `errors.deadletterqueue.topic.name`, `errors.retry.timeout=0`) và cho **Share Groups** (4.4, `share.version>=2`). Retry topic + DLQ cho consumer thường là pattern tự dựng. | [connect-guide], [ops-basic], [upgrade] |
| 6 | Rebalance: phân biệt `group.protocol=classic` (mặc định) và `consumer` (KIP-848, GA 4.0): `session.timeout.ms` và `heartbeat.interval.ms` client **không áp dụng** cho `consumer`, do broker quyết (`group.consumer.session.timeout.ms`=45000, `group.consumer.heartbeat.interval.ms`=5000). | [consumer-cfg], [broker-cfg], [upgrade] |
| 7 | Transactions: `transactional.id`, `transaction.timeout.ms=60000`, broker cap `transaction.max.timeout.ms=900000`; consumer cần `isolation.level=read_committed` (mặc định `read_uncommitted`) và `enable.auto.commit=false`; offset consumer commit **trong cùng transaction** với output. Lỗi fatal (`ProducerFencedException`, `OutOfOrderSequenceException`, `AuthorizationException`) chỉ còn `close()`, lỗi khác thì `abortTransaction()`. | [design], [producer-javadoc], [producer-cfg], [broker-cfg] |
| 7 | Giới hạn exactly-once: docs nói Kafka hỗ trợ exactly-once "in Kafka Streams"; hệ thống ngoài cần phối hợp. Với DB ngoài, cách docs mô tả là lưu offset cùng kết quả trong một giao dịch rồi `seek()` khi khởi động lại. | [design], [consumer-javadoc] |
| 8 | `cleanup.policy=delete` mặc định; `retention.ms`=604800000 (7 ngày), `retention.bytes`=-1; compaction: `min.cleanable.dirty.ratio`=0.5, `delete.retention.ms`=86400000, `min.compaction.lag.ms`=0. Nên có **lab compaction** (tình huống 12). 4.2: `cleanup.policy` rỗng nghĩa là giữ vô hạn. | [topic-cfg], [upgrade] |
| 9 | ACL: authorizer KRaft `org.apache.kafka.metadata.authorizer.StandardAuthorizer`; không có ACL thì chỉ super user truy cập (`allow.everyone.if.no.acl.found=false`); producer cần Write trên Topic (+ IdempotentWrite trên Cluster nếu idempotent, + Write trên TransactionalId nếu transactional); consumer cần Read trên Topic **và** Group. | [acl] |

### 1.2 Chủ đề chưa có trong lộ trình (xếp theo mức liên quan tới mission)

| Ưu tiên | Chủ đề | Ý chính có nguồn | Nguồn |
|---|---|---|---|
| Cao | Kích thước message, 4 tầng | Producer `max.request.size`=1048576; broker `message.max.bytes`=1048588 / topic `max.message.bytes` (kích thước batch sau nén); consumer `max.partition.fetch.bytes`=1048576 và `fetch.max.bytes`=52428800 **không phải giới hạn cứng** (batch đầu quá lớn vẫn được trả); follower `replica.fetch.max.bytes`=1048576. Lỗi: `RecordTooLargeException`. | [producer-cfg], [broker-cfg], [topic-cfg], [consumer-cfg], [exc-toolarge] |
| Cao | Hết hạn offset và `auto.offset.reset` | `offsets.retention.minutes`=10080 (7 ngày kể từ khi group rỗng); `auto.offset.reset=latest` mặc định; docs cảnh báo thêm partition khi `latest` "may cause silent message loss", khuyên `by_duration:<ISO8601>` (ví dụ `PT5S` cho consumer protocol, `PT6M` cho classic). | [broker-cfg], [consumer-cfg], [ops-basic] |
| Cao | Static membership + cooperative + KIP-848 | `group.instance.id` (mặc định null) tránh rebalance khi restart; `partition.assignment.strategy` mặc định `RangeAssignor, CooperativeStickyAssignor`; consumer protocol: assignor server-side `uniform,range` (`group.consumer.assignors`), `group.remote.assignor`. KIP-848 nêu rõ vấn đề của classic: rào chắn toàn group, một consumer lỗi làm ảnh hưởng cả group. | [consumer-cfg], [broker-cfg], [kip-848], [design] |
| Cao | Hanging transaction và `read_committed` | LSO là mốc đọc tối đa của `read_committed`; hanging transaction làm LSO không tiến, consumer `read_committed` kẹt, compaction không chạy (động cơ của KIP-890, đã bật khi finalize 4.0). | [consumer-javadoc], [kip-890], [upgrade] |
| Trung bình | ELR / `min.insync.replicas` đổi nghĩa | ELR = replica ngoài ISR nhưng đảm bảo có dữ liệu tới High Watermark; chỉ tồn tại khi ISR < min ISR. Mặc định bật cho cụm mới từ 4.1. Cảnh báo trong topic-configs: "when ELR is enabled, the semantics of this config changes". | [kip-966], [upgrade], [topic-cfg] |
| Trung bình | Share Groups (Queues for Kafka) | Preview 4.1, "production-ready" 4.2; nhiều consumer hơn số partition; ack từng record (accept/release/reject/renew); lock 30s (`share.record.lock.duration.ms`); `group.share.delivery.count.limit`=5; không đảm bảo thứ tự giữa các batch; không hỗ trợ static membership. | [upgrade], [design], [kip-932], [broker-cfg] |
| Trung bình | Quota | Hai loại: băng thông và request rate; áp theo (user, client-id), broker "mute" channel của client vi phạm. Tên config cụ thể chưa fetch được (xem Unverified). | [design], [multi-tenancy] |
| Trung bình | Admin tools | `kafka-features.sh` (bật `share.version`), `kafka-leader-election.sh` (kể cả unclean), `kafka-reassign-partitions.sh`. Không phải trọng tâm (ngoài scope vận hành). | [ops-basic], [topic-cfg] |
| Thấp | Compaction nâng cao, tiered storage, MirrorMaker 2 | MM1 đã bị xoá ở 4.0, chỉ còn MM2. | [upgrade] |

### 1.3 Thay đổi 4.0-4.4 khiến tutorial cũ sai (nên có hộp "đọc tutorial cũ thì coi chừng")

| Phiên bản | Thay đổi | Nguồn |
|---|---|---|
| 4.0 | ZooKeeper bị xoá, chỉ KRaft ("Apache Kafka 4.0 only supports KRaft mode"). Tutorial dùng `--zookeeper`, `zookeeper.connect` là hết hiệu lực. | [upgrade] |
| 4.0 | Java: client/Streams tối thiểu Java 11; broker/Connect/tools Java 17. Client phải >= 2.1 trước khi nâng cấp lên 4.0. | [upgrade] |
| 4.0 | `linger.ms` mặc định 0 -> **5**. | [upgrade], [producer-cfg] |
| 4.0 | Bỏ `log.message.format.version`; bỏ `poll(long)`; bỏ `Admin.alterConfigs`; MM1 bị xoá; Log4j -> Log4j2; `--bootstrap-server` chỉ nhận danh sách phân tách bằng dấu phẩy; file config KRaft không còn trong `config/kraft`. | [upgrade] |
| 4.0 | Group coordinator mới; KIP-848 GA, tự bật khi finalize lên 4.0; KIP-890 (giao thức transaction mới) bật khi finalize. | [upgrade] |
| 4.0 | `message.timestamp.after.max.ms` mặc định từ Long.Max -> 1 giờ (timestamp tương lai xa bị từ chối). | [upgrade] |
| 4.1 | ELR bật mặc định cho cụm mới; `flush()` trong callback của producer bị cấm (ném `KafkaException`); Share Groups preview. | [upgrade], [producer-javadoc] |
| 4.2 | Share Groups production-ready; null cho config kiểu LIST không còn được chấp nhận; `cleanup.policy` rỗng = giữ vô hạn. | [upgrade] |
| 4.3 | Metadata downgrade không còn được hỗ trợ; `group.coordinator.rebalance.protocols` deprecated (sẽ bỏ ở 5.0, chuyển sang feature version). | [upgrade], [broker-cfg] |
| 4.4 | `broker.id` deprecated (dùng `node.id`); `kafka-broker-api-versions.sh` deprecated; OAUTHBEARER broker khởi động lỗi nếu thiếu cấu hình audience/issuer; Share-group DLQ cần `share.version=2`. | [upgrade] |

---

## 2. Danh mục tình huống oái oăm (16 tình huống)

Quy ước mỗi tình huống: **Triệu chứng** / **Nguyên nhân (nguồn)** / **Config + default 4.4** / **Thí nghiệm** (cụm 3 broker). Domain đa dạng; phần "nếu là banking" chỉ thêm khi hữu ích.

### T1. Đơn hàng "đã nhận" nhưng mất sau khi broker chết (e-commerce flash sale)
- Triệu chứng: producer báo thành công, sau khi kill broker leader thì message biến mất.
- Nguyên nhân: ghi được coi là commit khi mọi replica **trong ISR** đã nhận; message không mất "as long as there is at least one in sync replica alive" ([design]). ISR có thể co lại chỉ còn leader (follower bị loại sau `replica.lag.time.max.ms`=30000 ([broker-cfg])); khi đó `acks=all` với `min.insync.replicas=1` thực tế tương đương một bản sao **(suy ra)**.
- Config: `acks=all` (mặc định), topic `replication.factor=3`, `min.insync.replicas=2` ([topic-cfg]: mặc định 1; docs: ISR nhỏ hơn min thì từ chối ghi).
- Thí nghiệm: topic RF=3 `min.insync.replicas=1`, `acks=1`; ghi liên tục, `kafka-topics --describe` để biết leader, kill leader ngay sau khi gửi; đếm message còn lại. Lặp lại với `acks=all` + minISR=2.

### T2. Cụm "sống" mà producer không ghi được (IoT telemetry)
- Triệu chứng: kill 2/3 broker, producer treo rồi lỗi sau ~2 phút.
- Nguyên nhân: `NotEnoughReplicasException` là `RetriableException` ([exc-nerep]); producer retry tới hết `delivery.timeout.ms`=120000 ([producer-cfg]). Đây là hành vi đúng: đổi availability lấy durability.
- Config: `min.insync.replicas=2` + `delivery.timeout.ms` (docs khuyên dùng nó thay `retries`); quyết định theo domain: telemetry chấp nhận mất vài điểm có thể chọn minISR=1, giao dịch tiền thì không.
- Thí nghiệm: kill 2 broker, quan sát thời gian tới khi `send()` callback trả lỗi; thử `delivery.timeout.ms=10000`.
- Biến thể: `NotEnoughReplicasAfterAppendException` - "Producer retries will cause duplicates" ([exc-nerep-after]) -> lý do cần idempotence ở T5.

### T3. Partition "chết" thay vì mất dữ liệu (game leaderboard/chat)
- Triệu chứng: sau khi mọi replica ISR chết, partition offline, ai cũng không ghi/đọc được.
- Nguyên nhân: mặc định chọn tính nhất quán, "favor waiting for a consistent replica" ([design]); `unclean.leader.election.enable=false` ([topic-cfg]). Bật lên thì có thể mất dữ liệu.
- Config: giữ `false` (mặc định); khi buộc phải ưu tiên sống, chạy `kafka-leader-election.sh` kiểu unclean thủ công ([topic-cfg]: KRaft chờ luồng định kỳ 5 phút nếu đổi động). ELR (4.1+ mặc định cho cụm mới) giảm rủi ro mất dữ liệu khi chỉ "min ISR - 1" broker unclean shutdown ([kip-966]).
- Thí nghiệm: RF=3; kill follower A, ghi tiếp; kill leader; bật lại A (A thiếu dữ liệu mới) -> xem `--describe` (Leader: none), rồi so sánh `unclean=true`.

### T4. Trạng thái đơn hàng bị đảo thứ tự khi retry (logistics: PICKED_UP -> DELIVERED)
- Triệu chứng: consumer thấy DELIVERED trước PICKED_UP dù cùng key.
- Nguyên nhân: `max.in.flight.requests.per.connection>1` + retries + idempotence tắt có thể đảo thứ tự; "if retries are disabled or if `enable.idempotence` is set to true, ordering will be preserved" ([producer-cfg]).
- Config: giữ `enable.idempotence=true` (mặc định), `max.in.flight<=5`. Đặt idempotence tường minh = true cùng `max.in.flight>5` sẽ báo lỗi ([producer-cfg], [upgrade]).
- Thí nghiệm: `enable.idempotence=false`, `max.in.flight=5`, `retries` lớn, gắn số thứ tự vào value, rồi kill/start leader liên tục khi ghi; so với `idempotence=true`. Khó tái hiện tất định, xem Unverified.

### T5. Trùng message dù đã bật idempotence (bán vé, đặt chỗ)
- Triệu chứng: cùng một lệnh đặt vé xuất hiện hai lần sau khi service restart.
- Nguyên nhân: idempotence chỉ bảo đảm trong "a single session" của producer; resend ở tầng ứng dụng "cannot be de-duplicated" ([producer-javadoc]). Design: broker lọc trùng bằng sequence number gắn theo producer ([design]).
- Config/giải pháp: dedup ở consumer bằng business id (bài 7), hoặc `transactional.id` ổn định nếu cần exactly-once trong Kafka ([producer-cfg]: persists across restarts). Với ghi DB + publish: outbox (pattern, không có trong docs Kafka).
- Thí nghiệm: producer gửi, `kill -9` trước khi nhận ack, chạy lại và gửi lại cùng payload; đếm trùng.

### T6. Auto-commit: vừa mất vừa trùng (gửi email/SMS thông báo)
- Triệu chứng: sau crash, vài thông báo không bao giờ gửi, vài cái gửi hai lần.
- Nguyên nhân: auto-commit mặc định bật, mỗi 5 giây ([consumer-cfg]). Nếu không xử lý xong mọi record của `poll` trước lần `poll` kế/`close` thì "committed offset can move ahead of consumption" (mất) ([consumer-javadoc]); xử lý xong nhưng chưa commit mà crash thì replay (trùng). Manual commit sau khi ghi DB cho at-least-once: crash giữa ghi và commit gây trùng ([consumer-javadoc]).
- Config: `enable.auto.commit=false` + `commitSync` sau xử lý; hoặc lưu offset cùng kết quả trong DB rồi `seek()` ([consumer-javadoc]).
- Thí nghiệm: handler `sleep` ngẫu nhiên, đẩy việc sang thread pool rồi để main thread `poll` tiếp; `kill -9` ở giữa; so sánh số xử lý thực với số gửi.

### T7. Xử lý chậm bị đá khỏi group, rebalance liên hồi (xử lý video, OCR chứng từ)
- Triệu chứng: log có `CommitFailedException`, message bị xử lý lặp, rebalance liên tục.
- Nguyên nhân: nếu không gọi `poll` trong `max.poll.interval.ms` (mặc định 300000) thì consumer rời group; `commitSync` ném `CommitFailedException` ([consumer-javadoc], [exc-commitfailed]). Rebalance chỉ diễn ra khi `poll` chạy ([consumer-javadoc]).
- Config: giảm `max.poll.records` (mặc định 500) hoặc tăng `max.poll.interval.ms` (docs: tăng có thể trì hoãn rebalance), `pause()/resume()` khi giao việc cho worker ([consumer-javadoc]).
- Thí nghiệm: `max.poll.interval.ms=10000`, handler ngủ 15 giây; quan sát log group qua `kafka-consumer-groups --describe --members`.

### T8. Một message hỏng chặn cả partition (poison pill - đa domain)
- Triệu chứng: lag của một partition tăng mãi, consumer log lặp cùng exception tại cùng offset.
- Nguyên nhân: lỗi giải mã ném `RecordDeserializationException` mang `topicPartition()` và `offset()` ([exc-deser]); không xử lý thì `poll` lặp lại lỗi **(suy ra)**. Docs chính thức của DLQ tồn tại cho Connect (`errors.tolerance=all` + `errors.deadletterqueue.topic.name`; mặc định `none`, không DLQ) ([connect-guide]) và Share Groups (REJECT, giới hạn `group.share.delivery.count.limit`=5) ([design], [broker-cfg]).
- Giải pháp: bắt exception, ghi record thô (`keyBuffer()/valueBuffer()`) sang topic lỗi rồi `seek` qua offset hỏng (pattern tự dựng; docs không mô tả cách bỏ qua - xem Unverified).
- Thí nghiệm: producer cố ý gửi chuỗi không phải JSON; consumer dùng JSON deserializer; quan sát lag đứng yên; sau đó thêm xử lý skip + DLQ.

### T9. Deploy rolling restart làm cả group dừng (gọi xe: dispatch thời gian thực)
- Triệu chứng: mỗi lần deploy, toàn group ngừng xử lý vài giây-chục giây.
- Nguyên nhân: classic protocol có rào chắn toàn group, "a single misbehaving consumer can take down or disturb the whole group" ([kip-848]).
- Config: (a) `group.instance.id` duy nhất cho mỗi instance - static membership, không rebalance khi restart ([design]); đánh đổi: member vắng mặt giữ partition tới hết session timeout (45000 mặc định) rồi mới bị gỡ ([kip-848], [consumer-cfg]); trùng `group.instance.id` thì member sau bị từ chối/fence ([design], [kip-848]); (b) `partition.assignment.strategy` có `CooperativeStickyAssignor` ([consumer-cfg]); (c) `group.protocol=consumer` (KIP-848 GA 4.0, ví dụ: "a consumer should not be impacted at all by a rebalance if its assignment is not changed" ([kip-848])). Lưu ý: với protocol `consumer`, `session.timeout.ms`/`heartbeat.interval.ms` phía client không dùng ([consumer-cfg]); Kafka Connect không hỗ trợ protocol mới ([kip-848]).
- Thí nghiệm: 3 consumer, kill/restart lần lượt; đo khoảng không có record được xử lý với classic vs static vs consumer protocol.

### T10. Thêm partition làm hỏng thứ tự theo key và âm thầm mất record (IoT, trạng thái thiết bị)
- Triệu chứng: sau `--alter --partitions 12`, cùng deviceId nằm ở partition khác; consumer mới `latest` bỏ lỡ record ở partition mới.
- Nguyên nhân: "messages with the same key may be routed to different partitions after the expansion", Kafka không phân phối lại dữ liệu cũ, không giảm được số partition ([ops-basic]). `auto.offset.reset=latest` + thêm partition "may cause silent message loss" ([consumer-cfg]).
- Config: quyết định số partition từ đầu (bài 2 đã có); dùng `auto.offset.reset=by_duration:<...>` (docs khuyên) hoặc `earliest`.
- Thí nghiệm: topic 3 partition, ghi key cố định, tăng lên 6, ghi tiếp, `--describe` + console consumer `--property print.key/partition` xem key chuyển partition.

### T11. Message quá lớn: lỗi ở một tầng, sửa xong lại lỗi ở tầng kế (tài liệu scan, ảnh)
- Triệu chứng: `RecordTooLargeException` ([exc-toolarge]); sửa producer xong thì broker từ chối; consumer đọc "giật".
- Nguyên nhân: 4 tầng giới hạn độc lập: producer `max.request.size`=1048576; broker `message.max.bytes`/topic `max.message.bytes`=1048588 (batch sau nén); follower `replica.fetch.max.bytes`=1048576; consumer `max.partition.fetch.bytes`=1048576 và `fetch.max.bytes`=52428800 (không cứng: batch đầu lớn vẫn được trả) ([producer-cfg], [topic-cfg], [broker-cfg], [consumer-cfg]).
- Giải pháp: tăng đồng bộ các tầng cho cần thiết, bật `compression.type` (mặc định `none` phía producer, topic `producer`) hoặc giữ file ngoài Kafka rồi gửi tham chiếu (pattern, không phải docs).
- Thí nghiệm: gửi 2 MB, ghi lại exception; nâng từng config theo thứ tự producer -> topic -> consumer.

### T12. Topic compacted làm "bảng giá mới nhất" (e-commerce giá sản phẩm, device shadow)
- Triệu chứng: kỳ vọng topic chỉ còn 1 bản ghi mỗi key nhưng vẫn thấy nhiều; consumer chậm không thấy lệnh xoá.
- Nguyên nhân: `cleanup.policy=compact` giữ giá trị mới nhất mỗi key; thứ tự không đổi, offset không đổi; consumer bám kịp thấy mọi message; hạn chót compaction "is not a hard guarantee"; tombstone (value null) bị xoá sau `delete.retention.ms`=86400000 nên consumer trễ hơn thế có thể bỏ lỡ delete ([design], [topic-cfg]). `min.cleanable.dirty.ratio`=0.5, `segment.bytes`=1 GiB (segment đang ghi không bị compact) ([topic-cfg]).
- Config: `cleanup.policy=compact`, hạ `segment.ms`/`segment.bytes`/`min.cleanable.dirty.ratio` trong lab để thấy hiệu quả.
- Thí nghiệm: ghi key A 1000 lần + tombstone cho key B; `kafka-console-consumer --from-beginning` trước và sau khi compact.

### T13. Transaction "giữa chừng": consumer read_committed đứng yên (inventory reserve + order event)
- Triệu chứng: producer transactional chết giữa chừng, consumer `read_committed` ngừng tiến; consumer `read_uncommitted` vẫn thấy dữ liệu aborted.
- Nguyên nhân: `read_committed` chỉ đọc tới LSO; record aborted/đang mở bị bỏ qua ([consumer-javadoc], [design]). Hanging transaction làm LSO không tiến ([kip-890]). Transaction bị coordinator đóng sau `transaction.timeout.ms` (60000, trần broker 900000) ([producer-cfg], [broker-cfg]).
- Config: producer `transactional.id` ổn định, consumer `isolation.level=read_committed` + `enable.auto.commit=false`; commit offset consumer bằng `sendOffsetsToTransaction` trong cùng transaction ([design]); lỗi `ProducerFencedException` -> `close()` ([producer-javadoc]); transaction cần RF>=3, minISR 2 ([producer-javadoc]).
- Thí nghiệm: consume-transform-produce; ném exception để abort rồi so hai consumer; sau đó kill -9 producer giữa transaction, đo thời gian LSO mới tiến.

### T14. Consumer nghỉ lâu, quay lại thì mất dữ liệu cũ (ETL đêm, báo cáo tuần)
- Triệu chứng: job chạy lại sau nghỉ lễ, không đọc được gì từ trước đó hoặc nhảy tới cuối.
- Nguyên nhân: offset đã commit bị xoá sau `offsets.retention.minutes`=10080 (7 ngày) kể từ khi group rỗng ([broker-cfg]); khi không còn offset hợp lệ, `auto.offset.reset` quyết định (`latest` mặc định) ([consumer-cfg]); dữ liệu cũ cũng có thể đã bị xoá bởi `retention.ms`=7 ngày ([topic-cfg]). Quan hệ nhân quả giữa hai việc này: **(suy ra)** từ các mô tả config.
- Config: `auto.offset.reset=earliest` hoặc `none` (ném lỗi), tăng `offsets.retention.minutes`/`retention.ms` hợp với chu kỳ job.
- Thí nghiệm: hạ `offsets.retention.minutes`/`retention.ms` ở cấp phù hợp (nếu dynamic config cho phép - chưa kiểm), đợi, chạy lại consumer.

### T15. Topic "ma" do gõ sai tên (microservice dùng chung cụm)
- Triệu chứng: consumer/producer chạy "bình thường" nhưng không ai nhận được message; `--list` hiện thêm topic lạ.
- Nguyên nhân: `auto.create.topics.enable=true`, `allow.auto.create.topics=true` ở consumer ([broker-cfg], [consumer-cfg]), topic mới lấy `num.partitions=1`, `default.replication.factor=1` (nếu không đặt tường minh) ([broker-cfg]).
- Config: `auto.create.topics.enable=false`; tạo topic bằng IaC/`kafka-topics.sh --create` với RF và minISR chủ ý. Trong ngân hàng quyền tạo topic thường nằm ở ACL ([acl]).
- Thí nghiệm: producer gửi vào `order-evnts`, `kafka-topics --describe` thấy RF=1.

### T16. Share Groups cho "hàng đợi việc" (xử lý ảnh/webhook) - khi partition giới hạn song song
- Triệu chứng: 4 partition nhưng cần 20 worker song song; consumer group chỉ dùng được 4.
- Nguyên nhân: consumer group: mỗi partition chỉ một consumer trong group; share group cho phép nhiều consumer hơn partition ([design], [kip-932]). Đổi lại: không đảm bảo thứ tự giữa batch, delivery at-least-once, ack từng record, không static membership ([kip-932]).
- Config: `kafka-features.sh` nâng `share.version=1` (4.1) - 4.2 production-ready, DLQ cần `share.version=2` (4.4); lock `share.record.lock.duration.ms`=30000; `group.share.delivery.count.limit`=5 (min 2, max 10); `group.share.partition.max.record.locks`=2000 ([upgrade], [ops-basic], [broker-cfg]). Cụm <3 broker phải hạ RF và minISR của topic state trước khi dùng lần đầu ([upgrade]).
- Thí nghiệm: producer ghi 100 job; 8 `ShareConsumer` trên topic 3 partition, một worker ngủ quá 30s -> record được giao lại; một record `REJECT` -> đi DLQ nếu cấu hình.
- Lưu ý bank: nếu nghiệp vụ cần thứ tự theo key thì share group không phù hợp **(suy ra từ [kip-932])**.

---

## 3. Đề xuất chỉnh lộ trình

### 3.1 Đề xuất bài mới/điều chỉnh

| # | Hiện tại | Đề xuất | Lý do (theo mission) |
|---|---|---|---|
| 3 | Dựng cụm + CLI + kill broker | Giữ; thêm: topic nội bộ + vì sao cần 3 broker; tắt auto-create; đọc `--describe` (Leader/Isr); xem ELR bật mặc định -> đặt minISR **cấp topic**; hộp "đọc tutorial cũ" (mục 1.3) | Lab thành nền cho mọi bài sau; tránh học sai từ tutorial ZooKeeper |
| 4 | Producer + durability | Giữ; lấy T1, T2, T4, T5 làm lõi; thêm timeout (`delivery.timeout.ms`, `request.timeout.ms`), `NotEnoughReplicas*`, T11 (kích thước message) | Mục tiêu "cấu hình để giải quyết loại vấn đề" |
| 5 | Consumer + commit | Giữ; lấy T6, T10, T14; thêm `kafka-consumer-groups` reset offset, `seek`, thread-safety/`wakeup` | Tình huống thực tế khi tech lead phải gỡ rối production |
| 6a | Poison pill, retry+DLQ, lag | Đổi tên: "Lỗi xử lý: poison pill, retry, DLQ" (T8); ghi rõ pattern tự dựng vs DLQ có trong docs (Connect, Share Groups) | Tránh lesson khẳng định pattern như thể docs Kafka đã quy định |
| 6b | rebalance, max.poll.interval.ms | Tách thành bài riêng: T7, T9; classic vs `consumer` protocol; static membership; cooperative | Quá nhiều ý cho một bài; đúng mục tiêu "rebalance gây trùng message" |
| 6c (mới, ngắn) | - | "Share Groups khác Consumer Groups thế nào" (T16) - chỉ so sánh + 1 lab nhỏ | Từ 4.2 production-ready; người đọc blog sẽ gặp thuật ngữ "queue for Kafka" |
| 7 | Idempotent consumer, outbox, transactions | Giữ; thêm T13 (hanging transaction/LSO); nhấn mạnh phạm vi EOS (Streams/trong Kafka, ngoài Kafka cần dedup) | [design] nêu rõ phạm vi |
| 8 | Schema, naming, retention vs compaction | Dời phần retention/offset retention (T14) lên bài 5; giữ compaction (T12) ở 8; thêm tương thích schema | `offsets.retention.minutes` tác động trực tiếp tới consumer |
| 9 | Security + lag + event design + Connect/Streams | **Đưa phần client-side security (cấu hình client kết nối cụm có SASL/SSL, ACL tối thiểu producer/consumer) lên trước bài 8**; phần còn lại giữ | Ngày đầu ở ngân hàng sẽ phải kết nối cụm bảo mật; cần hiểu lỗi quyền (Read trên Group) |

### 3.2 Thứ tự đề xuất
3 -> 4 -> 5 -> 6a -> 6b -> 7 -> security client-side -> 8 -> 6c (Share Groups, tùy chọn) -> 9 (giám sát lag, event design, Connect/Streams).

### 3.3 Chưa nên dành thời gian
- Tuning vận hành cụm (log cleaner, thread pool, `kafka-reassign-partitions.sh`, cordon/decommission), tiered storage, MirrorMaker 2 - nằm ngoài scope mission ([ops-basic] chỉ nên tra khi cần).
- Cấu hình quorum KRaft chi tiết (dynamic quorum, `controller.quorum.voters` ngoài mức lab).
- Kafka Streams/Connect nội bộ (chỉ cần tổng quan; Streams rebalance protocol KIP-1071 mới GA ở 4.2 và còn lỗi ở 4.2.0 theo [upgrade]).
- Đọc source code, tune `linger.ms`/`batch.size`/compression để tối ưu throughput (chỉ giữ ở mức hiểu trade-off).
- Mọi tài liệu ZooKeeper/Log4j 1/`poll(long)`.

---

## 4. Câu hỏi làm rõ mission (để giáo viên hỏi người học)

| # | Câu hỏi | Vì sao docs cho thấy nó quan trọng |
|---|---|---|
| 1 | Cụm ngân hàng là Apache Kafka tự vận hành, Confluent Platform hay Confluent Cloud/dịch vụ managed khác? Phiên bản broker là bao nhiêu? | Share Groups (4.1 preview, 4.2 GA), KIP-848 (GA 4.0), ELR (mặc định 4.1) phụ thuộc phiên bản; docs Apache không phủ Schema Registry hay tính năng của vendor |
| 2 | Client dùng thư viện nào: Java thuần, Spring Kafka, Kafka Streams, Kafka Connect, hay ngôn ngữ khác? | Protocol `consumer` chưa hỗ trợ cho Connect ([kip-848]); config `group.protocol` là của Java client; nếu Spring thì docs Apache không phủ (không có nguồn chính ở đây) |
| 3 | Ai tạo và cấu hình topic (số partition, RF, minISR, retention)? Có tắt `auto.create.topics.enable` không? | Đây là điểm quyết định T1, T10, T15 |
| 4 | Cơ chế bảo mật: SASL (cơ chế nào?), SSL/mTLS, ACL theo prefix hay từng topic? | Ảnh hưởng thứ tự bài; ACL consumer cần Read trên Group ([acl]); 4.4 thay đổi OAUTHBEARER ([upgrade]) |
| 5 | Yêu cầu giao hàng với nghiệp vụ chính là gì: không mất (at-least-once + dedup), hay "exactly-once" với DB/hệ thống ngoài? | Docs giới hạn EOS trong Kafka/Streams ([design]) |
| 6 | Thứ tự có ràng buộc theo khóa nào (tài khoản, đơn hàng)? Dự kiến throughput/số partition? | T4, T10, T16 |
| 7 | Schema: Avro/Protobuf/JSON? Có Schema Registry (của ai)? Quy ước đặt tên topic và versioning? | Ngoài phạm vi docs Apache đã fetch |
| 8 | Có cần nhiều trung tâm dữ liệu/DR (MirrorMaker 2), yêu cầu lưu giữ dài hạn/audit (retention vô hạn, compaction)? | `cleanup.policy` rỗng (4.2) và compaction đổi cách thiết kế event ([upgrade], [topic-cfg]) |

---

## Unverified (chưa kiểm chứng bằng nguồn chính đã fetch)

- Tên config quota (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) và cách đặt bằng `kafka-configs.sh`: trang [multi-tenancy] chỉ nói khái niệm, trang quota chi tiết chưa fetch.
- Config client cho SASL/SSL (`security.protocol`, `sasl.mechanism`, `sasl.jaas.config`...) và nội dung `/44/security/` ngoài trang ACL.
- Schema Registry, Avro: không thấy trong các trang đã fetch; chưa tìm kiếm có hệ thống trong docs Apache.
- Spring Kafka, Confluent: không thuộc nguồn chính của Apache Kafka, chưa tra.
- `kafka-console-consumer.sh` có cờ `--isolation-level` hay không, và cách dùng `kafka-transactions.sh` để xử lý hanging transaction: [ops-basic] không nêu.
- Cách bỏ qua một record hỏng: `RecordDeserializationException` không mô tả; việc `seek(partition, offset+1)` là suy ra từ khả năng `seek()` ([consumer-javadoc], phần "Storing Offsets Outside Kafka").
- Pattern retry-topic và DLQ cho consumer thường, outbox pattern, claim-check: không có trong docs Apache đã đọc.
- Share Groups có bật sẵn trên cụm 4.4 mới hay vẫn cần `kafka-features.sh` nâng `share.version`: upgrade notes chỉ nói cách bật ở 4.1 và DLQ ở `share.version=2`.
- Biến môi trường của image `apache/kafka` (ví dụ có map `KAFKA_MIN_INSYNC_REPLICAS` hay không) và hành vi thực tế khi ELR đã bật; mới có câu "broker-level min.insync.replicas ... will be removed" trong [upgrade].
- Tái hiện tất định cho T4 (đảo thứ tự do retry khi idempotence tắt): docs nói là có thể xảy ra, chưa có công thức lab đảm bảo.
- Điều chỉnh động `offsets.retention.minutes`/retention cho T14 trong lab: chưa kiểm xem config nào cho đổi động.
- Các trang chỉ đọc được một phần do giới hạn độ dài: [kip-848] (khoảng 100000/163678 ký tự), [kip-932] (100000/225772), [consumer-javadoc] (100000/206288), [broker-cfg] (hai đoạn đầu, thiếu ~1500 ký tự cuối).
- Phiên bản 4.4.0 là phiên bản hiện hành lấy từ đề bài; trang quickstart không được fetch lại trong lần này.
- Nội dung nhiều trang được tóm tắt/trích bởi công cụ fetch (WebFetch trả bản tóm tắt), nên câu trích nguyên văn nên được đối chiếu lại trên trang gốc trước khi đưa vào lesson.

---

## Sources (tất cả URL đã fetch)

| Tag | URL | Dùng cho |
|---|---|---|
| producer-cfg | https://kafka.apache.org/44/configuration/producer-configs/ | default producer, ràng buộc idempotence/acks/in-flight, transactional.id |
| consumer-cfg | https://kafka.apache.org/44/configuration/consumer-configs/ | default consumer, group.protocol, session/heartbeat, auto.offset.reset, fetch sizes |
| topic-cfg | https://kafka.apache.org/44/configuration/topic-configs/ | min.insync.replicas, unclean election, retention, compaction, max.message.bytes |
| broker-cfg | https://kafka.apache.org/44/configuration/broker-configs/ | default.replication.factor, num.partitions, auto.create, offsets/transaction topics, group.share.*, group.consumer.*, process.roles (hai đoạn: offset 0 và 100000) |
| upgrade | https://kafka.apache.org/44/getting-started/upgrade/ | thay đổi 4.0-4.4 (ZooKeeper, linger.ms, ELR, KIP-848, KIP-890, share groups) |
| design | https://kafka.apache.org/44/design/design/ | delivery semantics, idempotence, transactions, replication/ISR, compaction, quotas, static membership, share consumer |
| ops-basic | https://kafka.apache.org/44/operations/basic-kafka-operations/ | thêm partition, consumer-groups CLI/reset offset, leader election, share groups CLI/DLQ |
| consumer-javadoc | https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html | commit, rebalance, max.poll, thread-safety, read_committed/LSO (đọc nửa đầu trang) |
| producer-javadoc | https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html | idempotence phạm vi, transactions, lỗi fatal, callback |
| exc-nerep | https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/NotEnoughReplicasException.html | NotEnoughReplicasException retriable |
| exc-nerep-after | https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/NotEnoughReplicasAfterAppendException.html | trùng khi retry sau append |
| exc-toolarge | https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/RecordTooLargeException.html | RecordTooLargeException |
| exc-commitfailed | https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/CommitFailedException.html | CommitFailedException sau rebalance |
| exc-deser | https://kafka.apache.org/44/javadoc/org/apache/kafka/common/errors/RecordDeserializationException.html | accessor offset/partition khi giải mã lỗi |
| (shareconsumer) | https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/ShareConsumer.html | chỉ xác nhận API ack tồn tại; không mô tả semantics |
| connect-guide | https://kafka.apache.org/44/kafka-connect/user-guide/ | errors.tolerance, DLQ, exactly-once source connector |
| acl | https://kafka.apache.org/44/security/authorization-and-acls/ | authorizer, ACL producer/consumer |
| multi-tenancy | https://kafka.apache.org/44/operations/multi-tenancy/ | khái niệm quota/tenant |
| kip-848 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol | động cơ và thiết kế giao thức rebalance mới, static membership, giới hạn |
| kip-932 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka | share groups: bảo đảm, ack, giới hạn |
| kip-966 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas | ELR, ý nghĩa min.insync.replicas mới |
| kip-890 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense | hanging transaction, epoch bump |
