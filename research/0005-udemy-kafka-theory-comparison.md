# 0005 - Đối chiếu khoá Udemy "Learn Apache Kafka for Beginners v3" (section Kafka Theory) với bài 1-9

Ngày truy cập: 2026-11-12. Phiên bản đối chiếu cho claim kỹ thuật: Apache Kafka 4.4 (docs `kafka.apache.org/44/`). Curriculum Udemy chỉ dùng làm **bản đồ chủ đề**, không làm nguồn cho bất kỳ claim kỹ thuật nào. Claim kỹ thuật chỉ dẫn nguồn chính (docs Apache, KIP, mã nguồn Apache Kafka, docs của librdkafka cho hành vi của librdkafka). Link dạng `[tên]` trỏ tới mục "Sources" ở cuối file. Chỗ nào là suy luận của tôi được đánh dấu **(suy ra)**. ZooKeeper bị bỏ qua theo quyết định của người học (NOTES.md, 2026-11-12).

## Tóm tắt

1. **Lấy được curriculum thật**, từ chính trang khoá học [udemy-clp] (HTTP 200): 18 section, 123 lecture, 8h 20m, "Last updated 11/2026", course id `1075642`. Tên và thời lượng từng bài lấy từ dữ liệu curriculum nhúng trong HTML trang và khối JSON-LD `Syllabus`. API công khai [udemy-api] bị Cloudflare chặn (trang "Just a moment..."), nhưng không cần tới.
2. Section **Kafka Theory** có 10 video (45m31s) và 1 quiz. Bỏ 2 bài ZooKeeper/KRaft-lịch-sử, 8 bài còn lại thì bài 1-9 của workspace **phủ đủ 5 bài và phủ một phần 3 bài**. Workspace đi sâu hơn hẳn ở acks/ISR/minISR/unclean election (Bài 4) và commit/rebalance (Bài 5).
3. Khoảng trống trong phạm vi Kafka Theory: (a) `bootstrap.servers` chỉ để *khám phá* cụm, kèm `advertised.listeners`; (b) cấu trúc một record (key, value, timestamp, headers; broker chỉ lưu byte) và serializer/deserializer; (c) follower fetching theo rack/AZ (KIP-392, `client.rack`); (d) hàm băm key là murmur2, và client librdkafka mặc định băm khác (CRC32): cùng key có thể vào partition khác.
4. Khoảng trống từ các section khác của khoá, mức liên quan **cao** với mission: `kafka-configs.sh --alter` để sửa config của topic đã có (người học là người cấu hình topic); `max.block.ms` + `buffer.memory`: khi cụm không tới được, `send()` có thể chặn thread gọi tới 60 giây.
5. Mức **trung bình**: nén và batching (`compression.type`, `linger.ms`, `batch.size`) cho đánh đổi throughput/độ trễ; tổng quan Connect/Streams (MISSION ghi "chỉ tổng quan" nhưng NOTES ghi "Bỏ": cần người học chốt); segment (retention và compaction làm việc theo từng file segment).
6. Cấu trúc: **không nên** sắp lại theo khoá Udemy. Khoá học dạy 45 phút lý thuyết thuần rồi mới tới cài đặt/CLI/Java; workspace đan lý thuyết với lab theo đúng `teach`. Chỉ cần chèn 4-6 mục nhỏ vào Bài 2, 3, 4, 8 (mục 5.1).
7. Không đưa vào: ZooKeeper và lịch sử KRaft, "null key thì round-robin" (đã lỗi thời, 4.4 dùng sticky partition), cài đặt theo từng hệ điều hành, Conduktor, OpenSearch/Wikimedia, phần vận hành/bảo mật/MirrorMaker cho admin (mục 5.3).

---

## 1. Curriculum lấy được

### 1.1 Cách lấy và độ tin cậy

- `GET https://www.udemy.com/course/apache-kafka/` (User-Agent trình duyệt): HTTP 200, 397.916 byte. Trang chứa:
  - khối JSON-LD `@type: Course` với tên "Apache Kafka Series - Learn Apache Kafka for Beginners v3", `datePublished` 2026-11-01 (giá trị đúng như trang ghi), danh sách `Syllabus` (tên section + `timeRequired`);
  - dữ liệu curriculum nhúng (`COURSE_SECTION`, `VIDEO_LECTURE`, `ARTICLE_LECTURE`, `QUIZ`) có `title` và `durationInSeconds` cho từng bài, `lectureCount`/`contentLength` cho từng section;
  - dòng thống kê "18 sections • 123 lectures • 8h 20m total length" và "Last updated 11/2026";
  - `courseId=1075642`, giảng viên "Stephane Maarek".
- Thử thêm `GET https://www.udemy.com/api-2.0/courses/1075642/public-curriculum-items/?page_size=200`: nhận trang thử thách Cloudflare "Just a moment...", không có dữ liệu. Không thử `?fields[course]=id` vì đã có id trong HTML.
- Mỗi lecture có trường `aiSummary` do Udemy sinh tự động. Tôi chỉ dùng nó để biết bài nói về chủ đề gì, **không** coi là nội dung giảng viên nói và không dùng làm nguồn kỹ thuật.
- Không xem được video và nội dung quiz (cần mua khoá).

### 1.2 Danh sách 18 section (thứ tự như trang)

| # | Section | Số mục | Thời lượng |
|---|---|---|---|
| 1 | Kafka Introduction | 4 | 14m33s |
| 2 | Code Download | 1 | 0m16s |
| 3 | ====== Kafka Fundamentals ====== | 1 | 0m20s |
| 4 | **Kafka Theory** | 10 + quiz | 45m31s |
| 5 | Starting Kafka | 13 | 47m24s |
| 6 | [Archive] Starting Kafka with Zookeeper + Windows non WSL-2 | 5 | 19m51s |
| 7 | CLI (Command Line Interface) 101 | 9 + quiz | 44m44s |
| 8 | Kafka UI - Conduktor Demo | 1 | 4m40s |
| 9 | Kafka Java Programming 101 | 12 + quiz | 1h16m40s |
| 10 | ===== Kafka Real World Project ===== | 2 | 2m12s |
| 11 | Kafka Wikimedia Producer & Advanced Producer Configurations | 15 + quiz | 1h03m59s |
| 12 | OpenSearch Consumer & Advanced Consumer Configurations | 16 + quiz | 1h05m53s |
| 13 | Kafka Extended APIs for Developers | 9 + quiz | 34m07s |
| 14 | Real World Insights and Case Studies (Big Data / Fast Data) | 8 | 28m17s |
| 15 | Kafka in the Enterprise for Admins | 5 | 19m40s |
| 16 | ===== Advanced Kafka ===== | 1 | 0m14s |
| 17 | Advanced Topics Configurations | 8 | 28m12s |
| 18 | ===== Next Steps ===== | 3 | 4m11s |

(Thời lượng tính từ `contentLength` giây của từng section; tổng số mục là 123, khớp với dòng thống kê của trang.)

### 1.3 Chi tiết section "Kafka Theory" (id 2726988)

| # | Bài (nguyên văn) | Thời lượng | Chủ đề (rút từ `aiSummary` của Udemy, chỉ để định vị) |
|---|---|---|---|
| 1 | Topics, Partitions and Offsets | 6:10 | topic như stream, partition, offset, thứ tự, dữ liệu bất biến |
| 2 | Producers and Message Keys | 7:24 | producer ghi vào partition, key quyết định partition, null key, hàm băm murmur2 |
| 3 | Consumers & Deserialization | 4:01 | consumer kéo dữ liệu, đọc theo offset trong partition, key/value deserializer |
| 4 | Consumer Groups & Consumer Offsets | 7:04 | group chia partition, committed offset, at-least/at-most/exactly-once |
| 5 | Brokers and Topics | 4:28 | broker hợp thành cluster, partition rải trên broker, bootstrap server |
| 6 | Topic Replication | 5:30 | replication factor, leader, ISR, consumer replica fetching |
| 7 | Producer Acknowledgements & Topic Durability | 2:09 | acks=0/1/all, RF và độ bền |
| 8 | Zookeeper | 5:14 | (bỏ qua theo quyết định của người học) |
| 9 | Kafka KRaft - Removing Zookeeper | 1:58 | KIP-500, lịch sử chuyển sang KRaft |
| 10 | Theory Roundup | 1:33 | tổng kết |
| - | Quiz on Theory | (quiz) | không xem được nội dung |

---

## 2. Đối chiếu từng bài của Kafka Theory với bài 1-9

Mức phủ: **Đủ** = khái niệm có trong mục kiến thức kèm nguồn và có thực hành/quiz; **Một phần** = có nhắc nhưng thiếu ý chính; **Chưa** = không có.

| # | Bài Udemy | Workspace phủ ở đâu | Mức | Còn thiếu |
|---|---|---|---|---|
| 1 | Topics, Partitions and Offsets | Bài 1 §2 (log chỉ ghi thêm, không xoá khi đọc), §3 (partition khác replica); Bài 2 §2 (thứ tự trong partition, offset theo partition), §3 (mô phỏng) | Đủ | - |
| 2 | Producers and Message Keys | Bài 2 §2-3 (cùng key cùng partition; có key thì băm, không key thì sticky partition), §4 (key nóng); Bài 8 §1-2 (tăng partition làm đổi mapping) | Một phần | Không nói tên hàm băm (murmur2), nên thiếu bẫy client khác ngôn ngữ băm khác (mục 3.4). Không nói `RoundRobinPartitioner` bỏ qua key (mục 3.4). |
| 3 | Consumers & Deserialization | Bài 1 §2 (pull); Bài 6 §1.6 (`ErrorHandlingDeserializer`); Bài 8 §3 (schema là hợp đồng) | Một phần | Chưa có ý "broker lưu byte; serializer/deserializer ở client; hai bên phải cùng định dạng", và cấu trúc record (key, value, timestamp, headers) (mục 3.2). |
| 4 | Consumer Groups & Consumer Offsets | Bài 1 §3 ("Một group chia partition thế nào?"); Bài 5 §1 (vị trí, commit, `__consumer_offsets`, auto-commit, hết hạn offset); Bài 7 §1.1 (ba ngữ nghĩa giao hàng) | Đủ (sâu hơn khoá) | - |
| 5 | Brokers and Topics | Bài 1 §3 (broker, cluster); Bài 3 (dựng cụm, `--describe`); `bootstrap-server` chỉ xuất hiện trong lệnh | Một phần | Chưa giải thích bootstrap chỉ để khám phá cụm, và `advertised.listeners` quyết định địa chỉ client dùng sau đó (mục 3.1). |
| 6 | Topic Replication | Bài 1 §3; Bài 3 §1.3-1.4 (leader/follower, ISR co giãn theo `replica.lag.time.max.ms`), lab kill broker | Đủ, trừ một ý | Follower fetching (KIP-392, `client.rack`, `replica.selector.class`) chưa có (mục 3.3). |
| 7 | Producer Acknowledgements & Topic Durability | Bài 3 §1 (vì sao minISR=2); Bài 4 §1 (acks=all chờ ISR hiện tại, minISR, unclean election, NotEnoughReplicas*), mô phỏng + lab 04 | Đủ (sâu hơn khoá) | - |
| 8 | Zookeeper | Bỏ theo quyết định của người học | Bỏ | - |
| 9 | Kafka KRaft - Removing Zookeeper | Bài 1 §3 (controller KRaft); Bài 3 §1 (quorum, combined mode) | Đủ cho mission | Phần lịch sử (KIP-500, phiên bản production-ready) không cần (mục 5.3). |
| 10 | Theory Roundup | Bài 1 §5 (bản đồ), `reference/0001-core-concepts`, `summaries/`, Bài 9 phần A | Đủ | - |

---

## 3. Khái niệm chưa phủ hoặc phủ mỏng (trong phạm vi Kafka Theory)

### 3.1 Bootstrap chỉ để khám phá cụm; địa chỉ thật do `advertised.listeners` quyết định — liên quan: **trung bình-cao**

- Docs: `bootstrap.servers` là danh sách host/port "used to establish the initial connection to the Kafka cluster. Clients use this list to bootstrap and discover the full set of Kafka brokers"; nên ghi hơn một server "to ensure resilience if any servers are down"; danh sách "does not need to contain the entire set of brokers". [producer-cfg]
- Docs: `advertised.listeners` là địa chỉ broker "advertise to clients and other brokers", dùng khi `listeners` "does not represent the addresses that clients should use to connect, such as in cloud environments"; không đặt thì lấy giá trị `listeners`. [broker-cfg]
- Lab của workspace đã có sẵn ví dụ: `labs/03-cluster/docker-compose.yml` quảng bá `kafka-1:19092` cho mạng Docker và `localhost:29092` cho máy chủ; `labs/04` dùng `localhost:29092,39092,49092`.
- Lý do liên quan: ngày đầu ở nơi mới người học nhận một chuỗi bootstrap từ devops (MSK trả 3 broker ở các AZ khác nhau; Confluent Cloud là một endpoint `pkc-...`, xem research/0003 §3.4 và research/0004). Hiểu "bootstrap chỉ là cửa vào, sau đó client nói chuyện thẳng với leader của từng partition" giúp đọc lỗi "kết nối được bootstrap nhưng gửi không được" **(suy ra)**.
- Đặt vào: Bài 3 §1 (một mục) + một câu "Câu tự hỏi" ở Bước 2/3: vì sao trong container dùng `kafka-1:19092` mà từ Windows dùng `localhost:29092`.

### 3.2 Cấu trúc record và serializer/deserializer — liên quan: **cao**

- Docs: "Conceptually, an event has a key, value, timestamp, and optional metadata headers." [intro]
- Định dạng record trên đĩa: `key: byte[]`, `value: byte[]`, `Headers => [Header]`; khoá header "guaranteed to be non-null", value của header có thể null; "The order of headers in a record is preserved when producing and consuming." [msg-format]
- Producer phải có `key.serializer`/`value.serializer` (lớp hiện thực `Serializer`); consumer phải có `key.deserializer`/`value.deserializer`. [producer-cfg] [consumer-cfg]
- Timestamp: topic config `message.timestamp.type`, mặc định `CreateTime` (thay được bằng `LogAppendTime`). [topic-cfg] Mức liên quan của ý timestamp: thấp.
- Lý do liên quan: broker chỉ thấy byte, nên định dạng message là hợp đồng giữa producer và consumer (Bài 8 §3 đã nói ở mức thiết kế). Header là chỗ tự nhiên cho `eventId`/trace id (Bài 7 cần `eventId`) và là cơ chế Spring dùng ở DLT (Bài 6) **(suy ra)**. Deserializer sai là nguồn poison pill của Bài 6.
- Đặt vào: Bài 2 §2 (thêm ý 5: "một record gồm..."; broker lưu byte) hoặc hộp nhỏ ở Bài 1 §3. Một câu quiz kiểu "producer đổi từ JSON sang Avro trên cùng topic thì consumer cũ thấy gì?" trả lời được từ mục này **(suy ra)**.

### 3.3 Follower fetching theo rack/AZ (KIP-392) — liên quan: **trung bình**

- KIP-392 (Accepted): động cơ là "there is no easy way to leverage locality in order to reduce expensive cross-dc traffic", ví dụ AWS mỗi AZ coi như một datacenter. Consumer đặt `client.rack`; broker đặt `replica.selector.class`, mặc định trả về leader; lớp có sẵn `RackAwareReplicaSelector` "implements exact matching on the rackId from replicas". [kip-392]
- Đánh đổi theo KIP-392: vẫn chỉ trả dữ liệu đã committed, nhưng vì phải chờ high watermark lan từ leader nên "follower fetching may have higher latency"; có thể gặp "spurious out of range errors" nếu fetch tới replica không in-sync. [kip-392]
- Docs 4.4: `replica.selector.class` "used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader"; `client.rack` của consumer "corresponds with the broker config 'broker.rack'". [broker-cfg] [consumer-cfg]
- Liên quan: `broker.rack` cũng dùng để rải replica qua các rack/AZ ("can also be applied to other broker groupings such as availability zones in EC2"). [ops-basic] [broker-cfg]
- 4.4 có thêm phía producer: `partitioner.rack.aware` (mặc định `false`); khi bật cùng `client.rack`, sticky partition (record không key) được chọn trong các partition có leader cùng rack. [producer-cfg]
- Lý do liên quan: dịch vụ managed trên AWS chạy nhiều AZ; AWS khuyến nghị đặt `client.rack` = AZ ID cho consumer (research/0003 §3.4). Đây là câu "chém" về chi phí/độ trễ, không ảnh hưởng mất/trùng message. Bài 1 §3 hiện viết "Consumer đọc partition, không đọc từng bản sao": đúng về khái niệm, nhưng câu "đọc từ leader" (nếu có ở đâu đó) cần thêm ngoại lệ này **(suy ra)**.
- Đặt vào: Bài 8 §5 (điều khác đi khi managed) một mục ngắn, hoặc `reference/0004`. Không cần lab (cụm lab không có rack).

### 3.4 Hàm băm key và bẫy client đa ngôn ngữ — liên quan: **trung bình** (cao nếu nơi mới có service không viết bằng Java)

- Mã nguồn Apache Kafka 4.4.0, `BuiltInPartitioner.partitionForKey`: `Utils.toPositive(Utils.murmur2(serializedKey)) % numPartitions`. [src-bip]
- Docs 4.4 cho `partitioner.class`: có key thì "choose a partition based on a hash of the key"; không key thì "sticky partition that changes when at least batch.size bytes are produced". Tuỳ chọn `RoundRobinPartitioner` gửi mỗi record sang partition khác "regardless of whether the 'key' is provided or not" (tức **bỏ qua key**, mất thứ tự theo key). [producer-cfg]
- librdkafka: `partitioner` mặc định `consistent_random` = "CRC32 hash of key"; muốn giống Java phải đặt `murmur2_random` ("functionally equivalent to the default partitioner in the Java Producer"). [librdkafka-cfg]
- Hệ quả: một service Java và một service dùng librdkafka cùng ghi một topic với cùng key có thể đưa key đó vào hai partition khác nhau, mất thứ tự theo key mà không có lỗi nào **(suy ra từ hai nguồn trên)**.
- Đặt vào: Bài 2 §4 thêm usecase (c), hoặc `reference/0003-tricky-scenarios`. Kiểm chứng được bằng cụm lab nếu dùng `kcat` (dựa trên librdkafka) so với console producer Java **(suy ra, chưa thử)**.

---

## 4. Bản đồ các section khác của khoá (mức section)

Không được giao đối chiếu chi tiết, nhưng cần cho đề xuất (b) và (c).

| Section Udemy | Workspace | Ghi chú |
|---|---|---|
| Starting Kafka; [Archive] ZooKeeper | Bài 3 (wizard Docker trong WSL2) | Khoá dạy cài binary theo từng hệ điều hành; workspace dùng Docker, đủ. |
| CLI 101 (topics, console producer/consumer, consumer groups, reset offsets) | Bài 3 Bước 2-3, Bài 5 Bước 3, Bài 6 Bước 3 | Đủ. Thiếu `kafka-configs.sh` (khoá đặt ở "Advanced Topics Configurations"), xem 5.1. |
| Kafka UI - Conduktor | - | Sản phẩm của một vendor; không cần. |
| Java Programming 101 (callback, key, graceful shutdown, cooperative rebalance, static membership, auto-commit) | Bài 4 (`CompletableFuture` của `KafkaTemplate`), Bài 5 §1.3, §1.7 | Đủ với Spring Kafka. Graceful shutdown: container của Spring lo; mức thấp. |
| Wikimedia Producer & Advanced Producer Config (acks, retries, idempotent, safe settings, compression, linger/batch, sticky partitioner, `max.block.ms`/`buffer.memory`) | Bài 4, Bài 4b (`lessons/0010-idempotent-producer`), Bài 2 §3 | Thiếu: nén, batching, `max.block.ms`/`buffer.memory` (xem 5.1). |
| OpenSearch Consumer & Advanced Consumer Config (delivery semantics, idempotence, commit strategies, batching, offset reset, replay, internal threads, rack awareness) | Bài 5, Bài 7 | Đủ, trừ rack awareness (3.3). |
| Extended APIs (Connect, Streams, Schema Registry, "Which Kafka API should I use?") | Bài 6 §1.1 (DLQ của Connect), Bài 8 §3 (ranh giới schema) | MISSION: Schema Registry một bài, Connect/Streams tổng quan. NOTES: "Bỏ: ... Connect, Streams". Mâu thuẫn, xem 5.2. |
| Real World Insights (chọn số partition và RF, quy ước tên topic, case study MovieFlix/GetTaxi/MySocialMedia/MyBank/...) | Bài 8 (thẻ thiết kế), Bài 9 (bài toán lạ) | Đủ về số partition/RF. Quy ước tên: thẻ #6 chỉ có câu hỏi; Apache chỉ nêu giới hạn 249 ký tự [ops-basic], quy ước là chuyện của từng tổ chức. |
| Kafka in the Enterprise for Admins (kiến trúc cụm, monitoring, security, MirrorMaker, advertised listeners) | - | Ngoài mission (devops lo, bảo mật hoãn). Chỉ lấy ý advertised listeners (3.1). |
| Advanced Topics Configurations (đổi config topic, segment và index, cleanup policy, compaction, unclean leader election, large messages) | Bài 8 §1.3-1.5, Bài 4 §1 (unclean) | Thiếu: `kafka-configs.sh`, segment (xem 5.1). |

---

## 5. Đề xuất

### 5.1 (a) Nội dung nên bổ sung, theo bài

| Bài | Bổ sung | Liên quan | Nguồn |
|---|---|---|---|
| 2 | Ý 5 ở §2: record = key, value, timestamp, headers; broker lưu byte; serializer/deserializer ở hai đầu phải khớp định dạng. Một câu quiz tương ứng. | Cao | [intro] [msg-format] [producer-cfg] [consumer-cfg] |
| 2 | Usecase (c) ở §4: service Java (murmur2) và service librdkafka (mặc định CRC32) ghi cùng key vào hai partition khác nhau. Thêm một dòng: `RoundRobinPartitioner` bỏ qua key. | Trung bình | [src-bip] [librdkafka-cfg] [producer-cfg] |
| 3 | Mục ở §1: bootstrap chỉ để khám phá cụm; `advertised.listeners` quyết định địa chỉ thật. Câu tự hỏi dùng chính compose của lab (19092 so với 29092). | Trung bình-cao | [producer-cfg] [broker-cfg] |
| 3 hoặc 8 | Lab ngắn: sửa config của topic đã có bằng `kafka-configs.sh --entity-type topics --entity-name <topic> --alter --add-config min.insync.replicas=2` rồi `--describe` lại; `--delete-config` để quay về mặc định. Gắn với tình huống "topic đã tạo với minISR=1" hoặc "đặt `unclean.leader.election.enable=false` trên MSK". | Cao | [ops-basic] |
| 4 (hoặc 4b) | Usecase: cụm không tới được hoặc producer gửi nhanh hơn khả năng giao. `send()` chặn tới `max.block.ms` (mặc định 60000) để chờ metadata và chỗ trong buffer, rồi ném lỗi; `buffer.memory` mặc định 33554432 byte. Nếu `send()` được gọi trên thread xử lý HTTP thì API chậm tới 60 giây **(suy ra)**. Thời gian chặn ở serializer/partitioner không tính vào `max.block.ms`. | Cao | [producer-cfg] |
| 4 (hoặc 8) | Hộp "throughput/độ trễ": `compression.type` mặc định `none`, nén theo cả batch nên batching tốt thì nén tốt; `linger.ms` mặc định 5, `batch.size` mặc định 16384; tăng `linger.ms` lên 50 "add up to 50ms of latency ... in the absence of load". AWS khuyến nghị `linger.ms` 25, `batch.size` 64-128KB (research/0003 §3.4). | Trung bình | [producer-cfg] |
| 8 §1 | Một dòng về segment: "Retention and cleaning is always done a file at a time"; `segment.bytes` mặc định 1 GiB, `segment.ms` mặc định 7 ngày. Ở 4.4, `retention.ms` cũng kích hoạt roll segment mới khi điều kiện retention thoả. Đủ để trả lời "vì sao đặt retention 1 giờ mà dữ liệu không mất đúng 1 giờ". | Thấp-trung bình | [topic-cfg] |
| 8 §5 | Follower fetching: `client.rack` + `replica.selector.class=...RackAwareReplicaSelector` giảm traffic giữa các AZ, đổi lại độ trễ cao hơn. Hỏi platform team cụm đã bật chưa. | Trung bình | [kip-392] [broker-cfg] [consumer-cfg] |

Ghi chú: mọi mục trên đều nhỏ (một mục kiến thức, hoặc một usecase/lab ngắn); không mục nào cần bài mới **(suy ra)**. Theo NOTES.md, mỗi câu quiz mới phải trả lời được từ mục "Kiến thức tối thiểu" của chính bài đó, và bài sửa thì sửa cả `summaries/`.

### 5.2 (b) Có nên cấu trúc lại theo khoá Udemy không?

- **Không sắp lại thứ tự.** Khoá Udemy theo khuôn "lý thuyết 45 phút → cài đặt → CLI → Java → cấu hình nâng cao producer → consumer → API mở rộng → case study → admin → topic nâng cao". Workspace đã theo cùng mạch khái niệm (topic/partition/key → broker/replication → acks → consumer/offset → thiết kế topic), nhưng đan lý thuyết với lab theo `teach` và MISSION ("bức tranh tổng thể trước"). Thứ tự replication (Bài 3) trước acks (Bài 4) cũng là thứ tự của khoá **(suy ra từ thứ tự bài trong mục 1.3)**.
- **Không tách Bài 1-2 thành 7 bài nhỏ như khoá.** Người học là senior và học 2 ngày; Bài 1-2 đã gộp đọc nhanh (NOTES: "Bài 1-2 gộp, đọc nhanh + quiz").
- **Chỗ duy nhất nên cân nhắc:** khoá Udemy đặt "đổi config topic", segment, compaction, unclean election, large message thành một khối "Advanced Topics Configurations". Workspace rải các ý này ở Bài 4 và Bài 8. Việc rải là hợp lý (mỗi config đi với vấn đề nó giải quyết). Chỉ cần thêm `kafka-configs.sh` vào lab, không cần một khối riêng **(suy ra)**.
- **Mâu thuẫn cần người học chốt:** MISSION ghi "Kafka Connect và Kafka Streams: chỉ tổng quan", NOTES ghi "Bỏ: ... Connect, Streams". Nếu giữ theo MISSION: một đoạn "chọn API nào" (client thuần / Connect / Streams) ở Bài 8 hoặc reference/0001 là đủ, nhưng phải `research` trước vì RESOURCES chưa có nguồn về Connect/Streams ngoài DLQ của Connect.
- Schema Registry: MISSION có một bài, Bài 8 ghi chưa có nguồn. Khoá Udemy cũng chỉ có 2 bài (Introduction + Hands On). Nếu làm thì cần `research` riêng (docs Confluent là nguồn chính cho Schema Registry) trước khi viết.

### 5.3 (c) Khoá Udemy có, workspace không nên đưa vào

| Nội dung | Lý do |
|---|---|
| Zookeeper (5:14), [Archive] section ZooKeeper, lịch sử KRaft (KIP-500, phiên bản production-ready) | Người học đã quyết định bỏ; Kafka 4.x không còn chế độ ZooKeeper [upgrade]. NOTES: không đưa lịch sử phiên bản trừ khi nó đổi cách cấu hình hôm nay. |
| "Null key thì round-robin" (theo `aiSummary` của bài "Producers and Message Keys") | Lỗi thời cho client hiện tại: 4.4 dùng sticky partition, đổi partition khi đủ `batch.size` byte [producer-cfg]. Bài 2 đã viết đúng; nếu người học đọc tài liệu cũ thì chỉ cần một dòng "tutorial cũ nói round-robin". |
| Cài Kafka theo từng hệ điều hành, Windows non-WSL2, brew | Workspace dùng Docker trong WSL2; nơi mới dùng managed. |
| Conduktor (UI + Docker stack của vendor) | Công cụ của một vendor, không phải concept. |
| Dự án Wikimedia/OpenSearch | Domain và stack khác (Java client thuần); workspace dùng Spring Kafka và usecase đa lĩnh vực. Các config học được ở đó đã nằm ở 5.1. |
| Kafka in the Enterprise for Admins: monitoring, security, MirrorMaker, kiến trúc cụm | MISSION: vận hành cụm và bảo mật ngoài phạm vi (devops lo). Ngoại lệ là ý advertised listeners (3.1). |
| Case study "Big Data Ingestion", "Logging and Metrics Aggregation" | Xa mission (microservices, không mất message). Case study "MyBank" có thể đọc tham khảo, nhưng Bài 9 đã có bài toán thiết kế riêng. |
| Graceful shutdown bằng `wakeup()` | Với Spring Kafka, container lo việc này; mức thấp. Nếu cần, research/0001 đã ghi `wakeup()` là ngoại lệ thread-safe duy nhất. |

---

## 6. Ghi chú phụ khi đọc bài (không sửa gì)

- `lessons/0003-build-cluster` §1 và `lessons/0004-producer-durability` §1 có các mục đánh số bị trống (ví dụ Bài 3 mục 2 và 6; Bài 4 mục 3, 5-7, 10). Có thể là dấu vết sau khi bỏ ZooKeeper hoặc chuyển nội dung sang Bài 4b, và các file này đang được sửa trong working tree **(suy ra)**.
- Bài 4 trỏ tới "Bài 4b" `lessons/0010-idempotent-producer` (file đã có), nhưng `INDEX.md` chưa liệt kê bài này.

---

## Unverified

- Nội dung thật của các video và quiz "Quiz on Theory": không xem được. Bảng 1.3 chỉ có tên, thời lượng và `aiSummary` do Udemy sinh; không suy ra rằng giảng viên nói đúng như `aiSummary`.
- API công khai của Udemy (`public-curriculum-items`) bị Cloudflare chặn; không đối chiếu được dữ liệu trang với API.
- Phiên bản Kafka phát hành KIP-392: trang KIP không ghi (chỉ trỏ tới KAFKA-8443). Docs 4.4 xác nhận các config tồn tại; chưa xác nhận MSK/Confluent Cloud có bật `RackAwareReplicaSelector` sẵn không (research/0003 chỉ ghi AWS khuyến nghị `client.rack`).
- Các client bọc librdkafka (Go, Python, .NET của Confluent) có dùng mặc định `consistent_random` của librdkafka hay đặt mặc định riêng: chưa kiểm. Chỉ kiểm `CONFIGURATION.md` của librdkafka (nhánh master).
- Tình huống "cùng key, hai partition khác nhau giữa Java và librdkafka" là suy ra từ hai nguồn, chưa chạy thử trên cụm.
- Ý "`send()` trên thread HTTP làm API chậm tới 60 giây" là suy ra từ mô tả `max.block.ms`; `KafkaTemplate.send` của Spring có chặn y như vậy hay không chưa kiểm trong docs Spring.
- Câu "Retention ... triggers the rolling of new segment" trong `retention.ms` của 4.4: chưa tìm KIP tương ứng, chưa biết từ phiên bản nào.

## Sources

| Tag | URL | Dùng cho |
|---|---|---|
| udemy-clp | https://www.udemy.com/course/apache-kafka/ | Curriculum: 18 section, 123 lecture, thời lượng, "Last updated 11/2026", course id 1075642 (truy cập 2026-11-12, HTTP 200) |
| udemy-api | https://www.udemy.com/api-2.0/courses/1075642/public-curriculum-items/?page_size=200 | Đã thử, bị Cloudflare chặn, không có dữ liệu |
| intro | https://kafka.apache.org/intro | event gồm key, value, timestamp, headers |
| msg-format | https://kafka.apache.org/44/implementation/message-format/ | record trên đĩa: key/value `byte[]`, headers, thứ tự header |
| producer-cfg | https://kafka.apache.org/44/configuration/producer-configs/ | `bootstrap.servers`, `key.serializer`, `partitioner.class` (sticky, RoundRobinPartitioner), `partitioner.rack.aware`, `client.rack`, `compression.type`, `linger.ms`, `batch.size`, `max.block.ms`, `buffer.memory` |
| consumer-cfg | https://kafka.apache.org/44/configuration/consumer-configs/ | `client.rack`, `key.deserializer`, `auto.offset.reset` |
| broker-cfg | https://kafka.apache.org/44/configuration/broker-configs/ | `advertised.listeners`, `replica.selector.class`, `broker.rack`, `log.segment.bytes` |
| topic-cfg | https://kafka.apache.org/44/configuration/topic-configs/ | `segment.bytes`, `segment.ms`, `retention.ms`, `message.timestamp.type` |
| ops-basic | https://kafka.apache.org/44/operations/basic-kafka-operations/ | `kafka-configs.sh --alter --add-config/--delete-config`, rack awareness cho replica, giới hạn 249 ký tự tên topic |
| design | https://kafka.apache.org/44/design/design/ | đối chiếu ISR/replication (đã dùng trong Bài 1-4) |
| upgrade | https://kafka.apache.org/44/getting-started/upgrade/ | ZooKeeper bị gỡ ở 4.x (dẫn lại từ research/0001, không fetch lại lần này) |
| kip-392 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica | follower fetching: động cơ, `client.rack`, `replica.selector.class`, `RackAwareReplicaSelector`, độ trễ cao hơn |
| src-bip | https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java | hàm băm key: `murmur2(serializedKey) % numPartitions` (dòng 412 ở tag 4.4.0) |
| librdkafka-cfg | https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md | `partitioner` mặc định `consistent_random` (CRC32); `murmur2_random` tương đương Java |
