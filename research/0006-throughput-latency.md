# 0006 - Quá tải, thông lượng, độ trễ: batching, fetch, partition, lag, backpressure, quota

Ngày: 2026-11-17. Phiên bản đối chiếu: Apache Kafka 4.4 (docs `kafka.apache.org/44/`), Spring for Apache Kafka 4.1.2 (docs stable), Amazon MSK Developer Guide (bản hiện hành ngày đọc).

**Phạm vi:** cấu hình producer ảnh hưởng thông lượng/độ trễ (`batch.size`, `linger.ms`, `compression.type`, `buffer.memory`, `max.in.flight...`), đánh đổi `acks` với độ trễ, cấu hình fetch phía consumer, quan hệ số partition với song song, consumer lag (CLI, metric client, metric MSK), backpressure trong Spring Kafka, client quota và throttling. Không bàn sizing cụm (ngoài scope theo `MISSION.md`).

**Quy ước:** link dạng `[tên]` trỏ tới mục "Sources" cuối file. Câu trong ngoặc kép là trích nguyên văn (đọc từ HTML gốc qua `curl` + `pandoc`, không qua bản tóm tắt). **(suy ra)** = tôi tự ghép từ nguồn, không có câu nguyên văn. Default ghi kèm phiên bản.

**Nguồn đã đọc:** [producer-cfg], [consumer-cfg], [topic-cfg], [broker-cfg], [design], [ops-basic], [monitoring], [multi-tenancy], [upgrade], [intro], [consumer-javadoc], [sk-pause], [sk-pause-part], [sk-container-props], [sk-container], [sk-micrometer], [msk-client-bp], [msk-lag], [msk-metrics], [msk-limits], [msk-bp], [msk-replicator-quota]; báo cáo nội bộ `research/0003-msk.md`, `research/0004-confluent-cloud.md` cho Confluent Cloud (không đọc lại docs Confluent Cloud trong phiên này).

---

## Tóm tắt

1. Batching là đòn bẩy chính: docs gọi batching là "one of the big drivers of efficiency" và nói thẳng nó là cơ chế "to trade off a small amount of additional latency for better throughput" ([design]).
2. **Kafka 4.0 đổi `linger.ms` mặc định từ 0 sang 5**, với lý do "the efficiency gains from larger batches typically result in similar or lower producer latency despite the increased linger" ([producer-cfg], [upgrade]). Tutorial cũ ghi `linger.ms=0` là sai cho client 4.x. AWS còn khuyên tối thiểu 5 ms và "against ever using a value of zero in low latency use cases" ([msk-client-bp]).
3. Nén diễn ra **ở producer, theo cả batch**; broker giải nén để kiểm tra rồi **ghi nguyên dạng nén**, consumer tự giải nén ([design]). Topic mặc định `compression.type=producer` (giữ codec producer chọn) ([topic-cfg]). Producer mặc định `compression.type=none` ([producer-cfg]).
4. `acks=1` chỉ giảm độ trễ **phía producer**: docs nói dù `acks` là gì, message "will not be visible to the consumers" cho tới khi đã sang đủ ISR ([design]). (suy ra) Hạ `acks` không làm consumer thấy dữ liệu sớm hơn.
5. Số partition là **trần song song** của một consumer group: mỗi partition giao cho "exactly one consumer in the group" ([consumer-javadoc]); docs vận hành: "the partition count impacts the maximum parallelism of your consumers" ([ops-basic]). Docs Apache **không** đưa công thức chọn số partition theo thông lượng.
6. Tín hiệu quá tải cốt lõi là **consumer lag**. Hai cách đo khác nhau: `kafka-consumer-groups.sh --describe` tính LAG theo **offset đã commit**, còn metric client `records-lag-max` "is based on current offset and not committed offset" ([ops-basic], [monitoring]).
7. Backpressure trong Spring Kafka: `pause()`/`resume()` của container (vẫn `poll()` nên không rebalance), `pausePartition()`, `pauseImmediate`, `idleBetweenPolls`, `concurrency` ([sk-pause], [sk-pause-part], [sk-container-props], [sk-container]).
8. Quota: Apache mặc định **không giới hạn** ("By default, clients receive an unlimited quota") ([ops-basic]). Khi vượt, broker trả về độ trễ, **mute channel**, client tự dừng gửi; fetch bị throttle trả response **không có dữ liệu** ([design]). Với managed Kafka thì khác: MSK Express và Serverless có trần thông lượng, vượt thì bị throttle ([msk-limits]).
9. Bất ngờ cho môi trường ngân hàng: khi bật SSL, Kafka **không dùng `sendfile` (zero-copy)** ([design]). (suy ra) Cụm TLS/mTLS tốn CPU hơn cho đường đọc; đừng mang số benchmark plaintext sang.

---

## 1. Producer: batching, linger, nén, bộ đệm, in-flight

### 1.1 Cơ chế (design)

- Producer gom record trong bộ nhớ rồi gửi theo batch: "Batching is one of the big drivers of efficiency, and to enable batching the Kafka producer will attempt to accumulate data in memory and to send out larger batches in a single request." Mục đích ghi rõ: "a mechanism to trade off a small amount of additional latency for better throughput" ([design], mục Asynchronous send).
- Lý do sâu: tránh "too many small I/O operations"; nhóm message giúp "amortize the overhead of the network roundtrip"; docs nói tối ưu này "produces orders of magnitude speed up" ([design], mục Efficiency).
- Một request gửi tới broker chứa nhiều batch, "one for each partition with data available to be sent" ([producer-cfg], `batch.size`).

### 1.2 Bảng cấu hình producer (default Kafka 4.4)

| Config | Default 4.4 | Ý nghĩa theo docs | Nguồn |
|---|---|---|---|
| `batch.size` | 16384 (16 KiB) | Kích thước batch tối đa **cho mỗi partition**; "a batch size of zero will disable batching entirely"; batch quá lớn lãng phí bộ nhớ vì "we will always allocate a buffer of the specified batch size". | [producer-cfg] |
| `linger.ms` | **5** (từ 4.0; trước đó 0) | Chờ tối đa bấy nhiêu ms để gom thêm record; đủ `batch.size` thì gửi ngay. Ví dụ docs: `linger.ms=50` giảm số request "but would add up to 50ms of latency to records sent in the absence of load". Ghi chú: "broker backpressure can result in a higher effective linger time than this setting". | [producer-cfg], [upgrade] |
| `compression.type` | `none` | "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression)". Giá trị: `none, gzip, snappy, lz4, zstd`. | [producer-cfg] |
| `compression.gzip.level` / `compression.lz4.level` / `compression.zstd.level` | -1 / 9 / 3 | Mức nén theo codec. | [producer-cfg] |
| `buffer.memory` | 33554432 (32 MiB) | Tổng bộ nhớ đệm chờ gửi. "If records are sent faster than they can be delivered to the server the producer will block for `max.block.ms` after which it will fail with an exception." | [producer-cfg] |
| `max.block.ms` | 60000 | Giới hạn thời gian `send()` bị chặn (chờ metadata + chờ cấp buffer). | [producer-cfg] |
| `max.in.flight.requests.per.connection` | 5 | Số request chưa được ack trên một connection. Idempotence cần giá trị <= 5 "because broker only retains at most 5 batches for each producer". | [producer-cfg] |
| `max.request.size` | 1048576 | Giới hạn request, cũng là "a cap on the maximum uncompressed record batch size". | [producer-cfg] |
| `partitioner.adaptive.partitioning.enable` | true | Producer "will try to adapt to broker performance and produce more messages to partitions hosted on faster brokers" (chỉ khi không dùng partitioner tự viết). | [producer-cfg] |

**Điểm dạy:**
- `buffer.memory` đầy là **backpressure phía producer**: `send()` chặn thread gọi tới `max.block.ms` rồi ném exception ([producer-cfg]). (suy ra) Trong Spring, thread HTTP gọi `KafkaTemplate.send()` có thể bị treo tới 60 s khi broker chậm hoặc bị throttle.
- Metric phát hiện: `bufferpool-wait-ratio`, `waiting-threads` ("The number of user threads blocked waiting for buffer memory to enqueue their records"), `buffer-available-bytes`, `record-queue-time-avg/max`, `batch-size-avg`, `records-per-request-avg`, `compression-rate-avg` ([monitoring]).
- Khuyến nghị của AWS cho client MSK (khuyến nghị nhà cung cấp, không phải default Apache): `linger.ms` tối thiểu 5, "higher value of 25ms, for most use cases"; `batch.size` 64 KB hoặc 128 KB; `buffer.memory` 64 MB; nén `lz4` hoặc `zstd` khi mạng có độ trễ cao ([msk-client-bp]).

### 1.3 Nén xảy ra ở đâu

- Producer nén **cả batch**; "The broker decompresses the batch in order to validate it"; batch "will remain compressed in the log and it will also be transmitted to the consumer in compressed form. The consumer decompresses any compressed data that it receives." ([design], mục End-to-end Batch Compression).
- Topic/broker `compression.type` mặc định `producer`: "retain the original compression codec set by the producer"; có thể đặt codec khác hoặc `uncompressed` ("Specify the final compression type for a given topic") ([topic-cfg], [broker-cfg]).
- (suy ra) Nếu topic đặt codec khác codec producer dùng, broker phải nén lại, tốn CPU broker. Docs không mô tả chi tiết bước này -> xem mục Chưa kiểm chứng.
- Giới hạn kích thước ở broker/topic tính trên batch **sau nén**: `max.request.size` ghi "the server has its own cap on the record batch size (after compression if compression is enabled)" ([producer-cfg]).

### 1.4 `acks` và độ trễ

- `acks=0`: không chờ; "the retries configuration will not take effect"; offset trả về luôn -1. `acks=1`: chỉ leader ghi; mất nếu leader chết trước khi follower chép. `acks=all`: chờ đủ ISR; "This is the strongest available guarantee." Default 4.4: `all` ([producer-cfg]).
- Docs design: chờ commit "can take on the order of 10 ms" ([design], mục Message Delivery Semantics).
- **Quan trọng:** "Regardless of the acks setting, the messages will not be visible to the consumers until" (1) đã chép sang mọi replica trong ISR và (2) số ISR không nhỏ hơn `min.insync.replicas` ([design], mục Replicated Logs). (suy ra) `acks=1` chỉ rút ngắn thời gian producer chờ, **không** rút ngắn độ trễ end-to-end tới consumer, nhưng lại mở cửa mất dữ liệu. Với yêu cầu "không mất message" của ngân hàng, hạ `acks` gần như không có lợi.
- Replication theo ISR: "the latency is determined by the faster follower not the slower one" khi RF=3 (đoạn so sánh với majority vote) ([design]). (Lưu ý: câu này nằm trong đoạn nói về ưu điểm của majority vote; với ISR, docs nói commit cần mọi ISR -> follower chậm làm chậm tới khi bị loại khỏi ISR sau `replica.lag.time.max.ms` (30000, [broker-cfg]). Xem Chưa kiểm chứng.)

---

## 2. Consumer: fetch và poll

| Config | Default 4.4 | Ý nghĩa theo docs | Nguồn |
|---|---|---|---|
| `fetch.min.bytes` | 1 | Broker chờ đủ bấy nhiêu byte mới trả. Tăng lên "can improve server throughput a bit at the cost of some additional latency". | [consumer-cfg] |
| `fetch.max.wait.ms` | 500 | Thời gian broker chờ tối đa khi chưa đủ `fetch.min.bytes`. "This config is used only for local log fetch"; dữ liệu ở tiered storage dùng `remote.fetch.max.wait.ms` (broker, 500). | [consumer-cfg], [broker-cfg] |
| `max.partition.fetch.bytes` | 1048576 (1 MiB) | Trần dữ liệu **mỗi partition** mỗi fetch; batch đầu lớn hơn vẫn được trả "to ensure that the consumer can make progress". | [consumer-cfg] |
| `fetch.max.bytes` | 52428800 (50 MiB) | Trần cả request fetch, "not a absolute maximum"; "the consumer performs multiple fetches in parallel". | [consumer-cfg] |
| `max.poll.records` | 500 | Số record tối đa mỗi lần `poll()`. "does not impact the underlying fetching behavior. The consumer will cache the records from each fetch request and returns them incrementally from each poll." | [consumer-cfg] |
| `max.poll.interval.ms` | 300000 | Quá thời gian giữa hai lần `poll()` thì consumer bị coi là chết, group rebalance. | [consumer-cfg] |

**Điểm dạy:**
- Long poll là thiết kế có chủ đích: pull giúp "the consumer simply falls behind and catches up when it can"; tham số trong request cho phép "block in a 'long poll' waiting until data arrives (and optionally waiting until a given number of bytes is available...)" ([design], mục Push vs. pull).
- Hai núm cho hai việc khác nhau: `fetch.*` điều khiển **mạng/broker**; `max.poll.records` + `max.poll.interval.ms` điều khiển **vòng xử lý** của ứng dụng. Javadoc: giảm `max.poll.records` để "predict the maximum that must be handled within each poll interval" ([consumer-javadoc]).
- Xử lý lâu và không đều: javadoc khuyên chuyển xử lý sang thread khác, tắt auto commit, commit sau khi xử lý xong, và "pause the partition so that no new records are received from poll until after thread has finished" ([consumer-javadoc], mục Detecting Consumer Failures).
- AWS (khuyến nghị cho MSK): `fetch.min.bytes` "higher value of 128 bytes for most use cases", `fetch.max.wait.ms` 1000 ms ([msk-client-bp]).

---

## 3. Partition và song song

- Giao partition trong group: "each partition is assigned to exactly one consumer in the group. So if there is a topic with four partitions, and a consumer group with two processes, each process would consume from two partitions." ([consumer-javadoc]).
- Docs vận hành: "First each partition must fit entirely on a single server. So if you have 20 partitions the full data set (and read and write load) will be handled by no more than 20 servers (not counting replicas). Finally the partition count impacts the maximum parallelism of your consumers." ([ops-basic], mục Adding and removing topics).
- Intro: partition cho phép client "both read and write the data from/to many brokers at the same time" ([intro]).
- (suy ra) Consumer thứ N+1 trong group có N partition sẽ ngồi không. Docs Spring Kafka xác nhận tình huống này với nhiều topic: 3 topic x 5 partition, `concurrency=15` mà dùng `RangeAssignor` thì chỉ 5 consumer có việc, "with the other 10 consumers being idle" ([sk-container]). Với manual assignment, Spring tự hạ `concurrency` xuống bằng số partition ([sk-container]).
- Thêm partition có cái giá: docs cảnh báo `auto.offset.reset=latest` khi thêm partition "may cause silent message loss" và có "Metadata Propagation Delay" có thể gây "uneven distribution of messages or consumer lag" ([ops-basic]); key có thể đổi partition (đã ghi ở RESOURCES.md).
- Docs Apache **không** đưa số partition tối đa trên broker hay công thức "thông lượng mục tiêu / thông lượng mỗi partition". Số cụ thể chỉ có ở nhà cung cấp:
  - MSK Standard: khuyến nghị partition (gồm cả replica) mỗi broker, ví dụ 1000 cho `kafka.m5.large`/`m7g.large`, 4000 cho `m5.4xlarge` trở lên ([msk-bp]).
  - MSK Standard: "Throughput per-partition: Max 15 MB/s" ([msk-limits]).
  - MSK Serverless: ingress tối đa 5 MBps, egress 10 MBps mỗi partition, vượt thì "Slowdown with throttle duration in response" ([msk-limits]).
  - AWS: "We recommend the number of consumers be at least equal to the number of partitions for better parallelism and resiliency" ([msk-client-bp]).
- Ngoại lệ đã ghi ở `research/0001`: Share Groups (GA 4.2) cho phép nhiều consumer hơn số partition, đổi lại không giữ thứ tự giữa các batch.

---

## 4. Consumer lag: tín hiệu quá tải và cách quan sát

### 4.1 Định nghĩa và cảnh báo

- Pull model: consumer chậm thì "simply falls behind and catches up when it can" ([design]). Lag tăng đều = tốc độ tiêu thụ < tốc độ ghi. (suy ra) Đây là cách Kafka "hấp thụ" quá tải: dữ liệu nằm chờ trên đĩa trong giới hạn retention, không đẩy ngược về producer.
- (suy ra, nối với `research/0007`) Lag vượt retention = mất dữ liệu chưa đọc: docs gọi `retention.ms` là "an SLA on how soon consumers must read their data" ([topic-cfg]).

### 4.2 Ba cách đo

| Cách | Đo gì | Lưu ý | Nguồn |
|---|---|---|---|
| `kafka-consumer-groups.sh --bootstrap-server ... --describe --group my-group` | Cột `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG` theo partition, kèm `CONSUMER-ID`, `HOST`, `CLIENT-ID` | (suy ra) `CURRENT-OFFSET` là offset đã commit của group, nên LAG gồm cả phần đã xử lý mà chưa commit. | [ops-basic] |
| Metric client `records-lag-max` (mức client), `records-lag`, `records-lag-avg`, `records-lag-max` (mức partition) trong `kafka.consumer:type=consumer-fetch-manager-metrics` | Lag theo **vị trí hiện tại** của consumer | "NOTE: This is based on current offset and not committed offset"; "Published by the consumer, not broker." Với `read_committed`, lag tính theo LSO. | [monitoring], [consumer-javadoc] |
| API `KafkaConsumer.currentLag(TopicPartition)` | Lag của một partition, từ metadata cache | Trả `OptionalLong` rỗng nếu chưa biết; không chờ mạng. | [consumer-javadoc] |

- Spring Kafka: không có metric lag riêng; dùng `MicrometerConsumerListener`/`MicrometerProducerListener` để bind `KafkaClientMetrics` của Micrometer vào `MeterRegistry` (từ 2.5) ([sk-micrometer]). (suy ra) `records-lag-max` hiện qua Micrometer.

### 4.3 Managed Kafka

- **MSK:** metric CloudWatch/Prometheus `EstimatedMaxTimeLag`, `EstimatedTimeLag`, `MaxOffsetLag`, `OffsetLag`, `SumOffsetLag` ([msk-lag]). `EstimatedMaxTimeLag` = "Time estimate (in seconds) to drain MaxOffsetLag" ([msk-metrics]). Bẫy: chỉ phát khi group STABLE hoặc EMPTY; không phát nếu tên group có dấu `:`, chưa có offset commit, hoặc tên group có ký tự non-ASCII ("CloudWatch drops the consumer lag metrics") ([msk-lag]).
- **Confluent Cloud:** theo `research/0004`: Console > Clients > Consumer Lag, Metrics API `io.confluent.kafka.server/consumer_lag_offsets`; consumer dùng `assign()` không báo lag. Không đọc lại trong phiên này.
- AWS khuyên theo dõi phía client: `records-lag-max`, `records-consumed-rate`, `fetch-rate`, `poll-rate`, `rebalance-latency-avg`, `commit-rate` ([msk-client-bp]).

---

## 5. Backpressure trong Spring Kafka

| Cơ chế | Hành vi theo docs | Nguồn |
|---|---|---|
| `container.pause()` / `resume()` | "A pause() takes effect just before the next poll(); a resume() takes effect just after the current poll() returns. When a container is paused, it continues to poll() the consumer, avoiding a rebalance if group management is being used, but it does not retrieve any records." Lấy container qua `KafkaListenerEndpointRegistry.getListenerContainer(id)`. Phát `ConsumerPausedEvent`/`ConsumerResumedEvent`. | [sk-pause] |
| `pausePartition(tp)` / `resumePartition(tp)` (từ 2.7) | Tạm dừng một partition; `isPartitionPaused()` cho biết đã dừng thật. | [sk-pause-part] |
| `pauseImmediate` (default `false`, từ 2.9) | `true`: dừng sau record hiện tại; phần còn lại "retained in memory and will be passed to the listener when the container is resumed". Mặc định dừng sau khi xử lý hết batch của lần poll trước. | [sk-container-props], [sk-pause] |
| `pollTimeoutWhilePaused` (default 100 ms) | Timeout `poll()` khi đang pause. | [sk-container-props] |
| `idleBetweenPolls` (default 0) | "Used to slow down deliveries by sleeping the thread between polls"; thời gian xử lý batch + giá trị này phải nhỏ hơn `max.poll.interval.ms`. | [sk-container-props] |
| `concurrency` | `setConcurrency(3)` tạo ba `KafkaMessageListenerContainer`; không vượt được số partition một cách có ích (mục 3). | [sk-container] |
| `KafkaConsumer.pause/resume` (Apache) | "does not cause a group rebalance"; trạng thái pause giữ qua rebalance cho partition vẫn còn được giao (cooperative assignor hoặc KIP-848), mất với eager assignor. | [consumer-javadoc] |

(suy ra) Mẫu dùng: listener ghi xuống hệ thống chậm (core banking, DB) thì pause khi downstream báo quá tải (circuit breaker mở) rồi resume khi hồi phục; lag tăng trong lúc đó là chủ đích, không phải lỗi. Docs không mô tả mẫu circuit breaker này cho Spring Kafka; AWS chỉ khuyên "circuit breaker or a sleep with exponential back-off" để tránh crash gây "excessive rebalancing" ([msk-client-bp]).

---

## 6. Client quota và throttling

### 6.1 Apache Kafka

- Hai loại: "Network bandwidth quotas define byte-rate thresholds (since 0.9)" và "Request rate quotas define CPU utilization thresholds as a percentage of network and I/O threads (since 0.11)" ([design], mục Quotas).
- Áp theo nhóm (user, client-id), user, hoặc client-id; "the most specific quota matching the connection is applied"; mọi kết nối cùng nhóm **dùng chung** quota ([design]).
- **Quota tính theo từng broker**: "Each group of clients can publish/fetch a maximum of X bytes/sec per broker before clients are throttled." ([design]).
- Tên config quota (đã kiểm, giải quyết mục Unverified của `research/0001`): `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, đặt bằng `kafka-configs.sh --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 [--entity-type clients --entity-name clientA]`; `--entity-default` cho mặc định ([ops-basic], mục Setting quotas).
- Mặc định: "By default, clients receive an unlimited quota." ([ops-basic]).
- Cửa sổ đo: broker `quota.window.num`=11, `quota.window.size.seconds`=1 ([broker-cfg]).

### 6.2 Vượt quota thì sao

- "the broker first computes the amount of delay needed to bring the violating client under its quota and returns a response with the delay immediately. In case of a fetch request, the response will not contain any data. Then, the broker mutes the channel to the client ... until the delay is over." Client nhận delay khác 0 cũng "refrain from sending further requests to the broker during the delay" ([design], mục Enforcement).
- (suy ra) Throttle **không** trả lỗi; nó làm chậm. Producer bị chậm -> `buffer.memory` đầy -> `send()` chặn tới `max.block.ms` -> exception ở ứng dụng. Consumer bị chậm -> lag tăng.
- Metric: client `produce-throttle-time-avg/max`, `fetch-throttle-time-avg/max` ([monitoring]); broker `kafka.server:type={Produce|Fetch},user=...,client-id=...` thuộc tính `throttle-time` ("Ideally = 0") và `byte-rate` ([monitoring]).

### 6.3 Managed Kafka

- **MSK** hỗ trợ quota Kafka qua `kafka-configs.sh` (ví dụ chính thức cho MSK Replicator: `consumer_byte_rate` với entity là ARN assumed-role khi dùng IAM) ([msk-replicator-quota]). Metric CloudWatch: `ProduceThrottleTime`, `ProduceThrottleByteRate`, `FetchThrottleTime`, `RequestThrottleTime`... ([msk-metrics]).
- **MSK Express**: bảng "Sustained performance" và "Maximum quota" theo cỡ broker, ví dụ `express.m7g.large` ingress 15.6 / tối đa 23.4 MBps; "Maximum quota is the threshold at which your cluster will throttle read/write traffic" ([msk-limits]).
- **MSK Serverless**: ingress 200 MBps, egress 400 MBps mỗi cụm; vượt thì "Slowdown with throttle duration in response" ([msk-limits]).
- **Confluent Cloud:** theo `research/0004`: client quota chỉ có ở Enterprise, Freight, Dedicated; giới hạn theo eCKU có thể bị throttle.

---

## 7. Điểm phụ đáng biết

- **SSL tắt zero-copy:** "TLS/SSL libraries operate at the user space (in-kernel SSL_sendfile is currently not supported by Kafka). Due to this restriction, sendfile is not used when SSL is enabled." ([design], mục Efficiency). Liên quan trực tiếp nếu ngân hàng dùng TLS/mTLS.
- Pagecache: khi consumer theo kịp, "you will see no read activity on the disks whatsoever as they will be serving data entirely from cache" ([design]). (suy ra) Consumer lag lớn (đọc dữ liệu cũ) làm broker phải đọc đĩa, có thể ảnh hưởng consumer khác.
- Kết nối: AWS cảnh báo tạo quá nhiều kết nối cùng lúc gây "cascading failure"; khuyên jitter khi khởi tạo/đóng client và tạo client "only once per application in a singleton pattern" ([msk-client-bp]).

---

## 8. Usecase gợi ý (chỉ dựa trên điều docs hỗ trợ)

### U1. Market data ticks: độ trễ thấp, khối lượng lớn, được phép bỏ dữ liệu cũ
- Bối cảnh: topic giá chứng khoán, key = mã chứng khoán (thứ tự theo mã nhờ key -> partition, [intro]).
- Producer: giữ `linger.ms=5` mặc định (docs 4.0 nói thường không làm tăng độ trễ, [producer-cfg]); bật `compression.type=lz4` hoặc `zstd` khi gom batch tốt ([producer-cfg], [msk-client-bp]).
- Consumer hiển thị giá: tụt quá xa thì nhảy tới cuối. Javadoc mô tả đúng trường hợp này: "for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not attempt to catch up processing all records, but rather just skip to the most recent records" ([consumer-javadoc]) -> `seekToEnd` (Spring: `ConsumerSeekCallback.seekToEnd`, xem `research/0007`).
- Câu hỏi "oái oăm": một mã (ví dụ mã vốn hoá lớn) chiếm phần lớn tick -> một partition nóng; thêm consumer không giúp vì một partition chỉ có một consumer trong group ([consumer-javadoc]). (suy ra) Chỉ tách key (ví dụ mã + loại dữ liệu) hoặc giảm khối lượng mới giải quyết, đổi lại mất thứ tự giữa các key con.

### U2. Order flow: không mất, giữ thứ tự theo tài khoản, chịu được đợt cao điểm (mở cửa phiên)
- `acks=all` + idempotence (default 4.x) + `min.insync.replicas=2` ([producer-cfg], [design]). Không hạ `acks` để "nhanh hơn": consumer vẫn chỉ thấy message khi đã đủ ISR ([design]).
- Đợt cao điểm: lag tăng là bình thường; cảnh báo theo `EstimatedMaxTimeLag` (MSK) thay vì offset tuyệt đối, vì nó đo thời gian để đọc hết ([msk-metrics]).
- Hệ thống khớp lệnh phía sau chậm: Spring `pause()` container (không rebalance, [sk-pause]), hoặc `pausePartition` cho partition gặp sự cố ([sk-pause-part]).
- Câu hỏi "oái oăm": tăng `max.poll.records` để "xử lý nhiều hơn" nhưng mỗi record gọi API ngoài 200 ms -> 500 x 200 ms = 100 s, còn dưới `max.poll.interval.ms`=300 s; nếu API chậm lên 700 ms thì vượt -> rebalance lặp lại (suy ra từ [consumer-cfg], [consumer-javadoc]).

### U3. Nhiều team dùng chung cụm, một team replay làm chậm cả cụm
- Một consumer group đọc lại cả tuần dữ liệu (đọc đĩa vì không còn trong pagecache, [design]) và kéo hết băng thông.
- Giải pháp có trong docs: `consumer_byte_rate` cho principal của team đó, theo từng broker ([design], [ops-basic]); quan sát `throttle-time` để biết quota đang có tác dụng ([monitoring]). Trên MSK cần quyền chạy `kafka-configs.sh` (thường platform team giữ) ([msk-replicator-quota]).

---

## Điểm nên dạy (80/20)

1. Batching (`batch.size` theo partition + `linger.ms`) là đòn bẩy thông lượng chính; default `linger.ms` đã là **5** từ Kafka 4.0, tutorial ghi 0 là cũ.
2. Nén là việc của producer theo batch; topic mặc định `compression.type=producer` giữ nguyên; broker lưu và trả dạng nén. Batch to hơn thì nén tốt hơn.
3. `acks=1` không làm consumer thấy dữ liệu sớm hơn (message chỉ hiện khi đủ ISR), chỉ đổi rủi ro mất dữ liệu lấy độ trễ phía producer.
4. Số partition = trần song song của một consumer group; thừa consumer thì ngồi không; partition nóng không giải được bằng cách thêm consumer.
5. Lag là tín hiệu quá tải số 1. Phân biệt LAG của CLI (theo offset đã commit) với `records-lag-max` (theo vị trí hiện tại); trên MSK dùng `EstimatedMaxTimeLag` và nhớ các điều kiện mà metric biến mất.
6. Backpressure: phía producer là `buffer.memory` + `max.block.ms` (chặn thread gọi `send()`); phía consumer Spring là `pause()/resume()`, `pausePartition`, `idleBetweenPolls`, giữ thời gian xử lý mỗi poll dưới `max.poll.interval.ms`.
7. Quota Apache mặc định không giới hạn, tính theo từng broker, vượt thì bị **làm chậm** (mute channel, fetch rỗng) chứ không báo lỗi; MSK Express/Serverless có trần cứng.
8. Bật SSL thì mất zero-copy `sendfile`: đừng so số liệu benchmark plaintext với cụm TLS của ngân hàng.

## Chưa kiểm chứng / mâu thuẫn

1. Broker nén lại batch khi topic `compression.type` khác codec của producer: suy ra từ chữ "final compression type"; docs 4.4 không mô tả bước nén lại và chi phí CPU.
2. Câu "the latency is determined by the faster follower not the slower one" trong [design] thuộc đoạn nói về majority vote; với ISR docs nói commit cần mọi replica trong ISR. Mức ảnh hưởng của follower chậm tới độ trễ `acks=all` trước khi bị loại khỏi ISR chưa có câu trực tiếp.
3. LAG trong `kafka-consumer-groups.sh --describe` tính theo offset đã commit: suy ra từ tên cột `CURRENT-OFFSET` và mục "Checking consumer position"; docs không định nghĩa công thức.
4. Spring Boot 4.1.2 có tự bind `KafkaClientMetrics` qua Micrometer không (không cần tự thêm `MicrometerConsumerListener`): docs Spring Kafka chỉ mô tả cách thêm listener thủ công; chưa đọc docs Spring Boot về metrics Kafka.
5. Mẫu "pause khi downstream quá tải" với circuit breaker: không có docs Spring Kafka mô tả; là pattern tự dựng.
6. MSK Provisioned Standard: docs chỉ có ví dụ quota cho MSK Replicator; chưa thấy trang hướng dẫn quota cho ứng dụng thường (cơ chế Apache vẫn áp dụng nhưng chưa có câu chính thức của AWS cho trường hợp này, nhất là khi dùng IAM).
7. Confluent Cloud lag và quota: lấy lại từ `research/0004`, không đọc lại docs trong phiên này.
8. Số "Max 15 MB/s" mỗi partition của MSK Standard: không rõ là giới hạn cứng hay khuyến nghị (bảng ghi "Max" nhưng không ghi hậu quả khi vượt).

---

## Sources

- [producer-cfg] https://kafka.apache.org/44/configuration/producer-configs/ - `batch.size`, `linger.ms`, `compression.type`, mức nén, `buffer.memory`, `max.block.ms`, `max.in.flight...`, `acks`, `max.request.size`, partitioner
- [consumer-cfg] https://kafka.apache.org/44/configuration/consumer-configs/ - `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`, `fetch.max.bytes`, `max.poll.records`, `max.poll.interval.ms`
- [topic-cfg] https://kafka.apache.org/44/configuration/topic-configs/ - topic `compression.type`, `retention.ms` là SLA đọc
- [broker-cfg] https://kafka.apache.org/44/configuration/broker-configs/ - broker `compression.type`, `quota.window.*`, `remote.fetch.max.wait.ms`, `replica.lag.time.max.ms`
- [design] https://kafka.apache.org/44/design/design/ - Efficiency (batching, sendfile, SSL), End-to-end Batch Compression, Asynchronous send, Push vs. pull, Replicated Logs (visibility), Quotas (Enforcement)
- [ops-basic] https://kafka.apache.org/44/operations/basic-kafka-operations/ - partition count và parallelism, `kafka-consumer-groups.sh --describe`, Setting quotas, thêm partition
- [monitoring] https://kafka.apache.org/44/operations/monitoring/ - `records-lag-max`, metric producer buffer/batch/throttle, metric quota broker
- [multi-tenancy] https://kafka.apache.org/44/operations/multi-tenancy/ - quota trong cụm nhiều tenant
- [upgrade] https://kafka.apache.org/44/getting-started/upgrade/ - `linger.ms` 0 -> 5 ở 4.0
- [intro] https://kafka.apache.org/44/getting-started/introduction/ - partition và scalability, key -> partition
- [consumer-javadoc] https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html - consumer group, poll loop, Consumption Flow Control, `pause`, `currentLag`, skip to latest
- [sk-pause] https://docs.spring.io/spring-kafka/reference/kafka/pause-resume.html - pause/resume container
- [sk-pause-part] https://docs.spring.io/spring-kafka/reference/kafka/pause-resume-partitions.html - pause/resume từng partition
- [sk-container-props] https://docs.spring.io/spring-kafka/reference/kafka/container-props.html - `pauseImmediate`, `idleBetweenPolls`, `pollTimeoutWhilePaused`
- [sk-container] https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html - `concurrency`, consumer idle với `RangeAssignor`
- [sk-micrometer] https://docs.spring.io/spring-kafka/reference/kafka/micrometer.html - `MicrometerConsumerListener`, `KafkaClientMetrics`
- [msk-client-bp] https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html - khuyến nghị linger/batch/buffer/compression/fetch, số consumer, metric client, kết nối
- [msk-lag] https://docs.aws.amazon.com/msk/latest/developerguide/consumer-lag.html - metric lag MSK và điều kiện phát
- [msk-metrics] https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html - `EstimatedMaxTimeLag`, metric throttle
- [msk-limits] https://docs.aws.amazon.com/msk/latest/developerguide/limits.html - throughput mỗi partition, Express throttle, Serverless quota
- [msk-bp] https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html - partition mỗi broker Standard
- [msk-replicator-quota] https://docs.aws.amazon.com/msk/latest/developerguide/msk-replicator-bp-quotas.html - quota Kafka trên MSK với IAM principal
