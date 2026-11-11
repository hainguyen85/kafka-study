# 0004 - Confluent Cloud (AWS): loại cluster và điểm khác Apache Kafka

Ngày tra cứu: 2026-11-11. Chỉ dùng tài liệu chính thức (docs.confluent.io, kafka.apache.org/44, docs.spring.io). Tất cả trang tài liệu rất dài; WebFetch chỉ đọc được ~100k ký tự đầu của mỗi trang, phần không đọc được ghi ở "Unverified".

## Tóm tắt (đọc 1 phút)

- 5 loại cluster: Basic (dev/test, SLA 99.5%), Standard (public network), Enterprise (private network), Dedicated (CKU, tự scale), Freight (S3, latency cỡ giây) - [cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html).
- Với ngân hàng (cần private networking) nhiều khả năng là Enterprise hoặc Dedicated: Basic/Standard không có private networking ([regions - feature table](https://docs.confluent.io/cloud/current/get-started/regions.html)).
- Confluent Cloud **không công bố "phiên bản Kafka" của broker**; chỉ công bố baseline protocol client là Kafka 2.1.0 ([deprecation](https://docs.confluent.io/cloud/current/client-apps/deprecate-how-to.html)) và liệt kê tính năng từ Kafka 4.4 mà Cloud đã hỗ trợ ([release notes](https://docs.confluent.io/cloud/current/release-notes/index.html)).
- Trên Basic/Standard/Enterprise/Freight **không sửa được broker config**; chỉ sửa topic config trong giới hạn. `min.insync.replicas` chỉ nhận 1 hoặc 2 (mặc định 2), RF cố định 3, `unclean.leader.election.enable=false` không đổi được ([topic config](https://docs.confluent.io/cloud/current/topics/manage.html)).
- Multi-zone: RF 3 trải 3 AZ, min ISR 2; mất 1 AZ vẫn đọc/ghi, mất 2 AZ mất khả năng ghi ([resilience](https://docs.confluent.io/cloud/current/clusters/resilience.html)).
- Cloud nâng cấp bằng rolling restart toàn bộ broker, client phải retry (producer mặc định ~2 phút) ([client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)).
- Kết nối: `SASL_SSL` + `PLAIN` + API key/secret (hoặc `OAUTHBEARER`), bootstrap dạng `pkc-xxxxx.<region>.aws.confluent.cloud:9092`.
- Có KIP-848 (từ 2025-04-28), share groups chỉ Dedicated/Enterprise và còn preview ở client; transactions/idempotent producer hỗ trợ trên cả 5 loại (Freight mới thêm 2026-10-02).
- Schema Registry nằm trong Stream Governance; Essentials mặc định, BACKWARD là compatibility mặc định.
- Điểm dễ sai nhất khi giữ tư duy Apache Kafka thuần: xem mục 7.

---

## 1. Loại cluster (mức tổng quan)

Nguồn bảng: [Kafka Cluster Types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html).

| | Basic | Standard | Enterprise | Dedicated | Freight |
|---|---|---|---|---|---|
| Dùng cho | dev/test | production, **public** network | production, **private** network | production, throughput cao, private | chi phí thấp, chịu latency cao |
| SLA | 99.5% | 99.9%; 99.99% với >= 2 eCKU | 99.9%; 99.99% với >= 2 eCKU | 99.95% single-zone; 99.99% multi-zone (>= 2 CKU) | 99.99% |
| Capacity | 1-50 eCKU | 1-10 eCKU | 1-32 eCKU | CKU, AWS max 252 | 2-152 eCKU |
| Partition max (trước replication) | 1.500 | 2.500 | 96.000 | 100.000 | 50.000 |
| Partition nén (compacted) max | 1.500 | 2.500 | 32.000 | 100.000 | 50.000 |
| Ingress / egress max | 250 / 750 MBps | 250 / 750 MBps | 1.920 / 5.760 MBps | 9.120 / 27.360 MBps | 9.120 / 27.360 MBps |
| Storage | 5 TB / partition | không giới hạn / partition | không giới hạn | không giới hạn | không giới hạn |
| Networking | (public) | public | PrivateLink hoặc PNI | private | chỉ vài region AWS |
| Connections tối đa | 1.000 | 10.000 | 576.000 | 2.736.000 | 2.736.000 |
| Requests/giây tối đa | 5.000 | 15.000 | 240.000 | 2.280.000 | 2.280.000 |

Ghi chú quan trọng:
- Enterprise + PrivateLink trên AWS giới hạn **10 eCKU**; cần PNI để lên 32 eCKU ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
- Basic nâng lên Standard với zero downtime; chiều ngược lại và các chuyển đổi khác phải migrate sang cluster mới ([cluster FAQ](https://docs.confluent.io/cloud/current/clusters/cluster-faq.html)).
- Basic không có RBAC cho resource trong cluster Kafka (chỉ ACL) ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
- Freight: ghi thẳng object storage, latency "cỡ giây thay vì mili-giây", không có ksqlDB/schema validation/public network; không hỗ trợ share consumers ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html), [regions](https://docs.confluent.io/cloud/current/get-started/regions.html)).
- Với Basic/Standard/Enterprise/Freight, giới hạn lấy theo eCKU tối đa; Confluent "chưa luôn thực thi nghiêm" giới hạn request/connection, vượt có thể bị throttle ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
- Client quotas: chỉ Enterprise, Freight, Dedicated ([cluster FAQ](https://docs.confluent.io/cloud/current/clusters/cluster-faq.html)).
- Dedicated: zone availability **không đổi được sau khi tạo** ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
- Retention theo loại cluster: trang cluster types **không** nêu; retention đặt theo topic (mục 2). Cluster tính theo storage tổng ([topics FAQ](https://docs.confluent.io/cloud/current/topics/topics-faq.html)).

Multi-zone của Basic/Standard/Enterprise: tài liệu **không nói thẳng**. Khi tạo cluster bằng API, org tạo từ 2024-04-16 chọn `availability` = `Low` (SLA 99.5%/99.9%) hoặc `High` (99.99%); org cũ chọn `SINGLE_ZONE`/`MULTI_ZONE`; CLI ví dụ Basic hiển thị "single-zone" ([create cluster](https://docs.confluent.io/cloud/current/clusters/create-cluster.html)). Xem câu hỏi #1 ở mục 7.

## 2. Tương thích Kafka và điểm bị hạn chế

### 2.1 Phiên bản / protocol
- Baseline protocol client: **Kafka 2.1.0** (Java client tối thiểu 2.1.0; librdkafka >= 1.8.2). Kafka 3.7 deprecate (KIP-896), 4.0 gỡ; Cloud gia hạn 1 năm, **từ 2026-02 request cũ bị trả lỗi** ([deprecation](https://docs.confluent.io/cloud/current/client-apps/deprecate-how-to.html), [client overview](https://docs.confluent.io/cloud/current/client-apps/overview.html)).
- Không tìm thấy trang nêu phiên bản broker. Câu duy nhất: "The Kafka client version matches and maps to the version of Kafka that supports it" ([client overview](https://docs.confluent.io/cloud/current/client-apps/overview.html)).
- Release notes cho thấy Cloud theo kịp Kafka rất gần: 2026-10-26 hỗ trợ tính năng client/Streams của **Apache Kafka 4.4** (vd `partitioner.rack.aware` KIP-1123, `bootstrap.resolve.timeout.ms` KIP-909); share groups cần client tương thích 4.2+ ([release notes](https://docs.confluent.io/cloud/current/release-notes/index.html)).
- Bảng so sánh cluster types vẫn ghi "Client version (minimum) 0.11.0" - mâu thuẫn với baseline 2.1.0 ở trên (có vẻ cũ) ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
- Java client 4.0.x-4.3.x đang được hỗ trợ (4.3.x EoS 2028-05-22) ([client overview](https://docs.confluent.io/cloud/current/client-apps/overview.html)).

### 2.2 Tính năng
| Tính năng | Trên Confluent Cloud | Nguồn |
|---|---|---|
| Idempotent producer + transactions (EOS) | Hỗ trợ cả 5 loại; Freight mới thêm 2026-10-02 | [regions](https://docs.confluent.io/cloud/current/get-started/regions.html), [release notes](https://docs.confluent.io/cloud/current/release-notes/index.html) |
| KIP-848 (`group.protocol=consumer`) | Hỗ trợ từ 2025-04-28, client dựa Kafka 4.0+; mặc định client vẫn `classic` | [release notes](https://docs.confluent.io/cloud/current/release-notes/index.html), [client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html) |
| Share groups (KIP-932, "Queues for Kafka") | Chỉ **Dedicated và Enterprise**, Java client >= 4.2; Python còn "preview"; không có trên Freight | [release notes](https://docs.confluent.io/cloud/current/release-notes/index.html), [cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html) |
| Kafka Streams | Chỉ Java client; librdkafka chỉ admin/producer/consumer | [client overview](https://docs.confluent.io/cloud/current/client-apps/overview.html) |
| Compaction | Có (`cleanup.policy=compact`); giới hạn số partition nén (xem bảng mục 1); đổi `delete` -> `compact,delete` phải qua bước trung gian | [topic config](https://docs.confluent.io/cloud/current/topics/manage.html) |
| Auto topic creation | `auto.create.topics.enable` mặc định `false`; chỉ **Dedicated** đổi được | [broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html) |
| Unclean leader election | `false`, **không sửa được** | [topic config](https://docs.confluent.io/cloud/current/topics/manage.html) |
| Cluster Linking + transactions | "Not integrated with Kafka transactions"; replication bất đồng bộ | [cluster linking](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html) |
| Kafka REST v3 | Không consume được record; produce không nhận payload nén | tìm kiếm trên docs.confluent.io (kết quả search, xem Unverified) |
| Broker config (`server.properties`) | **Không sửa** trên Basic/Standard/Enterprise/Freight; Dedicated chỉ 6 setting (xem dưới) | [broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html) |

Dedicated chỉnh được: `auto.create.topics.enable`, `log.retention.ms`, `num.partitions`, `log.cleaner.max.compaction.lag.ms`, `ssl.enabled.protocols`, `ssl.cipher.suites` ([broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html)). TLS 1.2 và 1.3 đều bật, không cấu hình được trên các loại còn lại. `group.max.session.timeout.ms` = 1.200.000 ms (20 phút), không đổi được.

### 2.3 Topic config: sửa được / không sửa được
Nguồn: [Configuration Reference for Topics](https://docs.confluent.io/cloud/current/topics/manage.html). "Các giới hạn lúc tạo topic vẫn áp dụng khi sửa sau."

**Sửa được** (mặc định; giới hạn):
- `cleanup.policy` (delete); `retention.ms` (604.800.000 = 7 ngày; `-1` = Infinite Storage); `retention.bytes` (-1)
- `min.insync.replicas` (**2**; chỉ 1 hoặc 2)
- `num.partitions` (6; chỉ tăng, không giảm)
- `max.message.bytes` (2.097.164; tối đa 8.388.608 trên Basic/Standard, 20.971.520 trên Dedicated/Enterprise)
- `segment.bytes` (104.857.600; 52.428.800 - 1 GiB), `segment.ms` (604.800.000; tối thiểu 4 giờ)
- `delete.retention.ms` (86.400.000; tối đa 60.566.400.000), `min.compaction.lag.ms` (0), `max.compaction.lag.ms` (tối thiểu 21.600.000 Dedicated, 604.800.000 các loại khác)
- `message.timestamp.type` (CreateTime), `message.timestamp.{after,before,difference}.max.ms`

**Không sửa được**: `default.replication.factor` (**3**), `unclean.leader.election.enable` (false), `compression.type` (producer), `min.cleanable.dirty.ratio` (0.5), `flush.messages`, `flush.ms`, `index.interval.bytes`, `segment.index.bytes`, `segment.jitter.ms`, `file.delete.delay.ms`, `preallocate`, `message.downconversion.enable`.

### 2.4 So với Apache Kafka 4.4 (cluster Docker local)
| Config | Apache Kafka 4.4 default | Confluent Cloud |
|---|---|---|
| `min.insync.replicas` | 1 | 2 (chỉ 1 hoặc 2) |
| `default.replication.factor` | 1 | 3, cố định |
| `auto.create.topics.enable` | true | false (Dedicated; các loại khác không đổi được) |
| `num.partitions` | 1 | 6 |
| `message.max.bytes` / `max.message.bytes` | 1048588 | 2.097.164 (nâng lên 8/20 MB) |
| `segment.bytes` | 1 GiB | 100 MiB (50 MiB - 1 GiB) |
| `unclean.leader.election.enable` | false | false, không đổi |
| `retention.ms` | 7 ngày | 7 ngày |

Nguồn Apache: [broker configs 4.4](https://kafka.apache.org/44/configuration/broker-configs/), [topic configs 4.4](https://kafka.apache.org/44/configuration/topic-configs/). Nguồn Cloud: [topic config](https://docs.confluent.io/cloud/current/topics/manage.html), [broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html).

Hệ quả cho lab local: tạo topic với `--replication-factor 3 --config min.insync.replicas=2` để giống Cloud; cluster Docker 3 broker mới cho phép acks=all + minISR=2.

## 3. Độ bền / sẵn sàng ("không mất message")

**Replication và zone** ([resilience](https://docs.confluent.io/cloud/current/clusters/resilience.html)):
- Cluster multi-zone: RF = 3 trải 3 AZ, min ISR = 2, replication đồng bộ => "zero RPO" trong cluster.
- Mất 1 AZ: đọc/ghi bình thường, "RTO < 1 giây cho client cấu hình theo best practice". Mất 2 AZ: không ghi được (không đủ 2 ISR), vẫn đọc. Mất 3 AZ: mất cả đọc/ghi.
- Client phải tự xử lý rebalance và failover đúng cách để đạt RTO ~ 0.
- Durability doc: Confluent Cloud "enforces a replication factor of 3" ([durability](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html)).
- Cluster Linking (DR đa region) là **bất đồng bộ**; Confluent nói không có tùy chọn RPO=0 mặc định ([cluster FAQ](https://docs.confluent.io/cloud/current/clusters/cluster-faq.html)).

**Cấu hình client theo Confluent** ([durability](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html), [client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)):

| Producer | Giá trị | Ghi chú |
|---|---|---|
| `acks` | `all` (mặc định Java từ Kafka 3.0) | cùng `min.insync.replicas=2`, RF=3 |
| `enable.idempotence` | `true` (Java default true; **librdkafka default false**) | tránh trùng + giữ thứ tự khi retry |
| `retries` | mặc định MAX_INT; để nguyên khi bật idempotence | bị chặn bởi `delivery.timeout.ms` |
| `delivery.timeout.ms` | Java 120000 (2 phút); librdkafka 300000 | chọn theo "message hợp lệ bao lâu" nghiệp vụ |
| `max.in.flight.requests.per.connection` | 5 (idempotence); =1 nếu tắt idempotence và cần thứ tự | |

| Consumer | Giá trị |
|---|---|
| `enable.auto.commit` | `false` + commit sau khi xử lý xong để tránh mất |
| `isolation.level` | `read_committed` nếu dùng transaction (Java default `read_uncommitted`) |
| `group.protocol` | `classic` mặc định; `consumer` = KIP-848 (Java cần Kafka 4.0+) |

**Bảo trì**: Cloud "thường xuyên nâng cấp và bảo trì", làm **rolling restart tất cả broker**; warning retry được là bình thường, "alert trên lỗi thật, không alert trên retryable errors". Producer retry mặc định 2 phút, consumer/admin 1 phút. Gợi ý: `connections.max.idle.ms` khớp idle timeout của load balancer (AWS 350 giây) ([client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)). Dedicated: có "release priority" (cần Premier) để cluster test nhận update trước ([broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html)). Resilience page không mô tả chi tiết cách thay broker.

## 4. Kết nối client (tổng quan)

- Bootstrap: `pkc-xxxxx.<region>.aws.confluent.cloud:9092` (CLI in `SASL_SSL://pkc-exa45.us-west-2.aws.confluent.cloud:9092`; bỏ tiền tố `SASL_SSL://` khi đặt vào client). REST endpoint cùng host nhưng cổng 443. Ví dụ API cho Enterprise AWS dùng dạng `abc-00000-00000.us-east-1.aws.glb.confluent.cloud:9092` - nghĩa là endpoint có thể khác `pkc-`, luôn lấy từ Cluster Settings ([create cluster](https://docs.confluent.io/cloud/current/clusters/create-cluster.html)).
- Java, API key (nguồn chính thức, giá trị mẫu):
  ```
  security.protocol=SASL_SSL
  sasl.mechanism=PLAIN
  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='your-api-key' password='your-api-secret';
  ```
  ([clients cloud config](https://docs.confluent.io/cloud/current/cp-component/clients-cloud-config.html)). Key ID là username, secret là password; Basic/Standard **bắt buộc** key theo từng cluster (resource-scoped); mỗi key thuộc user hoặc service account, production nên dùng service account riêng cho mỗi app ([API keys](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html)).
- OAuth: `sasl.mechanism=OAUTHBEARER`, `OAuthBearerLoginModule` với `extension_logicalCluster` và `extension_identityPoolId`, `sasl.oauthbearer.token.endpoint.url` ([OAuth Java](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/java-clients.html)). Basic không có OAuth ([regions](https://docs.confluent.io/cloud/current/get-started/regions.html)).
- Spring Boot: `spring.kafka.bootstrap-servers=...` và các key không được hỗ trợ sẵn đưa qua `spring.kafka.properties[security.protocol]=SASL_SSL` ... (áp dụng cho mọi client; override cấu hình Boot) ([Spring Boot Kafka](https://docs.spring.io/spring-boot/reference/messaging/kafka.html)). Tài liệu Spring không có ví dụ SASL; ánh xạ ở đây là suy ra từ hai nguồn trên.
- Tạo sẵn config: Console > Clients, hoặc `confluent kafka client-config create java` ([config client](https://docs.confluent.io/cloud/current/client-apps/config-client.html)).
- Consumer group: không giới hạn số consumer, nhưng Confluent khuyên <= 2.000 consumer/group ([client FAQ](https://docs.confluent.io/cloud/current/client-apps/client-faq.html)).

## 5. Schema Registry (Stream Governance)

- Có sẵn trong Stream Governance, gói **Essentials** tự gắn vào environment (miễn phí, 100 schema, SLA 99.5%); **Advanced** từ 1 USD/giờ (20.000 schema, SLA 99.99%, data rules); cả hai giới hạn 75 đọc + 25 ghi/giây ([packages](https://docs.confluent.io/cloud/current/stream-governance/packages.html)). Schema tối đa 1 MB ([schemas manage](https://docs.confluent.io/cloud/current/sr/schemas-manage.html)).
- Serializer Java: `io.confluent.kafka.serializers.KafkaAvroSerializer`/`KafkaAvroDeserializer`, `...protobuf.KafkaProtobufSerializer`, `...json.KafkaJsonSchemaSerializer`; `schema.registry.url`; subject mặc định `TopicNameStrategy` (`<topic>-key`/`<topic>-value`); `auto.register.schemas` mặc định bật; `use.latest.version` chỉ áp dụng khi `auto.register.schemas=false` ([serdes](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html)). SR client dùng thuộc tính `schema.registry.basic.auth.user.info=$SR_APIKEY:$SR_APISECRET` (key riêng của SR, theo ví dụ console trên cùng trang).
- Compatibility: mặc định **BACKWARD**; BACKWARD_TRANSITIVE, FORWARD(_TRANSITIVE), FULL(_TRANSITIVE), NONE ([schema evolution](https://docs.confluent.io/cloud/current/sr/fundamentals/schema-evolution.html)). BACKWARD = consumer dùng schema X đọc được dữ liệu schema X hoặc X-1 => nâng cấp consumer trước producer.
- Schema validation (broker-side) chỉ có ở Dedicated ([regions](https://docs.confluent.io/cloud/current/get-started/regions.html)). Serializer Confluent là của Confluent, không nằm trong Apache Kafka; cần thêm dependency và repo Maven của Confluent (repo URL: chưa xác minh).

## 6. Giám sát và dịch vụ kèm

- Consumer lag: Console > cluster > Clients > Consumer Lag; Metrics API `io.confluent.kafka.server/consumer_lag_offsets`; group `EMPTY` >1 ngày không còn trả dữ liệu; consumer dùng `assign()` không báo lag ([monitor lag](https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html)). Metrics API cần key loại Cloud resource management (key của cluster bị từ chối) và role MetricsViewer ([metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)).
- Cluster Linking: sao chép topic giữa cluster/region (mirror topic read-only); destination phải Dedicated hoặc Enterprise ([cluster linking](https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html)).
- Managed connectors: "100+ fully managed connectors" ([connectors](https://docs.confluent.io/cloud/current/connectors/overview.html)).

## 7. Gotchas và câu hỏi ngày đầu

### Gotchas (so với Apache Kafka thuần)
1. **Không có `server.properties`** trên 4/5 loại cluster; muốn đổi hành vi phải qua topic config và bị giới hạn ([broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html)).
2. `min.insync.replicas` chỉ 1 hoặc 2 và **mặc định 2**; RF luôn 3. Lab local mặc định minISR=1/RF=1, sẽ khác ([topic config](https://docs.confluent.io/cloud/current/topics/manage.html), [Apache 4.4](https://kafka.apache.org/44/configuration/broker-configs/)).
3. Ai đặt `min.insync.replicas=1` sẽ làm yếu độ bền; `unclean.leader.election.enable` thì không ai bật được ([topic config](https://docs.confluent.io/cloud/current/topics/manage.html)).
4. `num.partitions` mặc định **6** (không phải 1), chỉ tăng, và **trần partition theo cluster type** (Basic 1.500, Standard 2.500) - vượt sẽ lỗi "You may not create more than the maximum number of partitions" ([topics FAQ](https://docs.confluent.io/cloud/current/topics/topics-faq.html), [cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
5. Message size mặc định ~2 MB, trần 8 MB (Basic/Standard) hoặc 20 MB (Enterprise/Dedicated); `max.message.bytes` tính trên batch sau nén ([topic config](https://docs.confluent.io/cloud/current/topics/manage.html)).
6. Auto topic creation **tắt**; `allow.auto.create.topics` của Java consumer mặc định true; suy luận (chưa có nguồn nói thẳng): vô tác dụng khi broker tắt auto-create ([client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html), [broker config](https://docs.confluent.io/cloud/current/clusters/broker-config.html)). Tạo topic bằng CLI/API/Console; không có bulk delete và không có scheduler ([topics FAQ](https://docs.confluent.io/cloud/current/topics/topics-faq.html)).
7. Đổi `cleanup.policy` từ `delete` sang `compact,delete` phải qua bước `compact`; đổi sang compact **xóa các record không có key** ([topics FAQ](https://docs.confluent.io/cloud/current/topics/topics-faq.html)).
8. Maintenance là rolling restart định kỳ: lỗi retryable là bình thường; producer bỏ cuộc sau `delivery.timeout.ms` (2 phút Java) nên app phải xử lý `TimeoutException` ([client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)).
9. Giới hạn **throttle** theo eCKU: Basic ingress 250 MBps/1.000 connections, request/giây thấp; quá giới hạn có thể bị chậm ([cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)).
10. Client librdkafka khác default Java (`enable.idempotence=false`, partitioner `consistent_random`, `isolation.level=read_committed`): app Java và non-Java ghi cùng topic phải đồng bộ partitioner ([client configs](https://docs.confluent.io/cloud/current/client-apps/client-configs.html)).
11. Consumer offsets giữ **7 ngày** (trùng `offsets.retention.minutes=10080` của Apache 4.4), group EMPTY bị xóa sau đó ([cluster FAQ](https://docs.confluent.io/cloud/current/clusters/cluster-faq.html), [Apache 4.4](https://kafka.apache.org/44/configuration/broker-configs/)).
12. Client cũ (< protocol Kafka 2.1.0) bị từ chối từ 2026-02 ([deprecation](https://docs.confluent.io/cloud/current/client-apps/deprecate-how-to.html)). Kafka client 4.x nằm trong danh sách hỗ trợ (mục 2.1).
13. Durability doc của Confluent khuyên `acks=all`, `enable.auto.commit=false` - giống Apache; khác ở chỗ platform ép RF=3 ([durability](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html)).
14. Share groups: tính năng mới, chỉ Enterprise/Dedicated, không bài học nào nên dựa vào nó cho Basic/Standard/Freight ([release notes](https://docs.confluent.io/cloud/current/release-notes/index.html)).

### 8 câu hỏi cho platform team
1. Cluster thuộc loại nào (Basic/Standard/Enterprise/Dedicated/Freight), SLA bao nhiêu, và `availability` là Low/High (single hay multi-zone)? Có cần nâng để đạt 99.99%?
2. Kết nối bằng gì: public hay PrivateLink/PNI (Enterprise AWS + PrivateLink giới hạn 10 eCKU)? Bootstrap endpoint là gì?
3. Xác thực: API key theo service account hay OAuth? Ai cấp, xoay key thế nào, mỗi app có service account riêng không?
4. Ai được tạo topic và bằng cách nào (self-service Console/Terraform/CLI)? Có chính sách đặt tên, quota partition và RBAC/ACL cho `CREATE`/`ALTER` topic không? Còn bao nhiêu partition trước trần của cluster?
5. Chuẩn topic của ngân hàng: RF=3, `min.insync.replicas=2`, `retention.ms` bao lâu, `max.message.bytes` cần nâng lên bao nhiêu? Có cho phép đặt `min.insync.replicas=1` không?
6. Có Stream Governance Essentials hay Advanced? Compatibility mặc định cho subject, ai quản lý schema, có bật auto-register trên production không?
7. Lịch bảo trì/nâng cấp và cách thông báo? Dedicated có bật release priority cho môi trường test? Monitoring/alert lag và lỗi do ai sở hữu (Metrics API/Datadog)?
8. DR: có Cluster Linking sang region khác không, RPO chấp nhận được là bao nhiêu (vì bất đồng bộ), và transactions/EOS có được dùng không (không mirror được)?

---

## Unverified

- Phiên bản Kafka cụ thể của broker Confluent Cloud: không có trang chính thức nêu.
- Basic/Standard/Enterprise "Low" availability có single-zone hay không: tài liệu chỉ cho ví dụ CLI Basic = single-zone, không có phát biểu chung. Mục "Cluster type support by feature" chỉ được đọc qua tóm tắt của công cụ, không trích nguyên văn.
- Trạng thái `auto.create.topics.enable` thực tế trên Basic/Standard/Enterprise/Freight: chỉ biết Dedicated mặc định `false` và các loại kia không sửa được.
- Kafka REST v3 không consume được / không nhận payload nén: chỉ thấy qua kết quả tìm kiếm trên docs.confluent.io, chưa mở trang [kafka-rest-cc](https://docs.confluent.io/cloud/current/kafka-rest/kafka-rest-cc.html).
- Share groups: release notes ghi "preview" (2025-09-04) rồi "launch" (2026-02-20); client Python còn "preview" (2026-06-30). Không có tuyên bố GA rõ ràng cho cluster.
- Các trang rất dài: chỉ đọc được các đoạn WebFetch trả về (thường ~100k ký tự đầu hoặc theo offset). Release notes 615k ký tự chỉ đọc 3 đoạn; có thể bỏ sót mục khác.
- Chi tiết thay broker / thời lượng rolling restart, trễ làm mới của Metrics API: không tìm thấy.
- Giới hạn số consumer group, số topic, số ACL, API key mỗi cluster: không đọc được con số.
- Bản đồ Spring `spring.kafka.properties[...]` sang Confluent là ghép từ hai nguồn; chưa chạy thử.
- Repo Maven và coordinates của serializer Confluent: trang đọc được không nêu.
- Mẫu `application.properties` Spring Boot chính thức của Confluent: URL examples/spring-boot.html trả về trang landing, không dùng được.

## Sources (đã fetch)

- https://docs.confluent.io/cloud/current/clusters/cluster-types.html - bảng cluster, SLA, limits, Freight, Dedicated zone.
- https://docs.confluent.io/cloud/current/clusters/broker-config.html - broker settings Dedicated, auto.create, release priority.
- https://docs.confluent.io/cloud/current/topics/manage.html - bảng topic config (editable, default, limits).
- https://docs.confluent.io/cloud/current/topics/topics-faq.html - trần partition, max message size, cleanup.policy, bulk delete.
- https://docs.confluent.io/cloud/current/clusters/cluster-faq.html - HA, upgrade Basic->Standard, offsets 7 ngày, RPO, client quotas.
- https://docs.confluent.io/cloud/current/clusters/resilience.html - RF 3, min ISR 2, hành vi khi mất AZ, RTO.
- https://docs.confluent.io/cloud/current/clusters/create-cluster.html - bootstrap format, `availability` Low/High.
- https://docs.confluent.io/cloud/current/get-started/regions.html - bảng feature theo cluster type.
- https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.html - acks, idempotence, retries, EOS.
- https://docs.confluent.io/cloud/current/client-apps/client-configs.html - default Java/librdkafka, rolling restart.
- https://docs.confluent.io/cloud/current/client-apps/client-faq.html - clients hỗ trợ, retry mặc định, 2.000 consumer.
- https://docs.confluent.io/cloud/current/client-apps/overview.html - support matrix, Java/librdkafka tối thiểu, Streams.
- https://docs.confluent.io/cloud/current/client-apps/deprecate-how-to.html - baseline protocol 2.1.0.
- https://docs.confluent.io/cloud/current/client-apps/config-client.html - SASL_PLAIN/OAUTHBEARER, `client-config create`.
- https://docs.confluent.io/cloud/current/cp-component/clients-cloud-config.html - block Java Properties.
- https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html - API key.
- https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/identity-providers/oauth/clients/java-clients.html - OAuth Java.
- https://docs.confluent.io/cloud/current/release-notes/index.html - KIP-848, share groups, Kafka 4.4, Freight transactions, KIP-890.
- https://docs.confluent.io/cloud/current/multi-cloud/cluster-linking/index.html - Cluster Linking.
- https://docs.confluent.io/cloud/current/connectors/overview.html - managed connectors.
- https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html - consumer lag.
- https://docs.confluent.io/cloud/current/monitoring/metrics-api.html - Metrics API.
- https://docs.confluent.io/cloud/current/stream-governance/packages.html - Essentials vs Advanced.
- https://docs.confluent.io/cloud/current/sr/schemas-manage.html - compat mặc định, giới hạn schema 1 MB.
- https://docs.confluent.io/cloud/current/sr/fundamentals/schema-evolution.html - kiểu compatibility.
- https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html - serializer, subject strategy.
- https://docs.confluent.io/kafka-clients/java/current/overview.html - đã fetch, không có config Cloud (chỉ xác nhận serializer artifact).
- https://docs.confluent.io/cloud/current/overview.html, https://docs.confluent.io/cloud/current/stream-governance/overview.html, https://docs.confluent.io/cloud/current/client-apps/examples/spring-boot.html - đã fetch, không chứa dữ liệu dùng được.
- https://kafka.apache.org/44/configuration/broker-configs/ - default Apache 4.4 broker.
- https://kafka.apache.org/44/configuration/topic-configs/ - default Apache 4.4 topic.
- https://docs.spring.io/spring-boot/reference/messaging/kafka.html - `spring.kafka.properties[...]`.
- Kết quả WebSearch (site docs.confluent.io) dùng để tìm URL; các tuyên bố được trích lại từ trang đã fetch, trừ Kafka REST (xem Unverified).
