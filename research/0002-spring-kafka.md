# 0002 - Spring Kafka cho demo "không mất message": phiên bản, API, hành vi mặc định, khung dự án

Ngày: 2026-11-10. Chỉ dùng nguồn chính (docs.spring.io, GitHub spring-projects/spring-kafka tag `v4.1.2`, Maven Central, kafka.apache.org/44). Link dạng `[tên]` trỏ tới mục "Sources" cuối file. **(suy ra)** = tôi tự suy/ghép từ nguồn, không có câu nguyên văn. Mục chưa kiểm chứng nằm ở "Unverified".

## Tóm tắt

1. GA mới nhất hôm nay: **Spring Boot 4.1.2** + **Spring Kafka 4.1.2** (Maven Central: 4.2.0-RC* vẫn là bản RC) ([mvn-boot], [mvn-sk]). Boot 4.1.2 quản lý `spring-kafka.version=4.1.2`, `kafka.version=4.2.2` ([mvn-deps]).
2. Baseline: Java 17+ (chạy được tới 27), Maven 3.6.3+ ([boot-sysreq]). Máy có Maven 3.8.6 và Java 25 là đủ; **Java 8 phải tránh** (JAVA_HOME phải trỏ JDK 17+).
3. Client là kafka-clients **4.2.2**, broker lab là 4.4: không có câu "client 4.2 tương thích broker 4.4" trong docs; chỉ có "broker >= 2.1 / client >= 2.1" cho mốc 4.0 ([kf-upgrade]) -> ghi Unverified.
4. `KafkaTemplate.send(...)` trả `CompletableFuture<SendResult<K,V>>` ([sk-send]); spring-retry đã bị gỡ khỏi 4.0, `@Backoff` -> `@BackOff` ([sk-history]).
5. Boot **không** đổi default của producer (`acks`, `retries`, idempotence đều "None" trong metadata) -> dùng default client 4.x: `acks=all`, `enable.idempotence=true` ([jar-meta], [kf-producer]). Chỉ bật/tắt qua `spring.kafka.producer.properties[...]`.
6. Listener: `ack-mode` default của Boot là None -> container dùng `BATCH`; `enable.auto.commit` do Spring ép `false` trừ khi tự đặt ([sk-container], [jar-meta]).
7. `DefaultErrorHandler` mặc định `FixedBackOff(0L, 9)` = 10 lần giao, hết thì **chỉ log ERROR và commit offset (mất message nếu không có DLT)** ([sk-errh]). DLT mặc định: `<topic>-dlt`, **cùng partition số** ([sk-errh], [sk-dlpr-src]) - khác tutorial cũ dùng `.DLT`.
8. `TopicBuilder` **không có** `minInsyncReplicas()`; dùng `.config("min.insync.replicas","2")` ([sk-topicbuilder-src]). Topic đã tồn tại thì config **không** bị sửa trừ khi `spring.kafka.admin.modify-topic-configs=true` ([jar-meta]).
9. Transactions: `spring.kafka.producer.transaction-id-prefix` tự dựng `KafkaTransactionManager`; consumer cần `isolation-level=read-committed` ([sk-boot], [sk-transactions], [kf-consumer]).
10. Idempotent consumer / outbox: **không có tài liệu chính thức** của Spring Kafka -> tự dựng (suy ra).

---

## 1. Phiên bản và toạ độ Maven

| Hạng mục | Giá trị | Nguồn |
|---|---|---|
| Boot GA mới nhất | `4.1.2` (list có `4.0.9`, `4.1.0-4.1.2`, `4.2.0-M1..RC2`; `latest`/`release` tag của metadata = `4.2.0-RC2` nhưng đó là RC) | [mvn-boot] |
| Spring Kafka GA mới nhất | `4.1.2` (metadata `release`=`4.2.0-RC1`, RC) | [mvn-sk] |
| Docs trang chủ | "This documentation is for Spring for Apache Kafka 4.1.2", stable | [sk-index] |
| BOM Boot 4.1.2 | `spring-kafka.version`=4.1.2, `kafka.version`=4.2.2, `spring-framework.version`=7.0.10, `testcontainers.version`=2.0.5 | [mvn-deps] |
| spring-kafka 4.1.2 -> kafka-clients | `org.apache.kafka:kafka-clients:4.2.2` (compile); `spring-context/messaging/tx` 7.0.10; không còn spring-retry | [mvn-sk-pom] |
| kafka-clients mới nhất trên Central | `4.4.0` (có 4.3.x, 4.4.0) - Boot 4.1.2 **không** kéo bản này | [mvn-kc] |
| Java | "Spring Boot 4.1.2 requires at least Java 17 and is compatible with versions up to and including Java 27"; Spring Kafka quick tour: "Minimum Java version: 17" | [boot-sysreq], [sk-quick] |
| Maven | "Maven 3.6.3 or later" (system requirements); plugin: "you must use Maven 3.6.3 or later" -> Maven 3.8.6 **đủ** | [boot-sysreq], [boot-mvn] |
| Kafka client min Java | client Java 11, broker/tools Java 17 ("minimum Java version required by clients ... Java 11 ... brokers, connect and tools now require Java 17") | [kf-upgrade] |

Lưu ý lệch docs: trang Quick Tour của Spring Kafka 4.1.2 vẫn ghi "Apache Kafka Clients: 4.0.x" ([sk-quick]) nhưng POM thật khai báo `kafka-clients 4.2.2` ([mvn-sk-pom]); tin POM. Trang testing còn link docs Kafka 4.2 ([sk-testing]).

**Tương thích client 4.2.2 <-> broker 4.4:** không tìm thấy câu chính thức cho cặp này. Docs 4.4 chỉ nói: "Clients (including Streams and Connect) must be on version 2.1 or higher before upgrading to 4.0" và "ensure brokers are version 2.1 or higher before upgrading Java clients" ([kf-upgrade]). (suy ra) client 4.2.2 nói chuyện với broker 4.4 được vì nằm sau mốc 2.1, nhưng đây là suy luận -> Unverified. Phương án đổi client: đặt `<kafka.version>4.4.0</kafka.version>` trong `<properties>` (docs Spring Kafka hướng dẫn cách override này, ví dụ dùng `4.0.0`) ([sk-override]); không có tuyên bố Spring Kafka 4.1.2 đã được test với client 4.4.0.

### Toạ độ (Maven)

```xml
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>4.1.2</version>   <!-- parent này kế thừa spring-boot-dependencies 4.1.2, java.version=17, compiler -parameters -->
</parent>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-kafka</artifactId>      <!-- không ghi version -->
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-kafka-test</artifactId> <!-- chỉ khi dùng @EmbeddedKafka -->
  <scope>test</scope>
</dependency>
```

- Parent 4.1.2: `java.version=17`, `maven.compiler.release=${java.version}`, plugin `spring-boot-maven-plugin` repackage, compiler `<parameters>true` ([mvn-parent]).
- `spring-boot-starter-kafka` = `spring-boot-starter` + `spring-boot-kafka`; `spring-boot-kafka` kéo `spring-kafka` + `spring-boot-transaction` ([mvn-starter], [mvn-boot-kafka]). Trang quick tour dùng đúng toạ độ starter này ([sk-quick]). Boot 4 tách module: `KafkaProperties` ở package `org.springframework.boot.kafka.autoconfigure` ([jar-meta]).
- Muốn `mvn spring-boot:run` phải có `spring-boot-maven-plugin` trong `<build>` (parent chỉ quản lý phiên bản/goal) ([mvn-parent]). (suy ra: khai báo plugin không version.)
- Cần `<java.version>25</java.version>`? Không bắt buộc: Boot hỗ trợ tới 27 ([boot-sysreq]); để `17` hoặc `25` đều được, nhưng JDK chạy Maven phải >= 17 (suy ra).

---

## 2. Từng kịch bản: cách chính thức + tên thuộc tính/class

Quy ước tên thuộc tính Boot lấy từ metadata trong jar `spring-boot-kafka-4.1.2` ([jar-meta]). Boot chỉ có property riêng cho một tập con; còn lại dùng `spring.kafka.{producer|consumer|admin}.properties[<tên Kafka>]` ("Properties set in this way override any configuration item that Spring Boot explicitly supports") ([sk-boot]).

### 2.0 Chung: kết nối cụm ngoài, KafkaTemplate

```properties
spring.kafka.bootstrap-servers=localhost:29092,localhost:39092,localhost:49092
```
Cú pháp `spring.kafka.bootstrap-servers=...` lấy từ [sk-boot] (ví dụ `localhost:9092`); danh sách 3 broker là (suy ra) vì `bootstrap-servers` kiểu `List<String>` ([jar-meta]). Có thể ghi đè theo từng client: `spring.kafka.producer.bootstrap-servers`, `...consumer...`, [jar-meta].

Gửi (Boot tự tạo `KafkaTemplate<?,?>`, [sk-boot]); trả về `CompletableFuture<SendResult<K, V>>` ([sk-send]):
```java
// Bất đồng bộ - [sk-send], "Example 1 (Non Blocking, Async)"
CompletableFuture<SendResult<String, String>> future = template.send(record);
future.whenComplete((result, ex) -> {
    if (ex == null) { handleSuccess(data); }
    else { handleFailure(data, record, ex); }
});
// Đồng bộ - [sk-send], "Example 2 (Blocking, Sync)"; docs khuyên get() có timeout
template.send(record).get(10, TimeUnit.SECONDS);   // ExecutionException.getCause() là KafkaProducerException, có producerRecord
```
Thay đổi API cần lưu ý: từ 3.0 future là `CompletableFuture` thay vì `ListenableFuture` ([sk-history]) -> tutorial cũ dùng `addCallback(...)` sẽ không biên dịch. Từ 4.0: bỏ ZooKeeper (`EmbeddedKafkaZKBroker` gỡ), bỏ JUnit 4 rule, bỏ spring-retry, Jackson 3 (`JsonKafkaHeaderMapper` thay `DefaultKafkaHeaderMapper`) ([sk-history]); trang serdes dùng `JacksonJsonSerializer/JacksonJsonDeserializer` ([sk-serdes], [sk-boot]).

### 2.1 Kịch bản 1 - Producer durability

Thuộc tính Boot có sẵn: `spring.kafka.producer.acks`, `.retries`, `.batch-size`, `.buffer-memory`, `.compression-type`, `.transaction-id-prefix`, `.properties` ([jar-meta]). **Không có** thuộc tính riêng cho `enable.idempotence`, `delivery.timeout.ms`, `linger.ms`, `request.timeout.ms`, `max.in.flight...` -> đi qua `properties`:

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        "[enable.idempotence]": true
        "[delivery.timeout.ms]": 120000
        "[request.timeout.ms]": 30000
        "[max.in.flight.requests.per.connection]": 5
```
(suy ra: ghép cú pháp `properties[...]` của [sk-boot] với tên/giá trị từ [kf-producer].)

Default client 4.4 (đã đọc ở [kf-producer]): `acks=all`; `enable.idempotence=true`; `retries=2147483647` ("prefer to leave unset, use `delivery.timeout.ms`"); `delivery.timeout.ms=120000` và phải >= `request.timeout.ms + linger.ms`; `request.timeout.ms=30000`; `linger.ms=5` (đổi từ 0 ở 4.0); `max.in.flight=5`; `max.block.ms=60000`. Quan hệ chống nhầm: idempotence bật mà đặt `acks != all`/`retries=0`/`in.flight>5`: nếu **tường minh** `enable.idempotence=true` -> `ConfigException`; nếu không tường minh -> idempotence âm thầm tắt ([kf-producer]).

Để demo mất message: hạ tường minh `acks: 1` và `"[enable.idempotence]": false` ((suy ra) từ [kf-producer]: `acks=1` chỉ "confirms only the leader's local write").

`min.insync.replicas` là **cấu hình topic** (default 1; "With acks=all, a write fails if the in-sync set is smaller than this value"; có thể đổi nghĩa khi bật ELR) ([kf-topic]) -> đặt khi tạo topic (mục 2.6), không đặt ở producer.

Phát hiện lỗi gửi: (a) `whenComplete`/`get()` như 2.0; (b) `ProducerListener` (`onSuccess`, `onError`), mặc định `LoggingProducerListener` chỉ log lỗi ([sk-send]). Nếu fire-and-forget (`template.send(...)` không đọc future) lỗi chỉ hiện trong log -> mất message âm thầm về phía ứng dụng (suy ra từ [sk-send]).
Với `linger.ms>0`, `flush()` chỉ cần khi muốn đẩy batch dở; template có tham số `autoFlush` ([sk-send]).

Kill broker khi đang gửi: ngoài phạm vi docs; (suy ra) quan sát qua `NotEnoughReplicas*Exception` khi ISR < `min.insync.replicas` (tên class lấy từ nghiên cứu 0001, không fetch lại ở đây).

### 2.2 Kịch bản 2 - Consumer

Thuộc tính Boot ([jar-meta]): `spring.kafka.consumer.{group-id, auto-offset-reset, enable-auto-commit, auto-commit-interval, max-poll-records, max-poll-interval, heartbeat-interval, isolation-level, fetch-min-size, fetch-max-wait, key-deserializer, value-deserializer, properties}`; `spring.kafka.listener.{ack-mode, ack-count, ack-time, concurrency, poll-timeout, idle-between-polls, immediate-stop, missing-topics-fatal, async-acks, type, auto-startup, client-id, observation-enabled}`. **Không có** `session-timeout` hay `group-instance-id` -> dùng `spring.kafka.consumer.properties[session.timeout.ms]` / `[group.instance.id]` (suy ra, theo [sk-boot]).

Default Kafka 4.4 ([kf-consumer]): `enable.auto.commit=true`, `auto.commit.interval.ms=5000`, `auto.offset.reset=latest`, `max.poll.records=500`, `max.poll.interval.ms=300000`, `session.timeout.ms=45000`, `heartbeat.interval.ms=3000`, `group.protocol=classic`, `isolation.level=read_uncommitted`. Lưu ý chính thức: tăng partition khi `latest` có thể "silent message loss" ([kf-consumer]).

**Auto vs manual commit** ([sk-container]):
- "The default `AckMode` is `BATCH`. Starting with version 2.3, the framework sets `enable.auto.commit` to `false` unless explicitly set in the configuration." -> với Boot không đặt gì: commit do container, `BATCH` (sau khi xử lý hết lô `poll()`).
- Các mode (khi không dùng transaction): `RECORD`, `BATCH`, `TIME`, `COUNT`, `COUNT_TIME`, `MANUAL`, `MANUAL_IMMEDIATE`. Mặc định `ackTime=5000`, `ackCount=1`, `syncCommits=true` ([sk-props]).
- Manual: `MANUAL`/`MANUAL_IMMEDIATE` yêu cầu listener nhận `Acknowledgment`; nếu factory không đặt ack mode thủ công thì "the `Acknowledgment` object will be null" ([sk-annot], [sk-container]).
```yaml
spring.kafka.listener.ack-mode: manual_immediate     # (suy ra: chuỗi enum, relaxed binding)
spring.kafka.consumer.enable-auto-commit: false
```
```java
// [sk-annot] (4.1+): override theo từng listener
@KafkaListener(id = "manual", topics = "myTopic", ackMode = "MANUAL")
public void listen(String data, Acknowledgment ack) { ...; ack.acknowledge(); }
```
- Auto commit thật của Kafka (để demo mất/trùng): đặt tường minh `spring.kafka.consumer.enable-auto-commit=true` (khi đó container không ép false) ([sk-container]: "unless specifically set").
- `nack(sleep)`: commit offset đã xử lý, seek lại record lỗi + phần còn lại; chỉ gọi trên thread consumer ([sk-container]).
- At-least-once = xử lý xong mới `acknowledge()`; trùng xảy ra khi crash giữa xử lý và commit (suy ra từ [sk-container]).

**Concurrency**: `spring.kafka.listener.concurrency` hoặc `@KafkaListener(concurrency = "3")`; `ConcurrentMessageListenerContainer` tạo N `KafkaMessageListenerContainer`; concurrency > số partition thì thừa consumer nhàn rỗi (suy ra từ ví dụ 3 topic x 5 partition trong [sk-container]) ([sk-annot], [sk-container]).

**Static membership**: "`ConcurrentMessageListenerContainer` now supports Static Membership when the concurrency is greater than one. The `group.instance.id` is suffixed with `-n` ... together with an increased `session.timeout.ms`" ([sk-container]). Config Kafka: `group.instance.id` ([kf-consumer]). Trên `@KafkaListener(properties=...)` được phép ghi đè property consumer, **trừ** `group.id` và `client.id` ([sk-annot]) -> `group.instance.id` đặt qua `spring.kafka.consumer.properties[group.instance.id]` hoặc `properties = "group.instance.id=..."` (suy ra: instance id phải khác nhau giữa 2 process).

**Rebalance**: `ConsumerAwareRebalanceListener` (`onPartitionsRevokedBeforeCommit`, `onPartitionsRevokedAfterCommit`, `onPartitionsAssigned`, `onPartitionsLost`); gán qua `ContainerProperties.setConsumerRebalanceListener(...)`; Boot tự gắn bean `ConsumerAwareRebalanceListener` vào factory mặc định ([sk-rebalance], [sk-boot]). Snippet trong docs có lỗi chữ ký (thiếu tham số `consumer`) - dùng chữ ký ở interface. KIP-848: `spring.kafka.consumer.properties.group.protocol=consumer`; khi đó assignor phía client bị bỏ qua ([sk-rebalance]). Khi `group.protocol=consumer`, `session.timeout.ms`/`heartbeat.interval.ms` client không áp dụng ([kf-consumer]) -> để demo static membership/session timeout giữ `classic` (mặc định).

**max.poll.interval.ms bị vượt**: `spring.kafka.consumer.max-poll-interval` (kiểu Duration) hoặc `@KafkaListener(properties = {"max.poll.interval.ms:60000", ...})` ([jar-meta], [sk-annot]). Hành vi theo Kafka: không `poll()` trước hạn -> consumer coi là failed, group rebalance; riêng consumer có `group.instance.id` thì partition "will not be immediately reassigned" ([kf-consumer]). Demo (suy ra): `Thread.sleep` > giá trị này trong listener. `idleBetweenPolls` được giới hạn bởi `max.poll.interval.ms - thời gian xử lý lô` ([sk-container]). Với back-off dài hơn `max.poll.interval.ms` dùng `ContainerPausingBackOffHandler` ([sk-errh]).

### 2.3 Kịch bản 3 - Lỗi: poison pill, retry, DLT

Boot: bean `CommonErrorHandler` được tự gắn vào container factory mặc định ("you simply need to add the error handler as a `@Bean` and Boot will add it to the auto-configured factory") ([sk-errh], [sk-boot]).

Mặc định `DefaultErrorHandler` ([sk-errh]):
- "By default, after ten failures, the failed record is logged (at the `ERROR` level)"; mặc định `FixedBackOff(0L, 9)` (không chờ, 10 lần giao). Hết retry: recoverer mặc định chỉ log; "The recovered record's offset is committed" -> **record bị bỏ qua**.
- Ngoại lệ "fatal" không retry, gọi recoverer ngay: `DeserializationException`, `MessageConversionException`, `ConversionException`, `MethodArgumentResolutionException`, `NoSuchMethodException`, `ClassCastException`. Tuỳ biến: `addNotRetryableExceptions(...)`/`setClassifications(...)`.
- `resetStateOnExceptionChange` mặc định `true` (từ 2.9); `resetStateOnRecoveryFailure` mặc định reset BackOff.
- Blocking retry: thread consumer ngủ theo back-off.

```java
// [sk-errh]: 3 lần thử, không chờ, hết thì gọi recoverer
DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> { /* ... */ }, new FixedBackOff(0L, 2L));
// [sk-errh]: 1 giây giữa các lần
factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));
// [sk-errh]: bean + DLT
@Bean public DefaultErrorHandler errorHandler(ConsumerRecordRecoverer recoverer) {
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer);
    handler.addNotRetryableExceptions(IllegalArgumentException.class);
    return handler;
}
// (suy ra) ghép: new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(1000L, 2L))
```
`DeadLetterPublishingRecoverer` ([sk-errh], [sk-dlpr-src]):
- Mặc định gửi tới `<originalTopic>-dlt`, **cùng số partition** của record gốc -> DLT "must have at least as many partitions as the original topic". Mã nguồn: `new TopicPartition(cr.topic() + "-dlt", cr.partition())`. (Tutorial cũ ghi `.DLT` là lỗi thời cho 4.1.)
- Giữ toàn bộ header gốc + thêm `KafkaHeaders.DLT_EXCEPTION_FQCN`, `DLT_EXCEPTION_MESSAGE`, `DLT_EXCEPTION_STACKTRACE`, `DLT_ORIGINAL_TOPIC`, `DLT_ORIGINAL_PARTITION`, `DLT_ORIGINAL_OFFSET`, `DLT_ORIGINAL_CONSUMER_GROUP`, ...
- Gửi DLT thất bại: `failIfSendResultIsError` mặc định **`true`** (mã nguồn); `waitForSendResultTimeout` khởi tạo 30 giây (mã nguồn; Javadoc setter nói khác - xem Unverified) ([sk-dlpr-src]). Khi recoverer ném lỗi thì record nằm trong danh sách seek lại, BackOff reset ([sk-errh]).
- DLT không tự tạo bởi `DeadLetterPublishingRecoverer`; cần tạo topic `*-dlt` bằng `NewTopic` (suy ra; docs chỉ nêu auto-creation cho `@RetryableTopic`, mục sau).

**Poison pill do deserialize** ([sk-serdes], [sk-ehd-src]):
```properties
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties[spring.deserializer.value.delegate.class]=org.apache.kafka.common.serialization.StringDeserializer
```
(tên property `spring.deserializer.value.delegate.class` lấy từ hằng `VALUE_DESERIALIZER_CLASS` trong mã nguồn; cú pháp Boot (suy ra).) Delegate lỗi -> value `null` + header `DeserializationException` (có raw bytes); với record listener, `ErrorHandler` được gọi và record không tới listener. DLT khôi phục lại value gốc (từ 2.3) ([sk-serdes], [sk-errh]). Batch listener thì phải tự xử lý ([sk-errh]).

**Non-blocking retry `@RetryableTopic`** ([sk-retry], [sk-naming]):
```java
@RetryableTopic(kafkaTemplate = "myRetryableTopicKafkaTemplate")   // nếu không: bean defaultRetryTopicKafkaTemplate, không có thì ném lỗi
@KafkaListener(topics = "my-annotated-topic", groupId = "myGroupId")
public void processMessage(MyPojo message) { }
@DltHandler public void processMessage(MyPojo message) { }        // không có thì consumer DLT chỉ log
```
- Hậu tố mặc định `-retry` và `-dlt`: `my-topic-retry-0`, ..., `my-topic-dlt`; từ 4.1 mặc định `SINGLE_TOPIC` cho khoảng cách retry bằng nhau (`sameIntervalTopicReuseStrategy`) ([sk-naming], [sk-whatsnew]). Backoff: `@RetryableTopic(backOff = @BackOff(delay = 2000, maxDelay = 10000, multiplier = 2))` - `@BackOff` là annotation của Spring Kafka từ 4.0 ([sk-history]).
- Boot: `spring.kafka.retry.topic.enabled` (default false), `.attempts` (default 3), `.backoff.delay` (1s), `.backoff.multiplier` (1), `.backoff.max-delay` (30s), `.backoff.jitter` ([jar-meta]).
- `autoCreateTopics`: topic retry/main tạo tự động, replication factor mặc định `-1` = broker default; ghi đè bằng `NewTopic` bean ([sk-retry]) -> với DLT nên khai báo `NewTopic` riêng để có `min.insync.replicas`/replicas đúng (suy ra).
- Không dùng được với batch listener; không kết hợp với container transaction ([sk-errh], [sk-transactions]). Retry topic không cùng group nếu không đặt `groupId` -> rebalance chéo ([sk-retry]).

### 2.4 Kịch bản 4 - Idempotent consumer / outbox

Không có trang Spring Kafka/Boot nào về idempotent consumer hay outbox (đã duyệt mục lục tại [sk-index]; chỉ có "Tips, Tricks and Examples" - không fetch). Thành phần chính thức dùng để dựng (suy ra): bảng "đã xử lý" (khoá `topic-partition-offset` hoặc id nghiệp vụ trong header, header có sẵn qua `ConsumerRecordMetadata` ([sk-annot])); `@Transactional` + `KafkaTemplate` để gửi trong cùng giao dịch ([sk-transactions]); `DLT_ORIGINAL_*` header để truy vết ([sk-errh]). Lý do cần: idempotence của producer chỉ trong một session; docs EOS: "(The read and process have at least once semantics)" ([sk-eos]).

### 2.5 Kịch bản 5 - Transactions / EOS

Boot ([sk-transactions], [sk-boot]): "With Spring Boot, it is only necessary to set the `spring.kafka.producer.transaction-id-prefix` property - Spring Boot will automatically configure a `KafkaTransactionManager` bean and wire it into the listener container." Property liên quan: `spring.kafka.template.transaction-id-prefix`, `spring.kafka.template.allow-non-transactional` ([jar-meta]).
```yaml
spring.kafka.producer.transaction-id-prefix: tx-demo-      # mỗi instance phải khác prefix
spring.kafka.consumer.isolation-level: read-committed      # tên enum (suy ra từ KafkaProperties$IsolationLevel, default read-uncommitted)
spring.kafka.consumer.enable-auto-commit: false
```
- `transactional.id` = prefix + n; "the `transactionIdPrefix` must be unique per instance"; `EOSMode.V2` là duy nhất (broker >= 2.5) ([sk-transactions], [sk-eos]).
- Producer-only transaction ([sk-transactions]):
```java
boolean result = template.executeInTransaction(t -> {
    t.sendDefault("thing1", "thing2");
    t.sendDefault("cat", "hat");
    return true;
});
// hoặc @Transactional trên method có template.send(...) + ghi DB
```
- Consume-process-produce: container (có `KafkaAwareTransactionManager`) mở transaction, `sendOffsetsToTransaction`, rollback + seek lại khi listener ném lỗi; với transaction thì **không** có error handler mặc định, dùng `AfterRollbackProcessor` (mặc định giao lại record lỗi, cắt "after ten failures ... logged") ([sk-eos], [sk-errh]).
- `transactional.id` ngầm bật idempotence; transaction cần cụm >= 3 broker; `transaction.timeout.ms=60000` ([kf-producer]). Consumer `read_committed` chỉ thấy tới LSO ([kf-consumer]). "Using transactions enables Exactly Once Semantics ... for a read -> process -> write sequence" nhưng chỉ phạm vi Kafka ([sk-eos]).
- `ChainedKafkaTransactionManager` deprecated từ 2.7: dùng `KafkaTransactionManager` ở container + `@Transactional` cho DB ([sk-transactions]). Boot 4.1.2 vẫn nhắc nó ở docs Boot ([sk-boot]) - ưu tiên docs Spring Kafka.

### 2.6 Kịch bản 6 - Tạo topic bằng code

Boot tự đăng ký `KafkaAdmin`; chỉ cần bean `NewTopic`/`NewTopics`; "If the topic already exists, the bean is ignored" ([sk-config-topics], [sk-boot]).
```java
// [sk-config-topics]
@Bean public NewTopic topic1() {
    return TopicBuilder.name("thing1").partitions(10).replicas(3).compact().build();
}
@Bean public NewTopic topic2() {
    return TopicBuilder.name("thing2").partitions(10).replicas(3)
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd").build();
}
// (suy ra) cho lab durability:
@Bean public NewTopic payments() {
    return TopicBuilder.name("payments").partitions(3).replicas(3)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2").build();
}
```
- API `TopicBuilder` (mã nguồn v4.1.2): `name`, `partitions`, `replicas`, `replicasAssignments`, `assignReplicas`, `configs(Map)`, `config(k,v)`, `compact()`, `build()` - **không có** `minInsyncReplicas` ([sk-topicbuilder-src]). `TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG` thuộc `org.apache.kafka.common.config.TopicConfig` (suy ra, tên chuỗi `min.insync.replicas` đã xác nhận ở [kf-topic]).
- Bỏ `partitions()`/`replicas()` -> dùng default broker (cần broker >= 2.4) ([sk-config-topics]); default broker nguy hiểm (replicas=1) - xem 0001.
- Topic đã có: chỉ **tăng** partition nếu ít hơn; docs không nói gì về sửa config ([sk-config-topics]). Boot: `spring.kafka.admin.modify-topic-configs` default `false`, `spring.kafka.admin.auto-create` default `true`, `spring.kafka.admin.fail-fast` default `false` ([jar-meta]) -> đổi config topic đã tồn tại phải bật `modify-topic-configs=true` (suy ra từ tên + mô tả "enable modification of existing topic configuration").
- Broker không sẵn sàng: mặc định chỉ log, context vẫn lên; đặt `fatalIfBrokerNotAvailable=true` (Boot: `spring.kafka.admin.fail-fast`) để dừng ([sk-config-topics], [jar-meta]). Chạy lại thủ công: `admin.initialize()`.
- Kiểm tra lúc chạy: `KafkaAdmin.describeTopics`, `createOrModifyTopics`, `deleteTopics` (từ 4.0), hoặc `AdminClient.create(admin.getConfigurationProperties())` ([sk-config-topics]). `Predicate<NewTopic>` qua `admin.setCreateOrModifyTopic(...)` ([sk-config-topics]).

---

## 3. Hành vi quan trọng cho "không mất message" (bảng nhanh)

| Chủ đề | Hành vi | Nguồn |
|---|---|---|
| Boot đổi default producer? | Không: `acks`, `retries`, `batch-size`... đều `None` -> dùng default client (4.x: `acks=all`, idempotence `true`) | [jar-meta], [kf-producer] |
| `spring.kafka.listener.ack-mode` | `None` trong Boot -> `ContainerProperties.ackMode` mặc định `BATCH` | [jar-meta], [sk-props] |
| `spring.kafka.consumer.enable-auto-commit` | `None` trong Boot; Spring ép `false` nếu không đặt tường minh (từ 2.3); Kafka thuần mặc định `true` | [jar-meta], [sk-container], [kf-consumer] |
| `auto.offset.reset` | `latest` (Kafka); Boot mặc định None. Quick tour đặt `spring.kafka.consumer.auto-offset-reset=earliest` | [kf-consumer], [sk-quick] |
| `DefaultErrorHandler` | 10 lần giao, 0ms, hết thì log ERROR + commit (bỏ record). Lỗi `Error` bỏ qua handler, consumer dừng | [sk-errh] |
| DLT | `<topic>-dlt`, cùng partition; header `DLT_*`; `failIfSendResultIsError=true` | [sk-errh], [sk-dlpr-src] |
| `isAckAfterHandle` | mặc định `true`: commit offset khi handler trả bình thường | [sk-errh] |
| `commitRecovered` | chỉ cho `MANUAL_IMMEDIATE` | [sk-errh] |
| Nhìn thấy lỗi gửi | `whenComplete` / `get(timeout)`; `LoggingProducerListener` chỉ log | [sk-send] |
| `KafkaTemplate` producer | singleton dùng chung (không transaction); `flush()` có thể làm chậm thread khác | [sk-send] |
| Rủi ro mất tại DLT | topic `*-dlt` phải đủ partition, nếu thiếu partition: recoverer đặt partition=null (kiểm `verifyPartition`) | [sk-errh] |
| `@RetryableTopic` + auto topic | replication factor `-1` (broker default) | [sk-retry] |

---

## 4. Lab ergonomics

- Cụm ngoài: chỉ cần `spring.kafka.bootstrap-servers` (xem 2.0). Không cần Testcontainers/Embedded.
- Embedded: `@EmbeddedKafka` từ `spring-kafka-test`, KRaft, `bootstrapServersProperty` tự đặt `spring.kafka.bootstrap-servers` (từ 3.0.10); khuyên `@DirtiesContext` ([sk-testing], [sk-boot]). Chỉ có `EmbeddedKafkaKraftBroker` ([sk-testing]).
- Testcontainers: docs Spring Kafka **không nhắc**; Boot docs chỉ link tới trang Testcontainers (nội dung không fetch được); BOM Boot có quản lý `testcontainers.version=2.0.5` ([mvn-deps]). Chỉ ghi chú, lab dùng cụm Docker ngoài.
- Mock không cần broker: `MockConsumerFactory`/`MockProducerFactory` (từ 3.0.7) ([sk-testing]).
- Chạy nhiều kịch bản trong một app (suy ra, cơ chế Spring Boot chuẩn): `@Profile("s1-producer")` trên `@Component implements CommandLineRunner` + `application-s1-producer.yml`; chạy `mvn spring-boot:run -Dspring-boot.run.profiles=s1-producer`. Docs Spring Kafka dùng `ApplicationRunner` với `template.send(...)` làm ví dụ ([sk-quick]); phần `@Profile` và `-Dspring-boot.run.profiles` **chưa fetch docs** -> xem Unverified.
- Hiệu ứng khởi động: `@KafkaListener` tự khởi động; muốn chỉ chạy producer thì dùng profile khác nhau, hoặc `spring.kafka.listener.auto-startup=false` (có trong metadata, default true) ([jar-meta]). Có `@KafkaListener(autoStartup="${...}")` ([sk-annot]).
- Log rebalance: container mặc định có logging rebalance listener ở INFO ([sk-rebalance]).
- Đặt `client-id`: `spring.application.name` làm tiền tố client id ([sk-connecting]).

---

## 5. Khung dự án tối thiểu (1 app, nhiều profile)

Cây file (suy ra):
```
kafka-lab/
  pom.xml
  src/main/java/lab/Application.java                 # @SpringBootApplication
  src/main/java/lab/s1/ProducerDurabilityRunner.java # @Profile("s1")  CommandLineRunner
  src/main/java/lab/s2/ConsumerListener.java         # @Profile("s2")  @KafkaListener
  src/main/java/lab/s3/ErrorHandlingConfig.java      # @Profile("s3")  DefaultErrorHandler + DLT
  src/main/java/lab/s4/IdempotentConsumer.java       # @Profile("s4")
  src/main/java/lab/s5/TxRunner.java                 # @Profile("s5")
  src/main/java/lab/TopicsConfig.java                # NewTopic beans (mọi profile)
  src/main/resources/application.yml                 # chung: bootstrap-servers
  src/main/resources/application-s1.yml ... s5.yml
```

`pom.xml` tối thiểu (các phần tử đã kiểm ở mục 1; `spring-boot-maven-plugin` không ghi version vì parent quản lý [mvn-parent]):
```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-parent</artifactId>
    <version>4.1.2</version><relativePath/>
  </parent>
  <groupId>lab</groupId><artifactId>kafka-lab</artifactId><version>0.0.1-SNAPSHOT</version>
  <properties><java.version>17</java.version></properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-kafka</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-kafka-test</artifactId><scope>test</scope></dependency>
  </dependencies>
  <build><plugins>
    <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
  </plugins></build>
</project>
```
Muốn `@Transactional` + JPA/JDBC cho kịch bản 4 cần thêm starter khác - chưa kiểm (xem Unverified). Muốn `kafka-clients` 4.4.0: thêm `<kafka.version>4.4.0</kafka.version>` ([sk-override]) - chưa kiểm tương thích.

`application.yml` (chung):
```yaml
spring:
  application:
    name: kafka-lab
  kafka:
    bootstrap-servers: localhost:29092,localhost:39092,localhost:49092
    admin:
      auto-create: true          # default true [jar-meta]
      fail-fast: false           # default false; đặt true để biết sớm khi cụm chưa lên
```

`application-s1.yml` (producer durability, đã kiểm tên ở 2.1):
```yaml
spring.kafka.producer:
  acks: all
  properties:
    "[enable.idempotence]": true
    "[delivery.timeout.ms]": 120000
    "[request.timeout.ms]": 30000
```
`application-s2.yml`:
```yaml
spring.kafka:
  consumer: { group-id: lab-s2, auto-offset-reset: earliest, enable-auto-commit: false, max-poll-records: 10 }
  listener: { ack-mode: manual_immediate, concurrency: 3 }
```
`application-s3.yml` (poison pill):
```yaml
spring.kafka.consumer:
  value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
  properties:
    "[spring.deserializer.value.delegate.class]": org.apache.kafka.common.serialization.StringDeserializer
```
`application-s5.yml` (transaction):
```yaml
spring.kafka:
  producer.transaction-id-prefix: tx-lab-
  consumer: { isolation-level: read-committed, enable-auto-commit: false }
```
Các khoá trong 4 file trên đều có trong metadata ([jar-meta]) trừ giá trị enum `manual_immediate`/`read-committed` (suy ra, relaxed binding của enum `AckMode` / `IsolationLevel`).

`TopicsConfig.java` (ghép từ [sk-config-topics]): `NewTopic` cho `payments` (3 partitions, 3 replicas, `min.insync.replicas=2`), `payments-dlt` (>= số partition của `payments`), `payments-out` (kịch bản 5). Cụm 3 broker nên `replicas(3)` hợp lệ (suy ra).

---

## Đính chính sau khi chạy thử (2026-11-11)

- Mục 2.2 ghi "nếu factory không đặt ack mode thủ công thì Acknowledgment sẽ là null": **sai khi chạy thật**. Với Spring Kafka 4.1.2, listener khai báo tham số `Acknowledgment` mà container không ở chế độ manual thì ném `IllegalStateException: No Acknowledgment available as an argument, the listener container must have a MANUAL AckMode to populate the Acknowledgment.` và record bị xử lý như lỗi listener (DefaultErrorHandler thử 10 lần rồi bỏ qua). Phát hiện khi chạy `labs/05-scenarios` profile s6 trên broker nhúng.
- Mục 2.1/2.3: `template.send(...)` trong transaction mà ném lỗi ngay sau đó thì lô chưa gửi bị hủy (`TransactionAbortedException: Failing batch since transaction was aborted`); muốn thấy bản ghi aborted phải chờ `.get()` trước khi ném lỗi (labs/05-scenarios S10).

## Unverified

1. Tương thích kafka-clients **4.2.2** (Boot 4.1.2) với broker **4.4**: không có câu chính thức; chỉ có mốc "clients/brokers >= 2.1" cho 4.0 ([kf-upgrade]). Không fetch KIP-896.
2. Spring Kafka 4.1.2 chạy được với kafka-clients 4.4.0 khi override `kafka.version`: không có tuyên bố.
3. `waitForSendResultTimeout` của `DeadLetterPublishingRecoverer`: mã nguồn khởi tạo 30 giây nhưng Javadoc setter nói "delivery.timeout.ms + buffer" (mâu thuẫn) ([sk-dlpr-src]).
4. Giá trị chuỗi enum Boot (`manual_immediate`, `read-committed`) và cú pháp YAML `"[a.b]"` cho khoá có dấu chấm: lấy từ kiểu trong metadata + ví dụ ở [sk-boot]; chưa chạy thử.
5. `@Profile`, `CommandLineRunner`, `-Dspring-boot.run.profiles`: cơ chế Spring Boot chuẩn nhưng chưa fetch docs trong phiên này.
6. Idempotent consumer / outbox: không có docs chính thức Spring Kafka (mục 2.4 hoàn toàn là suy ra).
7. Docs Boot "Testcontainers with Kafka" không đọc được; docs Spring Kafka không nhắc Testcontainers.
8. Tên class `NotEnoughReplicasException(AfterAppend)` ở 2.1 lấy từ nghiên cứu 0001, không fetch lại ở phiên này.
9. Thuộc tính Boot `spring.kafka.listener.ack-mode` hợp lệ với giá trị chữ thường; `KafkaTransactionManager` có chia sẻ producer factory với `KafkaTemplate` đúng như docs nhưng chưa chạy thử với Boot 4.1.2.
10. Trang "Tips, Tricks and Examples" và `@RetryableTopic` attributes `attempts`/`dltStrategy` (không thấy trong [sk-retry]; chỉ thấy `@RetryableTopic(backOff=...)` trong [sk-history]) chưa kiểm.
11. Mốc "Apache Kafka current release 4.4.0" lấy từ lời nhắc đầu bài và `kafka-clients` 4.4.0 trên Central ([mvn-kc]); trang upgrade 4.4 không tự nói "current" ([kf-upgrade]).
12. Docker image/tag cho broker 4.4 không thuộc phạm vi nghiên cứu này.

---

## Sources

Tên | URL | Dùng cho
- [mvn-boot] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-starter-parent/maven-metadata.xml - danh sách phiên bản Boot (GA mới nhất 4.1.2; lastUpdated 2026-11-05)
- [mvn-sk] https://repo.maven.apache.org/maven2/org/springframework/kafka/spring-kafka/maven-metadata.xml - phiên bản spring-kafka (GA 4.1.2; RC 4.2.0-RC1)
- [mvn-deps] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-dependencies/4.1.2/spring-boot-dependencies-4.1.2.pom - `kafka.version`, `spring-kafka.version`, `testcontainers.version`, `spring-framework.version`
- [mvn-sk-pom] https://repo.maven.apache.org/maven2/org/springframework/kafka/spring-kafka/4.1.2/spring-kafka-4.1.2.pom - phụ thuộc: kafka-clients 4.2.2, không spring-retry
- [mvn-kc] https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml - kafka-clients mới nhất 4.4.0
- [mvn-parent] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-starter-parent/4.1.2/spring-boot-starter-parent-4.1.2.pom - java.version 17, plugin management
- [mvn-starter] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-starter-kafka/4.1.2/spring-boot-starter-kafka-4.1.2.pom - cấu thành starter
- [mvn-boot-kafka] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-kafka/4.1.2/spring-boot-kafka-4.1.2.pom - kéo spring-kafka
- [jar-meta] https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-kafka/4.1.2/spring-boot-kafka-4.1.2.jar - tải về, đọc `META-INF/spring-configuration-metadata.json`: toàn bộ `spring.kafka.*` và default
- [boot-sysreq] https://docs.spring.io/spring-boot/system-requirements.html - Java 17-27, Maven 3.6.3+
- [boot-mvn] https://docs.spring.io/spring-boot/maven-plugin/index.html - "Maven 3.6.3 or later"
- [sk-boot] https://docs.spring.io/spring-boot/reference/messaging/kafka.html - bootstrap-servers, `properties[...]`, NewTopic, transaction-id-prefix, embedded Kafka, bean tự gắn vào factory
- [sk-index] https://docs.spring.io/spring-kafka/reference/index.html - phiên bản docs 4.1.2 + mục lục
- [sk-quick] https://docs.spring.io/spring-kafka/reference/quick-tour.html - toạ độ, ví dụ Boot, compat (đã lệch)
- [sk-whatsnew] https://docs.spring.io/spring-kafka/reference/whats-new.html - thay đổi 4.1 (ackMode attr, sameIntervalTopicReuseStrategy...)
- [sk-history] https://docs.spring.io/spring-kafka/reference/appendix/change-history.html - thay đổi 4.0, bỏ spring-retry, CompletableFuture từ 3.0
- [sk-send] https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html - send signature, whenComplete, get(timeout), ProducerListener
- [sk-container] https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html - AckMode, enable.auto.commit, nack, concurrency, static membership
- [sk-props] https://docs.spring.io/spring-kafka/reference/kafka/container-props.html - default ackMode/ackCount/ackTime/syncCommits/eosMode
- [sk-annot] https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html - id/groupId/concurrency/properties/ackMode attr
- [sk-rebalance] https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/rebalance-listeners.html - ConsumerAwareRebalanceListener, KIP-848
- [sk-errh] https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html - DefaultErrorHandler, DLPR, ErrorHandlingDeserializer, AfterRollbackProcessor
- [sk-dlpr-src] https://raw.githubusercontent.com/spring-projects/spring-kafka/v4.1.2/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java - `-dlt`, failIfSendResultIsError
- [sk-serdes] https://docs.spring.io/spring-kafka/reference/kafka/serdes.html - ErrorHandlingDeserializer
- [sk-ehd-src] https://raw.githubusercontent.com/spring-projects/spring-kafka/v4.1.2/spring-kafka/src/main/java/org/springframework/kafka/support/serializer/ErrorHandlingDeserializer.java - tên property delegate
- [sk-retry] https://docs.spring.io/spring-kafka/reference/retrytopic/retry-config.html - @RetryableTopic, @DltHandler, autoCreateTopics
- [sk-naming] https://docs.spring.io/spring-kafka/reference/retrytopic/topic-naming.html - `-retry`/`-dlt`
- [sk-transactions] https://docs.spring.io/spring-kafka/reference/kafka/transactions.html - transactionIdPrefix, executeInTransaction, AfterRollback
- [sk-eos] https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html - EOSMode V2, read->process->write
- [sk-config-topics] https://docs.spring.io/spring-kafka/reference/kafka/configuring-topics.html - KafkaAdmin, NewTopic, TopicBuilder, describeTopics
- [sk-topicbuilder-src] https://raw.githubusercontent.com/spring-projects/spring-kafka/v4.1.2/spring-kafka/src/main/java/org/springframework/kafka/config/TopicBuilder.java - API TopicBuilder
- [sk-testing] https://docs.spring.io/spring-kafka/reference/testing.html - @EmbeddedKafka, KRaft, Mock factories
- [sk-override] https://docs.spring.io/spring-kafka/reference/appendix/override-boot-dependencies.html - override `kafka.version`/`spring-kafka.version`
- [sk-connecting] https://docs.spring.io/spring-kafka/reference/kafka/connecting.html - client id prefix từ spring.application.name
- [kf-upgrade] https://kafka.apache.org/44/getting-started/upgrade/ - tương thích client/broker mốc 2.1, Java 11/17
- [kf-producer] https://kafka.apache.org/44/configuration/producer-configs/ - default producer
- [kf-consumer] https://kafka.apache.org/44/configuration/consumer-configs/ - default consumer
- [kf-topic] https://kafka.apache.org/44/configuration/topic-configs/ - `min.insync.replicas`
