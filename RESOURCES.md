# Kafka Resources

Đã đọc trực tiếp vào 2026-11-10 (phiên bản docs: Kafka 4.4).

## Knowledge

- [Apache Kafka: Introduction](https://kafka.apache.org/intro)
  Định nghĩa event, topic, partition, key, replication, 5 API. Use for: khái niệm nền; trích dẫn thứ tự trong partition và key.
- [Apache Kafka: Design (Message Delivery Semantics)](https://kafka.apache.org/44/design/design/)
  Tài liệu thiết kế chính thức: at-most-once / at-least-once / exactly-once, idempotent producer, transactions, vị trí consumer. Use for: mọi bài về delivery semantics và commit offset.
- [Basic Kafka operations (4.4)](https://kafka.apache.org/44/operations/basic-kafka-operations/)
  Lệnh quản trị topic; mục Modifying topics: thêm partition, không giảm được, key có thể đổi partition. Use for: bài về partition, thao tác `kafka-topics.sh --alter`.
- [KRaft overview (4.4)](https://kafka.apache.org/44/operations/kraft/)
  Controller, `process.roles` (broker/controller/combined), quorum 2N+1, metadata log. Use for: giải thích controller, dựng cụm, khi nào không dùng combined mode.
- [Producer configs (4.4)](https://kafka.apache.org/44/configuration/producer-configs/)
  Mặc định và ý nghĩa `acks`, `enable.idempotence`, `retries`, `delivery.timeout.ms`, `linger.ms`, `batch.size`... Idempotence cần `acks=all`; cấu hình xung đột mà không bật idempotence tường minh thì idempotence tự tắt, bật tường minh thì `ConfigException` (đọc 2026-11-19). Use for: bảng tra cấu hình producer, mô phỏng tổng hợp.
- [Consumer configs (4.4)](https://kafka.apache.org/44/configuration/consumer-configs/)
  `group.id`, `enable.auto.commit`, `auto.offset.reset`, `max.poll.interval.ms`, `isolation.level`, `group.protocol`... Use for: bảng tra cấu hình consumer, rebalance.
- [Kafka Quickstart](https://kafka.apache.org/quickstart)
  Lệnh KRaft chính thức (format storage, tạo topic, console producer/consumer) và image `apache/kafka`. Use for: dựng cụm, thử CLI.
- [apache/kafka: docker examples](https://github.com/apache/kafka/tree/trunk/docker/examples)
  Compose mẫu cụm nhiều node (combined/isolated; plaintext, SSL, SASL). Use for: dựng cụm 3 broker.
- [Topic configs (4.4)](https://kafka.apache.org/44/configuration/topic-configs/)
  `min.insync.replicas`, `unclean.leader.election.enable`, retention, `cleanup.policy=compact`, `max.message.bytes`. Use for: bài độ bền, compaction, kích thước message.
- [Broker configs (4.4)](https://kafka.apache.org/44/configuration/broker-configs/)
  `default.replication.factor`, `num.partitions`, `auto.create.topics.enable`, `offsets.retention.minutes`, `group.consumer.*`. Use for: default phía broker (nguồn của nhiều lỗi), dựng cụm.
- [Upgrade notes 4.0-4.4](https://kafka.apache.org/44/getting-started/upgrade/)
  Thay đổi từng phiên bản: ZooKeeper bị gỡ (4.0), `linger.ms` 0→5 (4.0), ELR bật mặc định cho cụm mới (4.1), Share Groups. Use for: cảnh báo "tutorial cũ có thể sai".
- [Authorization and ACLs (4.4)](https://kafka.apache.org/44/security/authorization-and-acls/)
  ACL cần cho producer (Write) và consumer (Read trên Topic và Group). Use for: bài bảo mật client, hiểu lỗi quyền.
- [KafkaConsumer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) và [KafkaProducer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
  Commit offset, rebalance, thread-safety, phạm vi của idempotence, transactions. Use for: bài consumer, producer, transactions.
- KIP trên cwiki.apache.org: [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) (giao thức rebalance mới), [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) (Share Groups), [KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas) (ELR), [KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense) (hanging transaction). Use for: hiểu động cơ thiết kế khi docs chưa đủ.
- [Kafka Connect user guide (4.4)](https://kafka.apache.org/44/kafka-connect/user-guide/)
  `errors.tolerance`, dead letter queue của Connect. Use for: phân biệt DLQ có trong docs (Connect, Share Groups) với pattern tự dựng.
- Báo cáo nội bộ (2026-11-11): `research/0003-msk.md` (Amazon MSK) và `research/0004-confluent-cloud.md` (Confluent Cloud): khác biệt so với Apache Kafka, mặc định, giới hạn, 8 câu hỏi cho platform team. Tóm tắt ở `reference/0004-managed-kafka-msk-vs-confluent.html`. Nguồn gốc: docs.aws.amazon.com/msk, docs.confluent.io/cloud.
- Báo cáo nội bộ (2026-11-12): `research/0005-udemy-kafka-theory-comparison.md`: so mục "Kafka Theory" của khoá Udemy "Learn Apache Kafka for Beginners" (Stéphane Maarek; curriculum lấy từ trang khoá, chỉ dùng làm bản đồ chủ đề, không làm nguồn kỹ thuật) với bài 1-9; liệt kê khái niệm còn thiếu kèm nguồn chính.
- Báo cáo nội bộ: `research/0001-curriculum-coverage.md` (2026-11-10) — 16 tình huống oái oăm có nguồn, khoảng trống lộ trình, đối chiếu thay đổi 4.0-4.4. WebFetch trả bản tóm tắt, nên đối chiếu lại câu trích nguyên văn trên trang gốc trước khi đưa vào lesson.
- [Docker Engine: install on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) và [post-install (Linux)](https://docs.docker.com/engine/install/linux-postinstall/)
  Lệnh cài Docker Engine bằng apt repo (Ubuntu 24.04 được hỗ trợ), nhóm `docker`, bật cùng systemd. Use for: wizard bài 3. Trang cài đặt không nhắc WSL.
- [WSL: advanced settings (wsl.conf / .wslconfig)](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)
  Bật systemd (`[boot] systemd=true`, `wsl.exe --shutdown`), `localhostForwarding` mặc định true. Use for: bài 3, lỗi kết nối từ Windows.
- [Confluent Developer: Apache Kafka 101](https://developer.confluent.io/courses/apache-kafka/events/)
  16 module ngắn (topics, partitions, brokers, replication, producers, consumers, Schema Registry, Connect...). Trang không nêu giá, chân trang có mục "Free Courses". Use for: xem/nghe bổ trợ sau mỗi bài.
- [Book: _Kafka: The Definitive Guide_, 2nd ed. (Shapira, Palino, Sivaram, Petty; O'Reilly, 2021)](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/)
  Sách tham chiếu chính, mức beginner-intermediate. Use for: đào sâu sau mỗi chặng (producer, consumer, reliability, exactly-once).

- [kafka-clients 4.2.0 source: ConsumerCoordinator, ApplicationEventProcessor, CommitRequestManager, AsyncKafkaConsumer](https://github.com/apache/kafka/tree/4.2.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals)
  Source code (primary). Auto-commit chỉ được xét trong `poll()` (classic: `ConsumerCoordinator.poll` → `maybeAutoCommitOffsetsAsync`; consumer/KIP-848: sự kiện poll → `updateTimerAndMaybeCommit`) và khi đổi assignment; giá trị là `subscriptions.allConsumed()`. `close()` commit đồng bộ chỉ khi auto-commit bật. Use for: câu hỏi "commit lúc nào" mà javadoc không nói thẳng. Đọc ngày 2026-11-13.
- [Spring Kafka 4.1.2 source: KafkaMessageListenerContainer](https://github.com/spring-projects/spring-kafka/blob/v4.1.2/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java)
  Source code (primary). `ackCurrent` ghi nhận offset sau khi xử lý từng record (BATCH); `wrapUp` gọi `commitPendingAcks()` trước `consumer.close()` khi dừng bình thường, không có khi lỗi fatal hoặc có `KafkaTransactionManager`. Use for: Spring có commit khi tắt ứng dụng không. Đọc ngày 2026-11-13.

- Báo cáo nội bộ (2026-11-17): `research/0006-throughput-latency.md` (batching, `linger.ms` 5 từ 4.0, nén, `acks` và độ trễ, fetch, partition và song song, lag, backpressure Spring, quota/throttle), `research/0007-replay-retention-audit.md` (retention theo segment, compaction, tiered storage, reset offset, seek theo timestamp, CreateTime/LogAppendTime, giới hạn khi làm audit log), `research/0008-schema-evolution.md` (Schema Registry, compatibility, wire format, AWS Glue Schema Registry, Spring). Use for: bài quá tải, bài replay/audit, bài schema.
- [Monitoring (4.4)](https://kafka.apache.org/44/operations/monitoring/)
  Bảng metric JMX của broker, producer, consumer: `records-lag-max` (theo vị trí hiện tại, không theo offset đã commit), `bufferpool-wait-ratio`, `produce-throttle-time-*`, `fetch-throttle-time-*`, `throttle-time` của quota. Use for: quan sát quá tải, lag, throttling.
- [Design: Efficiency, Log Compaction, Quotas (4.4)](https://kafka.apache.org/44/design/design/)
  Cùng trang Design ở trên, các mục khác: batching và nén end-to-end, `sendfile` không dùng khi bật SSL, 4 đảm bảo của compaction, quota tính theo broker và cách broker throttle (mute channel). Use for: bài thông lượng, compaction, quota.
- [Implementation: Log (4.4)](https://kafka.apache.org/44/implementation/log/)
  Xoá theo segment, retention theo timestamp lớn nhất của segment, `OutOfRangeException`, CRC khi khôi phục. Use for: giải thích retention chính xác, bẫy timestamp cũ.
- [Tiered Storage (4.4)](https://kafka.apache.org/44/operations/tiered-storage/), [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) và [Tiered Storage GA Release Notes](https://cwiki.apache.org/confluence/x/9xDOEg)
  "production-ready since Kafka 3.9", không có RemoteStorageManager sẵn, không hỗ trợ topic compact, một partition remote mỗi fetch. Use for: câu hỏi giữ dữ liệu lâu, audit nhiều năm.
- [ProducerRecord Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html)
  CreateTime (producer gán) và LogAppendTime (broker ghi đè). Use for: bài timestamp, audit.
- [apache/kafka 4.4.0 source: GroupOffsetsResetter, Utils.getDateTime](https://github.com/apache/kafka/blob/4.4.0/tools/src/main/java/org/apache/kafka/tools/GroupOffsetsResetter.java)
  Source code (primary). `--to-datetime` không có múi giờ thì hiểu là UTC; partition không có offset theo timestamp thì về log end. Use for: replay theo giờ Việt Nam. Đọc ngày 2026-11-17.
- [Spring Kafka: How the pattern works (non-blocking retries)](https://docs.spring.io/spring-kafka/reference/retrytopic/how-the-pattern-works.html)
  Record lỗi được chuyển sang retry topic kèm mốc back off; consumer của retry topic `pause` partition tới khi tới hạn; hết lượt thì DLT; "you lose Kafka's ordering guarantees for that topic"; gợi ý `AckMode.RECORD`. Đọc 2026-11-19. Use for: Bài 6, Bài 11 (cách ly nhà cung cấp).
- [KafkaConsumer javadoc: Consumption Flow Control](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
  Mỗi poll "fetch sequentially" từ offset đã đọc; `pause()`/`resume()` để tập trung đọc một số partition trước (ví dụ: lấy dữ liệu mới của vài topic trước khi đọc topic khác). Đọc 2026-11-19. Use for: làn ưu tiên (Bài 11).
- [Spring Kafka: Seeking to a Specific Offset](https://docs.spring.io/spring-kafka/reference/kafka/seek.html), [Pausing and Resuming Listener Containers](https://docs.spring.io/spring-kafka/reference/kafka/pause-resume.html), [Pausing and Resuming Partitions](https://docs.spring.io/spring-kafka/reference/kafka/pause-resume-partitions.html), [Listener Container Properties](https://docs.spring.io/spring-kafka/reference/kafka/container-props.html)
  `ConsumerSeekAware`, `seekToTimestamp`, `AbstractConsumerSeekAware`; `pause()` vẫn `poll()` nên không rebalance; `pauseImmediate`, `idleBetweenPolls`. Use for: replay và backpressure trong Spring.
- [Confluent Schema Registry: Concepts](https://docs.confluent.io/platform/current/schema-registry/fundamentals/index.html), [Schema Evolution and Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html), [Formats, Serializers, and Deserializers](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
  Docs chính thức của Confluent (chỉ dùng cho Schema Registry). Subject, schema ID, compatibility (default BACKWARD, không transitive), bảng thay đổi được phép theo Avro/Protobuf/JSON Schema, subject name strategy, wire format (magic byte + 4 byte ID, hoặc GUID trong header từ CP 8.1.1), `auto.register.schemas` mặc định true. Use for: bài schema, thiết kế event nhiều team.
- [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) và [Java implementation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-gs-serde-java.html)
  8 compatibility mode (`_ALL` = transitive, `DISABLED`), IAM, miễn phí; serializer mặc định không auto-register, compatibility BACKWARD. Use for: nếu nơi mới dùng MSK + Glue.
- [Amazon MSK: Best practices for Apache Kafka clients](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html), [Monitor consumer lags](https://docs.aws.amazon.com/msk/latest/developerguide/consumer-lag.html), [Tiered storage](https://docs.aws.amazon.com/msk/latest/developerguide/msk-tiered-storage.html)
  Khuyến nghị của AWS (không phải default Apache): `linger.ms` >= 5 (25 cho đa số), `batch.size` 64-128 KB, nén lz4/zstd; điều kiện metric lag biến mất; tiered tối thiểu 3 ngày ở tầng rẻ. Use for: đối chiếu cấu hình client với môi trường MSK.
- [Multi-tenancy (4.4)](https://kafka.apache.org/44/operations/multi-tenancy/)
  Đọc 2026-11-18. Khuyến nghị đặt tên topic phân cấp: `<organization>.<team>.<dataset>.<event-name>` hoặc `<project>.<product>.<event-name>`; không đưa vào tên thứ hay đổi (tên consumer) hay metadata có sẵn chỗ khác (số partition, cấu hình); không đổi tên topic tại chỗ được; ép tiền tố bằng prefix ACL (KIP-290) hoặc `CreateTopicPolicy`. Use for: quy ước đặt tên topic, phân quyền theo tiền tố.
- [Topic.java (4.4.0)](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/internals/Topic.java) và [TopicCommand.java](https://github.com/apache/kafka/blob/4.4.0/tools/src/main/java/org/apache/kafka/tools/TopicCommand.java)
  Đọc 2026-11-18. Tên topic: ký tự `[a-zA-Z0-9._-]`, tối đa 249, cấm `.` và `..`; `.` và `_` va nhau trong tên metric ("best to use either, but not both"). Use for: luật cứng khi đặt tên topic.

## Wisdom (Communities)

- [Apache Kafka mailing lists (users@, dev@)](https://kafka.apache.org/community/contact/)
  Kênh chính thức; có lưu trữ công khai. Use for: câu hỏi cấu hình/hành vi, đọc cách maintainer trả lời.
- [Confluent Community Forum](https://forum.confluent.io/)
  Diễn đàn Discourse, bài lưu lâu dài (Slack của Confluent xoá tin sau ~2 tuần). Use for: hỏi cách làm thực tế, tìm lỗi người khác từng gặp.
- Nội bộ: đồng nghiệp ở ngân hàng mới (platform/Kafka team). Use for: chuẩn đặt tên topic, bảo mật, quy trình của riêng nơi đó.

## Gaps

- Docs Apache không mô tả retry topic/DLQ cho consumer thường, outbox, claim-check: đây là pattern ứng dụng. Cần nguồn tin cậy khác, hoặc ghi rõ "pattern tự dựng" trong lesson (trước bài 6 và 7).
- Đã có nguồn (2026-11-17): Schema Registry/Avro (`research/0008`), tên config quota `producer_byte_rate`/`consumer_byte_rate`/`request_percentage` (`research/0006`). Còn thiếu: quy tắc đổi tên field (Avro aliases, spec Avro chưa nằm trong danh sách nguồn), wire format chi tiết của AWS Glue.
- Chưa kiểm chứng: cấu hình client SASL/SSL (trước bài bảo mật); Spring Kafka và Confluent (chỉ cần nếu nơi mới dùng); biến môi trường của image `apache/kafka` cho `min.insync.replicas` (kiểm bằng thực nghiệm ở bài 3).
- Chưa hỏi người học có muốn tham gia cộng đồng hay không.
