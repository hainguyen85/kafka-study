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
  Mặc định và ý nghĩa `acks`, `enable.idempotence`, `retries`, `delivery.timeout.ms`, `linger.ms`, `batch.size`... Use for: bảng tra cấu hình producer.
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
- Báo cáo nội bộ: `research/0001-curriculum-coverage.md` (2026-11-10) — 16 tình huống oái oăm có nguồn, khoảng trống lộ trình, đối chiếu thay đổi 4.0-4.4. WebFetch trả bản tóm tắt, nên đối chiếu lại câu trích nguyên văn trên trang gốc trước khi đưa vào lesson.
- [Docker Engine: install on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) và [post-install (Linux)](https://docs.docker.com/engine/install/linux-postinstall/)
  Lệnh cài Docker Engine bằng apt repo (Ubuntu 24.04 được hỗ trợ), nhóm `docker`, bật cùng systemd. Use for: wizard bài 3. Trang cài đặt không nhắc WSL.
- [WSL: advanced settings (wsl.conf / .wslconfig)](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)
  Bật systemd (`[boot] systemd=true`, `wsl.exe --shutdown`), `localhostForwarding` mặc định true. Use for: bài 3, lỗi kết nối từ Windows.
- [Confluent Developer: Apache Kafka 101](https://developer.confluent.io/courses/apache-kafka/events/)
  16 module ngắn (topics, partitions, brokers, replication, producers, consumers, Schema Registry, Connect...). Trang không nêu giá, chân trang có mục "Free Courses". Use for: xem/nghe bổ trợ sau mỗi bài.
- [Book: _Kafka: The Definitive Guide_, 2nd ed. (Shapira, Palino, Sivaram, Petty; O'Reilly, 2021)](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/)
  Sách tham chiếu chính, mức beginner-intermediate. Use for: đào sâu sau mỗi chặng (producer, consumer, reliability, exactly-once).

## Wisdom (Communities)

- [Apache Kafka mailing lists (users@, dev@)](https://kafka.apache.org/community/contact/)
  Kênh chính thức; có lưu trữ công khai. Use for: câu hỏi cấu hình/hành vi, đọc cách maintainer trả lời.
- [Confluent Community Forum](https://forum.confluent.io/)
  Diễn đàn Discourse, bài lưu lâu dài (Slack của Confluent xoá tin sau ~2 tuần). Use for: hỏi cách làm thực tế, tìm lỗi người khác từng gặp.
- Nội bộ: đồng nghiệp ở ngân hàng mới (platform/Kafka team). Use for: chuẩn đặt tên topic, bảo mật, quy trình của riêng nơi đó.

## Gaps

- Docs Apache không mô tả retry topic/DLQ cho consumer thường, outbox, claim-check: đây là pattern ứng dụng. Cần nguồn tin cậy khác, hoặc ghi rõ "pattern tự dựng" trong lesson (trước bài 6 và 7).
- Chưa kiểm chứng: Schema Registry/Avro (cần trước bài 8); cấu hình client SASL/SSL và tên config quota (trước bài bảo mật); Spring Kafka và Confluent (chỉ cần nếu nơi mới dùng); biến môi trường của image `apache/kafka` cho `min.insync.replicas` (kiểm bằng thực nghiệm ở bài 3).
- Chưa hỏi người học có muốn tham gia cộng đồng hay không.
