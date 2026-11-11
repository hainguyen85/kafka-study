<!-- Tự sinh từ 0002-first-day-questions.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bảng tra · [Khái niệm](0001-core-concepts.md)

# 8 câu hỏi cho ngày đầu ở nơi mới

**Đã có bản mới hợp với dịch vụ managed (MSK / Confluent Cloud): [xem bảng này](0004-managed-kafka-msk-vs-confluent.md).** Bản dưới đây là bản đầu, viết khi còn nghĩ cụm là Apache Kafka tự vận hành. Mỗi câu có một lý do kỹ thuật: câu trả lời đổi cách bạn cấu hình. Cập nhật 2026-11-10. Nguồn: [research/0001, mục 4](../research/0001-curriculum-coverage.md) và mục 1-2 cho phần giải thích.

| # | Câu hỏi | Đã biết | Vì sao quan trọng |
|---|---|---|---|
| 1 | **Cụm là loại nào, phiên bản broker bao nhiêu?** | Apache Kafka tự vận hành. **Phiên bản: chưa biết, cần hỏi.** | Tính năng phụ thuộc phiên bản: giao thức rebalance mới KIP-848 (GA từ 4.0), ELR (bật mặc định cho cụm mới từ 4.1), Share Groups (production-ready từ 4.2). [[Upgrade notes]](https://kafka.apache.org/44/getting-started/upgrade/) |
| 2 | **Client dùng thư viện nào?** | Spring Kafka. | Demo trong workspace viết bằng Spring Kafka. Cần hỏi thêm: phiên bản Spring Boot / Spring Kafka, có dùng Kafka Streams hay Connect không (giao thức rebalance mới chưa hỗ trợ Connect, theo [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)). |
| 3 | **Ai tạo và cấu hình topic (partition, RF, min.insync.replicas, retention)?** | Bạn tự tạo. Cần hỏi: có tắt `auto.create.topics.enable` không, và quy ước đặt tên. | Mặc định broker nguy hiểm: `num.partitions=1`, `default.replication.factor=1`, `min.insync.replicas=1`, tự tạo topic bật sẵn. Gõ sai tên topic sẽ tạo "topic ma" với RF=1. [[Broker configs]](https://kafka.apache.org/44/configuration/broker-configs/) |
| 4 | **Bảo mật: SASL hay SSL/mTLS? ACL theo topic hay prefix?** | Bỏ qua ở giai đoạn study. Có thể SSL/mTLS theo topic (chưa chắc). | Consumer cần quyền Read trên cả Topic *và* Group; thiếu một trong hai là lỗi quyền khó hiểu. [[ACL]](https://kafka.apache.org/44/security/authorization-and-acls/) Hỏi lại khi cần kết nối cụm thật. |
| 5 | **Yêu cầu giao hàng của nghiệp vụ chính?** | Không mất message. Cần hỏi thêm: có chấp nhận trùng (nếu consumer chống trùng) không, hay đòi "exactly-once" với DB? | Hướng "không mất" = ghi bền (`acks=all`, RF=3, `min.insync.replicas=2`) + at-least-once ở consumer + chống trùng. Docs giới hạn exactly-once trong Kafka/Streams; ghi sang hệ thống ngoài cần phối hợp. [[Design]](https://kafka.apache.org/44/design/design/) |
| 6 | **Thứ tự ràng buộc theo khoá nào (tài khoản, đơn, thiết bị)? Dự kiến throughput và số partition?** | Chưa biết. | Quyết định key và số partition; số partition không giảm được và tăng sẽ làm key đổi partition. [[Basic operations]](https://kafka.apache.org/44/operations/basic-kafka-operations/) |
| 7 | **Định dạng dữ liệu: Avro, Protobuf hay JSON? Có Schema Registry của ai? Quy ước versioning?** | Chưa biết. | Ảnh hưởng thiết kế event và khả năng thay đổi sau này. Schema Registry không thuộc Apache Kafka lõi; chưa kiểm chứng nguồn (xem RESOURCES.md, mục Gaps). |
| 8 | **Cần nhiều trung tâm dữ liệu (DR) hoặc lưu giữ dài hạn / audit?** | Chưa biết. | Quyết định retention, compaction, và có dùng sao chép liên cụm không. [[Topic configs]](https://kafka.apache.org/44/configuration/topic-configs/) |

## Cách hỏi (gợi ý câu chữ)

- "Cụm đang chạy Kafka phiên bản nào? Bên mình nâng cấp theo chu kỳ nào?" (câu 1)
- "Topic mới tạo qua đâu: script, IaC hay tạo tay? Mặc định RF và min.insync.replicas là bao nhiêu?" (câu 3)
- "Với nghiệp vụ này, bên mình chấp nhận gửi trùng nếu phía nhận có chống trùng không?" (câu 5)
- "Event này cần giữ thứ tự theo khoá nào, và có yêu cầu giữ lại bao lâu?" (câu 6, 8)

Sau khi có câu trả lời thật, ghi lại vào `MISSION.md` (Constraints) để các bài sau bám đúng.
