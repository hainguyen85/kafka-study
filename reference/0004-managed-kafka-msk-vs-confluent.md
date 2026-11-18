<!-- Tự sinh từ 0004-managed-kafka-msk-vs-confluent.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bảng tra · [Sổ tay tình huống](0003-tricky-scenarios.md) · [8 câu hỏi (bản đầu)](0002-first-day-questions.md)

# Kafka managed trên AWS: MSK và Confluent Cloud khác Kafka thuần ở đâu

Bạn chưa biết nơi mới dùng dịch vụ nào, nên đây là bản đồ cho cả hai. Cập nhật 2026-11-11. Nguồn: [research/0003 (MSK)](../research/0003-msk.md), [research/0004 (Confluent Cloud)](../research/0004-confluent-cloud.md), đều lấy từ tài liệu chính thức của AWS, Confluent và Apache; số liệu có thể đổi theo thời gian.

## 1. Ba điều cần biết trước tiên

1. **Phần lớn "an toàn" đã được nền tảng đặt sẵn: RF=3 và min ISR 2.** Nhưng mỗi dịch vụ khoá một số cấu hình khác nhau, nên câu "tôi sẽ đặt `min.insync.replicas` cho topic" có thể không làm được.
2. **Có một bẫy mặc định nguy hiểm trên MSK Standard:** `unclean.leader.election.enable` mặc định là **true** (Apache và Express/Confluent: false). Bật nghĩa là chấp nhận mất dữ liệu để partition sống lại.
3. **Auto-create topic tắt ở cả hai** (Apache thì bật). Topic phải được tạo có chủ đích, và bạn cần có quyền (IAM hoặc RBAC) để tạo.

## 2. So sánh mặc định và giới hạn

| Hạng mục | Apache 4.4 (lab của bạn) | MSK Standard | MSK Express | MSK Serverless | Confluent Cloud |
|---|---|---|---|---|---|
| Replication factor | 1 | 3 (cụm 3 AZ); 2 (cụm 2 AZ) | 3, khoá; tạo topic RF khác sẽ bị đổi thành 3 im lặng | 2 replica ở 2 AZ khác nhau (FAQ) | 3, cố định |
| `min.insync.replicas` | 1 | 2 (3 AZ); 1 (2 AZ) | 2, khoá | không sửa được; giá trị hiệu lực chưa rõ | mặc định 2; chỉ nhận 1 hoặc 2 |
| `unclean.leader.election.enable` | false | **true** (non-tiered); false (tiered) | false, khoá | không sửa được | false, khoá |
| `auto.create.topics.enable` | true | false | false | không nêu rõ | tắt (Dedicated); loại khác chưa rõ |
| `num.partitions` mặc định | 1 | 1 | 1 | - | 6 |
| Kích thước message | ~1 MB | mặc định Apache, chỉnh được | chỉnh được | tối đa 8 MiB | mặc định ~2 MB; tối đa 8 MB (Basic, Standard) hoặc 20 MB (Enterprise, Dedicated) |
| Sửa cấu hình broker | tự do | qua "custom configuration" (danh sách giới hạn) | hầu hết read-only | không sửa được | chỉ Dedicated (6 setting) |
| Sửa cấu hình topic | tự do | qua Kafka CLI/Admin | một phần bị bảo vệ | chỉ 7 mục (vd. `retention.ms`, `max.message.bytes`) | danh sách cho phép, có giới hạn giá trị |
| Phiên bản Kafka | 4.4 | tới 4.1.x (3.9.x là "Recommended") | 3.6/3.8/3.9/4.2 | không công bố trong tài liệu đã đọc | không công bố; có tính năng client 4.4 |
| Giao thức rebalance mới (KIP-848) | GA từ 4.0 | có từ MSK 4.0 | MSK 4.2 | chưa rõ | có từ 2025-04-28 |
| Share Groups (KIP-932) | production-ready từ 4.2 | 4.1 chỉ preview | chưa hỗ trợ | chưa rõ | chỉ Enterprise và Dedicated |
| Xác thực | tuỳ cấu hình | IAM (9098), SASL/SCRAM (9096), mTLS (9094) | như Standard | chỉ IAM | API key (SASL_SSL, PLAIN) hoặc OAuth |

## 3. Điều này đổi gì trong cách bạn thiết kế

- **Cấu hình lab giống production hơn nếu bạn dùng RF=3, `min.insync.replicas=2`.** Lab 03/04/05 của bạn đã đặt như vậy, khớp với mặc định của cả hai dịch vụ.
- **Hỏi ai sở hữu "cluster configuration".** Trên MSK Standard nó quyết định `unclean.leader.election.enable`; bạn có thể ghi đè cho từng topic nếu được cấp quyền.
- **Bảo trì là chuyện thường:** broker bị khởi động lại lần lượt, client thấy lỗi leader thoáng qua (thường dưới vài giây). AWS khuyến nghị `retries` tối đa, `delivery.timeout.ms` ≥ 60 giây, `acks=all`, RF=3, `min.insync.replicas` ≤ RF-1 (đặt bằng RF sẽ chặn ghi khi rolling update). Producer Java trên Confluent Cloud mặc định thử lại 2 phút.
- **Tạo topic bằng code (`NewTopic`) cần quyền.** MSK: IAM `kafka-cluster:CreateTopic`; Serverless chỉ cho 2 request/giây với API quản lý topic. Express âm thầm đổi RF sang 3.
- **Client:** trên MSK, group id nên là ASCII, không có dấu hai chấm, nếu không metric lag trên CloudWatch không xuất hiện; AWS khuyến nghị luôn đặt `group.instance.id` và dùng assignor kiểu sticky.
- **Tính năng mới nhất của Kafka 4.4 (Share Groups, nhiều thứ của 4.2+) có thể không dùng được** trên cụm của bạn. Bài học cốt lõi (key, partition, offset, acks, commit, DLT, idempotent consumer, transaction) dùng được ở mọi nơi.

## 4. Tám câu hỏi cho platform team (gộp cho cả hai dịch vụ)

| # | Hỏi | Vì sao |
|---|---|---|
| 1 | Dịch vụ nào (MSK Standard/Express/Serverless, Confluent Basic/Standard/Enterprise/Dedicated/Freight)? Phiên bản Kafka? Có single-zone hay multi-zone? | Quyết định gần như mọi cấu hình bên dưới |
| 2 | Ai sở hữu cấu hình cụm? Giá trị hiện tại của `unclean.leader.election.enable`, `min.insync.replicas`, RF mặc định, auto-create topic? | MSK Standard mặc định `unclean`=true |
| 3 | Ai tạo/sửa topic và theo quy trình nào (script, IaC, Console)? Tài liệu thiết kế topic gửi ở đâu, ai duyệt? Quy ước đặt tên, trần số partition? | Bạn thiết kế topic, team riêng tạo khi go-live; auto-create tắt |
| 4 | Xác thực và mạng: IAM / SCRAM / mTLS hoặc API key / OAuth; PrivateLink, peering? | Cấu hình client Spring; bạn hoãn học bảo mật nhưng phải biết cách kết nối |
| 5 | Lịch bảo trì/nâng cấp và cách báo? MSK tự nâng version sau hết hạn hỗ trợ, không báo trước | Quyết định retry/timeout của client |
| 6 | Quota: partition mỗi broker/cụm, throughput, số connection (IAM: 3000/broker), số consumer group? | Số partition cho topic mới |
| 7 | Ai giám sát lag và under-replication? Dashboard, cảnh báo, mức monitoring? | Bạn cần biết lag ở đâu để xử lý sự cố |
| 8 | Schema (Avro/Protobuf/JSON, Schema Registry của ai)? DR (MSK Replicator, Cluster Linking), retention/audit? Có dùng transactions/exactly-once không? | Cluster Linking không tích hợp với transactions |

## Còn chưa chắc

- MSK Serverless: phiên bản Kafka, `min.insync.replicas` hiệu lực; AWS không có trang nói rõ transactions/exactly-once theo phiên bản.
- Confluent Cloud: loại cluster nào multi-zone, trạng thái auto-create ở các loại không phải Dedicated, phiên bản Kafka của broker.
- Chi tiết cấu hình Spring Boot cho IAM (thư viện `aws-msk-iam-auth`, tài liệu AWS ghi hai phiên bản khác nhau) và cho API key của Confluent: nằm ở báo cáo nghiên cứu, bạn học khi cần kết nối thật.
