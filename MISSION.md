# Mission: Apache Kafka cho kiến trúc microservices (áp dụng vào ngân hàng)

## Why
Sắp onboarding ở một ngân hàng dùng Kafka trong kiến trúc microservices. Bạn là tech lead / mix (vừa code vừa thiết kế), cần hiểu Kafka đủ sâu để trao đổi với các team và tham gia thiết kế sản phẩm ngay từ đầu, không phải ngồi nghe mà không hiểu. Mục tiêu cốt lõi: nắm concept đủ chắc để "chém" (thảo luận tự tin) về Kafka ngay sau khi onboarding; code demo là công cụ để hiểu concept, không phải đích. Cốt lõi là hiểu cách cấu hình để giải quyết một lớp vấn đề (thứ tự, mất/trùng message, quá tải, lỗi), để áp vào bất kỳ domain nào, kể cả banking.

## Success looks like
- Nắm vững nền tảng trước: Kafka giải quyết vấn đề gì, gồm những thành phần nào và chúng phối hợp ra sao (bức tranh tổng thể), rồi mới đi sâu vào từng thành phần.
- Giải thích được cho đồng nghiệp đường đi của một message từ producer tới consumer: topic, partition, key, offset, replication, consumer group.
- Đọc một cấu hình producer/consumer và nói được nó đánh đổi gì (mất message, trùng message, thứ tự, độ trễ).
- Tự dựng cụm Kafka 3 broker (KRaft) trên máy, viết code Spring Kafka producer/consumer, và cố tình làm hỏng (kill broker, consumer chết, message lỗi) rồi quan sát hệ thống xử lý.
- Bài kiểm tra cuối: cho một bài toán mới trong lĩnh vực bất kỳ (gọi xe, đơn hàng, IoT, thanh toán, sổ cái...), trong 30 phút bạn tự đề xuất topic, key, số partition, cấu hình độ bền, cấu hình producer/consumer, cách xử lý trùng/lỗi, và giải thích được đánh đổi bằng lời như đang họp.

## Constraints
- Thứ tự học: bức tranh tổng thể trước, từng thành phần sau (đặt ra 2026-11-10 sau khi bài đầu bị nhận xét là nhảy vào chi tiết quá sớm).
- Trình độ xuất phát (2026-11-10): biết khái niệm cơ bản, chưa tự chạy hay cấu hình.
- Máy Windows 11 + WSL2 Ubuntu 24.04; chưa có Docker (sẽ cài Docker trong WSL2). Có Java 25 và 8, Maven, Node, Python, Go.
- Nơi mới (người học xác nhận 2026-11-10): Apache Kafka tự vận hành (không phải Confluent), client là Spring Kafka, người học tự tạo và cấu hình topic, yêu cầu nghiệp vụ chính là KHÔNG MẤT message. Phiên bản broker chưa biết.
- Code demo bằng Java với Spring Kafka (stack của nơi mới); chỉ dùng client thuần khi cần thấy cấu hình gốc của Kafka.
- Thời gian (giả định tạm 2026-11-10, người học chưa đưa số thật): khoảng 4 tuần, 5 giờ/tuần trước khi onboarding. Cập nhật khi biết ngày thật.
- Học bằng usecase "oái oăm" kèm code chạy thật, không học lý thuyết khô. Usecase đa dạng nhiều lĩnh vực, không bắt buộc gắn với ngân hàng (có thể ánh xạ sang banking khi cần).

## Out of scope
- Schema Registry: học một bài (ảnh hưởng thiết kế event). Kafka Connect và Kafka Streams: chỉ tổng quan.
- Bảo mật (SASL/SSL/mTLS/ACL) ở giai đoạn study này, theo yêu cầu của người học; nơi mới có thể dùng SSL/mTLS theo topic (chưa chắc). Quay lại khi cần.
- Vận hành cụm production (tuning OS, capacity planning chi tiết) trừ khi mission đổi.
- Đọc source code Kafka.
