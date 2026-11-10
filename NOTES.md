# Ghi chú cho giáo viên (agent)

## Sở thích của người học
- Học bằng tiếng Việt; thuật ngữ Kafka giữ nguyên tiếng Anh (topic, partition, offset...).
- Thích usecase "oái oăm" (tình huống dễ sai) + code sinh sẵn để xem cấu hình chạy ra sao.
- Tech lead/mix; nơi làm mới là banking nhưng bài học KHÔNG cần gắn với banking. Dùng usecase đa dạng (gọi xe, e-commerce/flash sale, IoT, game, logistics, thanh toán...) để thấy nhiều dạng vấn đề; mục tiêu là hiểu cách cấu hình xử lý từng dạng vấn đề. Đôi khi thêm một dòng "nếu là banking thì..." khi thật sự hữu ích.
- Đã bảo tôi hỏi lại khi chưa rõ.
- Muốn hiểu nền tảng (bức tranh tổng thể) trước khi đi sâu từng thành phần: đừng nhảy vào chi tiết khi chưa dựng bản đồ.

## Môi trường
- Windows 11 Home, WSL2 Ubuntu 24.04, chưa có Docker (2026-11-10). Java 25 + 8, Maven 3.8.6, Node, Python, Go.
- Kafka hiện tại theo kafka.apache.org/quickstart: 4.4.0 (cần Java 17+). Image: `apache/kafka`.
- Chọn: dựng cụm bằng Docker Engine trong WSL2, code Java với Spring Kafka (nơi mới dùng Spring Kafka; người học là người tạo topic; Apache Kafka tự vận hành; yêu cầu chính là không mất message).
- Nguồn Spring Kafka: research/0002-spring-kafka.md (đang chạy, chưa dùng để sinh code cho tới khi đọc xong).

## Lộ trình (người học chấp nhận 2026-11-10, dựa trên research/0001)
Nguyên tắc: nền tảng/bức tranh tổng thể trước, rồi mới đi sâu từng thành phần. Mỗi bài từ bài 3 có code trong `labs/` (Spring Kafka).
1. Bức tranh tổng thể (xong)
2. Key, partition, offset (xong)
3. Dựng cụm 3 broker KRaft bằng Docker trong WSL2 (dùng `wizard`); CLI; tắt auto-create topic; `--describe`; hộp "đọc tutorial cũ thì coi chừng" (ZooKeeper, v.v.)
4. Producer (Spring Kafka) + độ bền, hướng "không mất": acks, idempotence, delivery.timeout.ms, RF, min.insync.replicas, NotEnoughReplicas; kích thước message. Lab: kill broker khi đang gửi
5. Consumer + offset: auto vs manual commit, at-least-once, auto.offset.reset, offset hết hạn, reset offset bằng CLI
6a. Lỗi xử lý: poison pill, retry, DLQ (ghi rõ đây là pattern ứng dụng, docs Kafka chỉ có DLQ cho Connect và Share Groups)
6b. Rebalance: max.poll.interval.ms, static membership, cooperative, giao thức `consumer` (KIP-848)
7. Idempotent consumer, outbox, transactions / read_committed, phạm vi exactly-once
8. Schema, đặt tên topic, retention vs compaction
6c. (tuỳ chọn) Share Groups khác Consumer Groups thế nào
9. Giám sát lag, thiết kế event; tổng quan Connect/Streams
(Bài "bảo mật phía client" bị hoãn theo yêu cầu người học.)
Kho 16 usecase oái oăm có nguồn: research/0001-curriculum-coverage.md mục 2.

## Quy ước
- GLOSSARY.md chưa tạo: chỉ thêm thuật ngữ khi người học dùng đúng (quiz/bài tập). Bảng tóm tắt bài 1-2 nằm ở reference/0001-core-concepts.html.
- Sau khi sửa lessons/ hoặc reference/: chạy `python scripts/html2md.py`.

## Quy trình theo AGENTS.md (ghi sau lần review 2026-11-10)
- Đổi mission: hỏi theo vòng kiểu `grilling` (đánh số, kèm đề xuất), chỉ sửa MISSION.md sau khi người học xác nhận.
- Bài 3 (cài Docker trong WSL2, dựng cụm): dùng `wizard` (liệt kê stage cho người học xác nhận, rồi sinh script bash chạy trong WSL).
- Kiến thức mới ngoài RESOURCES.md: chạy `research` (background agent), mỗi claim kèm nguồn. Mọi câu trong lesson về hành vi Kafka phải có nguồn hoặc ghi rõ là minh hoạ.
- Thuật ngữ chưa chốt, cần quyết khi người học bắt đầu dùng: "event" vs "message" vs "record" (docs dùng cả ba); "position" vs "committed offset". Các bài hiện dùng "event/message" lẫn nhau.
- Độ dài lesson (người học nhắc 2026-11-10): một chiến thắng mỗi bài, nhưng ngắn quá cũng không nên. Mỗi bài đủ kiến thức có nguồn + thực hành có phản hồi (code/cụm/quiz) + usecase oái oăm. Skill `teach` chủ đạo, các skill khác chỉ phụ trợ (xem AGENTS.md).
- Bài 1 và 2 hiện chỉ có khái niệm + mô phỏng + quiz, chưa có code chạy thật: từ Bài 3 trở đi mỗi bài kèm code trong `labs/`.
- Wizard bài 3: `scripts/wizard-03-docker-kafka.sh` (6 stage người học đã chấp nhận). Mới kiểm tra `bash -n`, chưa chạy thật. Lab 04 (`labs/04-producer-durability`) đã biên dịch được, chưa chạy với cụm thật: lesson bài 4 chỉ viết SAU khi người học báo kết quả thật, không khẳng định kết quả dự kiến như đã quan sát.
- Cụm lab là combined mode: kill 2 node = mất đa số controller (khác báo cáo research/0001 T2 giả định controller tách riêng). Thí nghiệm "NotEnoughReplicas" dùng topic min.insync.replicas=3 và kill 1 node.
- JAVA_HOME máy người học là JDK 8: Maven cho lab Spring Boot 4 phải chạy bằng JDK 17+ (đặt JAVA_HOME tạm thời).
