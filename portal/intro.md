# Apache Kafka cho kiến trúc microservices

Khoá tự học Kafka cho kỹ sư backend Java/Spring Boot đã có kinh nghiệm, viết bằng tiếng Việt, thuật ngữ Kafka giữ nguyên tiếng Anh. Học trên một cụm Kafka 3 broker tự dựng (Docker trong WSL2) với code Spring Kafka chạy được.

## Mục tiêu

Hiểu Kafka đủ chắc để **thảo luận và thiết kế** ngay khi vào dự án: đọc một cấu hình producer/consumer và nói được nó đánh đổi gì (mất message, trùng message, thứ tự, độ trễ), và với một bài toán lạ, trong 30 phút đề xuất được topic, key, số partition, cấu hình độ bền, cách xử lý trùng và lỗi.

Yêu cầu nghiệp vụ dẫn đường cho cả khoá là **không mất message**. Các bài đi theo chuỗi đó: producer ghi bền → consumer ghi nhận đúng lúc → xử lý lỗi không bỏ rơi message → chống trùng → thiết kế topic.

## Cấu trúc

| Phần | Bài | Bạn làm được gì sau phần này |
|---|---|---|
| 1 · Nền tảng | 1, 2, 3 | Kể đường đi của một message; chọn key; dựng cụm và đọc leader, ISR |
| 2 · Producer | 4, 4b | Cấu hình "không mất" phía ghi và biết nó đổi lấy gì; idempotent producer |
| 3 · Consumer và xử lý lỗi | 5, 5b, 5c, 6, 7 | Commit đúng lúc; rebalance; chọn at-most/at-least/exactly-once; poison pill và DLT; idempotent consumer, outbox, transaction |
| 4 · Thiết kế và kiểm tra cuối | 8, 8b, 8c, 8d, 9 | Thẻ thiết kế topic; quá tải, thông lượng, độ trễ; đọc lại, audit, giữ dữ liệu; hợp đồng sự kiện (schema); bài thi thiết kế 30 phút |
| 5 · Tổng kết | 10 | Ba ý cốt lõi (partition, ack, commit), các ngộ nhận phổ biến, sáu câu hỏi thiết kế |

Ngoài bài học còn có ba nhóm hỗ trợ ở thanh bên:

- **Mô phỏng**: năm mô phỏng tương tác (key → partition, độ bền, idempotent producer, commit offset, rebalance). Mô phỏng chạy theo mô hình mà tài liệu Kafka mô tả, không phải broker thật.
- **Tóm tắt ôn nhanh**: mỗi bài một đoạn tóm tắt bằng lời, khoảng một phút, để ôn trước buổi họp.
- **Tra cứu**: khái niệm cốt lõi, sổ tay tình huống, câu hỏi cho ngày đầu, so sánh dịch vụ Kafka managed trên AWS.

## Cách học một bài

1. Đọc mục **Kiến thức tối thiểu**. Mỗi ý có link tới tài liệu gốc (kafka.apache.org, Spring Kafka, KIP).
2. Chạy **mô phỏng** của bài (nút ở đầu bài) và đoán trước khi bấm.
3. Làm phần **thực hành** trên cụm: lệnh và kết quả mong đợi có trong bài. Chỗ nào ghi "chưa chạy thử" là dự đoán từ tài liệu; kết quả thật của bạn mới là bằng chứng.
4. **Tự nói thành lời** không nhìn tài liệu, rồi so với bản tóm tắt.
5. Làm **quiz** và dán dòng kết quả cho giáo viên (agent) để bài sau điều chỉnh.

Dùng nút "Đánh dấu hoàn thành" để theo dõi tiến độ; tiến độ lưu trong trình duyệt của bạn.

## Ngoài phạm vi

Bảo mật (SASL/SSL/ACL), vận hành cụm, Kafka Connect và Kafka Streams, Share Groups. Schema Registry chờ nghiên cứu nguồn chính trước khi viết bài.

## Mã nguồn đi kèm

- `labs/03-cluster`: cụm 3 broker KRaft (Docker Compose), dựng bằng `scripts/wizard-03-docker-kafka.sh`.
- `labs/03-spring-clients`: producer và consumer Spring Boot tối giản (gửi, nhận, xem rebalance, đo thông lượng, đọc lại theo thời gian) cho các bước thực hành của Bài 3, 5, 5b, 8b, 8c.
- `labs/08d-schema`: Spring Boot + Avro + Schema Registry (ba phiên bản schema) cho Bài 8d.
- `labs/04-producer-durability`: Spring Boot, ba cấu hình độ bền và bộ đối chiếu "mất bao nhiêu".
- `labs/05-scenarios`: các tình huống consumer, lỗi, transaction, thiết kế topic, mỗi tình huống một profile.
