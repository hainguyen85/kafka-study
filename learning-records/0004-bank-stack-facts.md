# Stack nơi mới: Apache Kafka tự vận hành + Spring Kafka, yêu cầu "không mất"

Người học cho biết (2026-11-10): cụm Kafka là Apache Kafka tự vận hành; client là Spring Kafka; chính họ tạo và cấu hình topic; yêu cầu nghiệp vụ chính là không mất message; bảo mật (có thể SSL/mTLS theo topic, chưa chắc) bỏ qua ở giai đoạn study. Đã chấp nhận lộ trình mới ở `NOTES.md`.

Implications: code demo dùng Spring Kafka; ưu tiên cấu hình độ bền (acks, min.insync.replicas, RF, manual ack) và các cấu hình mà người tạo topic phải quyết; bỏ bài bảo mật; chỉ dạy tính năng có trong Apache Kafka. Phiên bản broker vẫn chưa biết, nên đánh dấu rõ tính năng phụ thuộc phiên bản (KIP-848, ELR, Share Groups).
