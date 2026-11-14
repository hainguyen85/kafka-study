# Ngữ nghĩa giao hàng, thời điểm commit, lab bằng Spring Boot

2026-11-13. Người học hỏi (a) bài nào giải thích kỹ at-least-once / exactly-once và dùng khi nào; (b) với auto-commit thì consumer commit lúc nào, cụ thể trong Spring Boot; (c) yêu cầu các lab producer/consumer dùng code mẫu và cấu hình Spring Boot. Trước đó còn hỏi "khi nào dùng A, B, C" trong mô phỏng commit.

Kiểm tra: ngữ nghĩa giao hàng chỉ có ở Bài 7 ý 1 (một câu) và bảng thêm vội ở lượt trước; chưa có chỗ ghép nửa ghi và nửa đọc. Thời điểm commit chỉ nói "auto-commit chạy trong poll()" và một câu về AckMode BATCH. Các bước thực hành producer/consumer ở Bài 3, 5, 5b dùng console CLI, lệch với stack Spring Kafka của người học.

Đã làm: Bài 5c; mục thời điểm commit trong Bài 5 (trích docs Kafka và Spring); lab `labs/03-spring-clients` và chèn code/cấu hình vào các bài.

Implications: người học học theo hướng "cấu hình Spring cụ thể cho từng khái niệm", nên khái niệm mới phải đi kèm đoạn yml/Java tương ứng. Kiểm lại bằng quiz Bài 5 và 5c.
