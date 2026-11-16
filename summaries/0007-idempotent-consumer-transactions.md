# Tóm tắt Bài 7: Idempotent consumer, outbox, transaction

[← Tóm tắt Bài 6](0006-error-handling-dlt.md) · [Bài học](../lessons/0007-idempotent-consumer-transactions.html) · [Tóm tắt Bài 8 →](0008-topic-design.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Không mất nghĩa là có thể trùng, nên consumer phải chịu được trùng. Cách làm chuẩn: mỗi sự kiện có một mã riêng. Trong cùng một transaction DB, ghi mã đó vào bảng có khoá duy nhất rồi mới cập nhật nghiệp vụ; gặp lại mã cũ thì bỏ qua. DB commit xong mới commit offset. Nhớ trong RAM là sai, vì crash là mất trí nhớ.

Phía phát cũng có bẫy: ghi DB rồi gửi Kafka là hai việc riêng, crash ở giữa là mất sự kiện. Outbox sửa việc này: ghi sự kiện vào một bảng trong cùng transaction với dữ liệu, rồi một tiến trình riêng đẩy lên Kafka.

Transaction của Kafka cho exactly-once khi cả đầu vào lẫn đầu ra là topic. Đầu ra là DB thì vẫn cần at-least-once cộng chống trùng. Nên khi ai đó nói "bên mình exactly-once", hãy hỏi: đầu ra cuối là Kafka hay DB?
