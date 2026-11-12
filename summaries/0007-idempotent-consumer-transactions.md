# Tóm tắt Bài 7: Idempotent consumer, outbox, transaction

[← Tóm tắt Bài 6](0006-error-handling-dlt.md) · [Bài học](../lessons/0007-idempotent-consumer-transactions.html) · [Tóm tắt Bài 8 →](0008-topic-design.md)

*Tóm tắt cả bài bằng lời, khoảng 60-90 giây. Dùng để ôn nhanh và tra cứu.*

Không mất nghĩa là at-least-once, tức là có thể trùng. Idempotent producer chỉ chống trùng do retry trong một session; ứng dụng gửi lại sau restart là record mới với offset mới. Nên chống trùng phải nằm ở **consumer**, bằng **mã nghiệp vụ** của sự kiện, không bằng offset.

Cách làm: trong cùng một transaction DB, insert `eventId` vào bảng có khoá duy nhất rồi mới thực hiện tác dụng phụ; trùng khoá thì bỏ qua; commit DB xong mới ack offset. Dedup trong RAM là sai vì crash là mất trí nhớ. Phía phát, ghi DB rồi gửi Kafka là ghi kép, crash giữa hai bước là mất; dùng **outbox**: ghi sự kiện vào bảng outbox cùng transaction với dữ liệu, một relay đẩy lên Kafka.

Transaction của Kafka cho exactly-once khi đọc topic, ghi topic và commit offset trong cùng một transaction, với consumer `read_committed`. Phạm vi chỉ trong Kafka: đầu ra là DB thì vẫn cần at-least-once cộng idempotent consumer. Producer chết giữa transaction làm consumer `read_committed` đứng chờ tới hết `transaction.timeout.ms`.

## Hỏi nhanh, đáp gọn

- **Bên kia nói "mình có exactly-once", hỏi lại gì?** "Đầu ra cuối cùng là Kafka hay DB?" Nếu là DB thì đó là at-least-once cộng dedup.
- **Bảng dedup giữ bao lâu?** Lâu hơn khoảng thời gian một bản trùng còn có thể xuất hiện, thường gắn với retention của topic.
