# Tóm tắt Bài 6: Poison pill, retry, DLT

[← Tóm tắt Bài 5c](0012-delivery-semantics.md) · [Bài học](../lessons/0006-error-handling-dlt.html) · [Tóm tắt Bài 7 →](0007-idempotent-consumer-transactions.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Message lỗi đi vào consumer Spring Kafka mặc định thì `DefaultErrorHandler` giao lại tối đa 10 lần, không chờ, rồi **chỉ log ERROR và commit offset**: message mất im lặng. Với yêu cầu "không mất", mặc định này không chấp nhận được.

Cấu hình tối thiểu: `DefaultErrorHandler` với back-off ngắn cho lỗi tạm thời, đánh dấu lỗi dữ liệu là không retry, và `DeadLetterPublishingRecoverer` đẩy record hỏng sang topic `<topic>-dlt`, cùng số partition, kèm header lý do lỗi. Topic DLT phải tự tạo, có ít nhất số partition của topic gốc. Lỗi giải mã thì bọc deserializer bằng `ErrorHandlingDeserializer`.

Đánh đổi chính: blocking retry giữ thứ tự nhưng làm cả partition đứng và có thể vượt `max.poll.interval.ms`; retry topic (`@RetryableTopic`) không chặn partition nhưng phá thứ tự theo key. Và DLT không phải thùng rác: cần cảnh báo khi có record, cần quy trình replay. DLQ cho consumer thường là pattern ứng dụng, không phải tính năng trong docs Apache.

## Hỏi nhanh, đáp gọn

- **Chọn retry kiểu nào cho dòng tiền theo tài khoản?** Thường là blocking retry ngắn để giữ thứ tự theo tài khoản, rồi DLT; lỗi kéo dài thì cảnh báo thay vì retry vô hạn.
- **DLT xong rồi thì sao?** Có người nhận cảnh báo, sửa nguyên nhân, rồi replay record về topic gốc; consumer phải idempotent vì replay là giao thêm lần nữa.
