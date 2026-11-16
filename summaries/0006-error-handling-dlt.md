# Tóm tắt Bài 6: Poison pill, retry, DLT

[← Tóm tắt Bài 5c](0012-delivery-semantics.md) · [Bài học](../lessons/0006-error-handling-dlt.html) · [Tóm tắt Bài 7 →](0007-idempotent-consumer-transactions.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Một message lỗi đi vào consumer Spring Kafka mặc định sẽ được thử lại vài lần, rồi chỉ ghi log và đi tiếp. Nghĩa là mất im lặng, không chấp nhận được với sự kiện tiền.

Cấu hình đúng chia lỗi làm hai loại. Lỗi tạm thời, như dịch vụ phụ chết vài giây, thì thử lại vài lần có giãn cách. Lỗi vĩnh viễn, như dữ liệu sai định dạng, thì không thử lại mà chuyển ngay sang dead letter topic, kèm lý do lỗi.

Có một đánh đổi: thử lại tại chỗ giữ được thứ tự nhưng làm cả partition đứng chờ; đẩy sang topic retry thì không chặn, nhưng phá thứ tự theo key.

Và dead letter topic không phải thùng rác. Phải có cảnh báo khi có message rơi vào, có người xử lý, và có quy trình đưa message về topic gốc sau khi sửa lỗi.
