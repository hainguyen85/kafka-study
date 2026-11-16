# Tóm tắt Bài 5c: At-most-once, at-least-once, exactly-once

[← Tóm tắt Bài 5b](0011-rebalance.md) · [Bài học](../lessons/0012-delivery-semantics.html) · [Tóm tắt Bài 6 →](0006-error-handling-dlt.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Có ba mức bảo đảm. At-most-once: có thể mất, không bao giờ trùng. At-least-once: không mất, có thể trùng. Exactly-once: mỗi message được xử lý đúng một lần.

Mức của cả luồng do mắt xích yếu nhất quyết định. Producer cấu hình chuẩn mà consumer lưu vị trí trước khi xử lý thì cả luồng vẫn có thể mất.

Kafka mặc định at-least-once. Exactly-once của Kafka chỉ đúng khi đọc từ topic và ghi ra topic, bằng transaction. Khi đầu ra là database hay một API bên ngoài, cách thực tế là at-least-once cộng consumer chống trùng.

Áp vào ngân hàng: tiền và lệnh dùng at-least-once, chống trùng bằng mã sự kiện trong cùng transaction DB. Bước xử lý luồng giữa các topic có thể dùng transaction Kafka. Còn dữ liệu như giá hiển thị thời gian thực thì at-most-once cũng được.
