# Tóm tắt Bài 5c: At-most-once, at-least-once, exactly-once

[← Tóm tắt Bài 5b](0011-rebalance.md) · [Bài học](../lessons/0012-delivery-semantics.html) · [Tóm tắt Bài 6 →](0006-error-handling-dlt.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Có ba ngữ nghĩa: **at-most-once** (có thể mất, không bao giờ giao lại), **at-least-once** (không mất, có thể giao lại) và **exactly-once** (mỗi message được xử lý đúng một lần). Ngữ nghĩa là của cả chuỗi, gồm nửa ghi và nửa đọc, và nửa yếu nhất quyết định. Nửa ghi: không retry là có thể mất, có retry là có thể trùng, thêm idempotence thì hết trùng do retry. Nửa đọc: lưu vị trí trước khi xử lý là at-most-once, xử lý xong mới lưu là at-least-once. Kafka mặc định at-least-once.

Exactly-once của Kafka chỉ phủ luồng Kafka → Kafka, bằng transaction và `read_committed`. Ra DB hay API thì phải phối hợp với hệ thống đó; cách thực tế là at-least-once cộng consumer idempotent (khoá dedup trong cùng transaction DB) và outbox ở phía phát. Vì vậy tiền và lệnh đi theo "at-least-once + idempotent", bước xử lý luồng giữa các topic dùng transaction, còn giá hiển thị thời gian thực có thể at-most-once.

## Hỏi nhanh, đáp gọn

- **Bên kia nói "mình exactly-once"?** Hỏi "đầu ra cuối là Kafka hay DB?". DB thì thực chất là at-least-once cộng dedup.
- **Cấu hình at-most-once trong Spring?** Producer `retries: 0`, `acks: "1"`, tắt idempotence; listener `ack.acknowledge()` trước rồi mới xử lý.
- **Vì sao transaction Kafka không cứu được SMS gửi hai lần?** Cuộc gọi ra ngoài không nằm trong transaction; abort và giao lại thì SMS đi lần nữa.
