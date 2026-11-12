# Tóm tắt Bài 9: Bài kiểm tra cuối

[← Tóm tắt Bài 8](0008-topic-design.md) · [Bài học](../lessons/0009-final-design-test.html)

*Tóm tắt cả bài bằng lời, khoảng 2 phút (đề bán vé). Dùng để ôn nhanh và tra cứu.*

*Mở bằng yêu cầu.* Hệ thống cần ba thứ: không bán một ghế hai lần, không mất thanh toán đã thành công, và email xác nhận đi ít nhất một lần. Đỉnh khoảng 20.000 yêu cầu mỗi giây trong 2 phút.

*Đi qua quyết định kèm đánh đổi.* Mình tách bốn topic: `seat-commands`, `seat-events`, `payment-events`, `notifications`. Key của lệnh giữ ghế là `seatId`, để mọi yêu cầu cho một ghế được xử lý tuần tự; nếu key theo sự kiện thì dồn hết vào một partition. Số partition tính từ đỉnh tải và số consumer cần chạy song song, có dư địa vì không giảm được. Topic lệnh và thanh toán: RF=3, `min.insync.replicas=2`, `acks=all`, idempotence, và API đọc kết quả gửi rồi mới trả lời người dùng.

Kafka không quyết ai thắng ghế; DB quyết, bằng cập nhật có điều kiện. Consumer tắt auto-commit, ack sau khi DB commit, chống trùng bằng `UNIQUE(commandId)`; webhook thanh toán gọi lại thì dedup bằng mã giao dịch của bên thanh toán; phía phát dùng outbox. Lỗi: retry ngắn rồi DLT cùng số partition, có cảnh báo và replay; giữ blocking retry cho lệnh giữ ghế để không phá thứ tự theo ghế. Hạn giữ ghế 10 phút là bộ đếm giờ nghiệp vụ, để ở DB và một job quét, không dựa vào Kafka.

*Kết bằng rủi ro và câu hỏi.* Rủi ro còn lại: lag tăng ở đỉnh tải làm hết hạn giữ ghế trước khi xử lý, key nóng, và DLT không ai xem. Câu hỏi cho platform team: `unclean.leader.election.enable`, quyền tạo topic, trần partition và kích thước message, lịch bảo trì.

## Hỏi nhanh, đáp gọn

- **Khung nói dùng lại cho đề bất kỳ** Yêu cầu (mất được không, trùng được không, thứ tự theo gì, tải bao nhiêu) → topic và key → partition → độ bền → consumer và chống trùng → lỗi → dữ liệu (retention, kích thước, schema) → rủi ro → câu hỏi cho platform team.
- **Người nghe ngắt lời hỏi "sao không exactly-once?"** Vì đầu ra cuối là DB; transaction Kafka chỉ phủ Kafka sang Kafka. Ở đây at-least-once cộng dedup trong DB cho cùng kết quả.
