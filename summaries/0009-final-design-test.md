# Tóm tắt Bài 9: Bài kiểm tra cuối

[← Tóm tắt Bài 8d](0016-schema-evolution.md) · [Bài học](../lessons/0009-final-design-test.html) · [Tóm tắt Bài 10 →](0013-core-80-20.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Đề: bán 5.000 ghế cho 50.000 người cùng lúc. Yêu cầu: không bán một ghế hai lần, không mất thanh toán đã thành công, email xác nhận đi ít nhất một lần.

Mình tách bốn topic: lệnh giữ ghế, kết quả giữ ghế, sự kiện thanh toán và thông báo. Lệnh giữ ghế dùng mã ghế làm key, nên mọi yêu cầu cho một ghế được xử lý lần lượt. Topic lệnh và thanh toán cấu hình không mất: ba bản sao, `min.insync.replicas=2`, `acks=all`, và API đọc kết quả gửi rồi mới trả lời người dùng.

Kafka không quyết ai thắng ghế; DB quyết, bằng cập nhật có điều kiện "chỉ khi ghế còn trống". Consumer ack sau khi DB commit và chống trùng bằng mã lệnh. Webhook thanh toán có thể gọi lại nhiều lần, nên chống trùng bằng mã giao dịch của bên thanh toán. Lỗi thì thử lại ngắn rồi chuyển sang dead letter topic có cảnh báo. Hạn giữ ghế 10 phút là bộ đếm giờ nghiệp vụ, để ở DB và một job quét.

Rủi ro còn lại: lag tăng ở đỉnh tải làm hết hạn giữ ghế, và dead letter topic không ai xem.
