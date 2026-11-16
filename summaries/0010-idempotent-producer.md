# Tóm tắt Bài 4b: Idempotent producer

[← Tóm tắt Bài 4](0004-producer-durability.md) · [Bài học](../lessons/0010-idempotent-producer.html) · [Tóm tắt Bài 5 →](0005-consumer-offset-commit.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Producer phải gửi lại khi không nhận được xác nhận. Nếu lần đầu thật ra đã ghi thì log có hai bản. Idempotent producer chặn đúng loại trùng này: mỗi producer có một mã riêng, mỗi lô gửi mang một số thứ tự, và broker thấy số thứ tự cũ thì không ghi lại. Cùng cơ chế đó giữ đúng thứ tự khi gửi lại.

Nhưng nó không chặn trùng do ứng dụng gửi lại. Service restart là producer mới với mã mới, nên broker coi bản gửi lại là bản ghi mới. Loại trùng này phải xử lý ở consumer, bằng mã sự kiện.

Idempotence bật sẵn, nhưng tự tắt âm thầm nếu ai đó đổi `acks` hoặc tăng số request song song quá năm. Vì vậy trong Spring Boot nên ghi tường minh `enable.idempotence=true`: cấu hình sai sẽ báo lỗi ngay lúc khởi động thay vì lặng lẽ mất bảo đảm.
