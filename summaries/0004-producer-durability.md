# Tóm tắt Bài 4: Producer và độ bền

[← Tóm tắt Bài 3](0003-build-cluster.md) · [Bài học](../lessons/0004-producer-durability.html) · [Tóm tắt Bài 4b →](0010-idempotent-producer.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Producer nhận "thành công" chưa chắc dữ liệu đã an toàn. Muốn không mất, cần bốn thứ.

Một: `acks=all`, chỉ báo thành công khi mọi bản sao đang bám kịp đã nhận. Hai: topic ba bản sao với `min.insync.replicas=2`. Còn ít hơn hai bản sao thì broker từ chối ghi, thay vì ghi một bản rồi hứa suông. Ba: producer tự gửi lại khi lỗi tạm thời. Bốn: idempotence, để gửi lại không tạo bản trùng.

Cái giá là tính sẵn sàng: mất hai trên ba broker thì topic ngừng nhận ghi. Với sổ cái, thà báo lỗi còn hơn báo thành công rồi mất. Với telemetry mất vài điểm không sao, có thể nới.

Phía ứng dụng có hai bẫy. Gọi `send()` mà không đọc kết quả thì lỗi chỉ nằm trong log, người dùng vẫn thấy thành công. Và khi cụm sự cố, `send()` có thể treo tới một phút, kéo cả API treo theo.
