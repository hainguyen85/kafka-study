# Tóm tắt Bài 8b: Quá tải, thông lượng, độ trễ

[← Tóm tắt Bài 8](0008-topic-design.md) · [Bài học](../lessons/0014-throughput-latency.html) · [Tóm tắt Bài 8c →](0015-replay-retention.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Khi quá tải, Kafka không sập mà để dữ liệu chờ. Chờ ở bộ đệm của producer thì lệnh gửi bị chặn lại; chờ trên đĩa broker thì consumer tụt lại phía sau, gọi là lag.

Muốn tăng thông lượng thì gom nhiều message thành một lô và nén cả lô. Kafka 4 đã mặc định chờ 5 mili giây để gom lô, và thường không làm chậm hơn. Trong lab, chỉ bật nén đã tăng thông lượng gần gấp ba, và độ trễ giảm từ vài giây xuống vài chục mili giây, vì message không còn xếp hàng trong bộ đệm.

Có hai điều hay bị hiểu sai. Một: giảm mức xác nhận ghi không làm phía đọc thấy dữ liệu sớm hơn, vì consumer chỉ thấy message khi đủ bản sao. Hai: thêm consumer chỉ giúp tới số partition; một mã chứng khoán quá nóng thì một partition vẫn gánh gần hết tải.

Tín hiệu quá tải số một là lag. Theo dõi lag theo thời gian cần để đọc hết, và khi hệ thống phía sau chậm thì tạm dừng nhận, đừng để consumer lỗi liên tục.
