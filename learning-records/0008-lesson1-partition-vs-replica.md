# Bài 1 xong: 8/8 quiz; đã gỡ nhầm lẫn partition với replica

2026-11-12. Người học làm quiz Bài 1 được 8/8 (gồm câu mới về partition/replica và câu "5 instance, 3 partition"). Trước đó, khi xem sơ đồ kiến trúc, người học hiểu nhầm P0, P1, P2 có cùng nội dung vì cả 3 broker đều giữ P0-P2; đã sửa sơ đồ (ghi nội dung đơn trong từng partition) và thêm hộp phân biệt. Người học cũng tự bắt lỗi hướng "hàng ngang / cột dọc" trong hộp đó: đọc sơ đồ kỹ và chủ động sửa sai.

Người học hỏi thêm ngoài bài: "Kafka có phải message queue không" (đã trả lời trong chat, chưa đưa vào bài) và xác nhận đúng ý "trong một group mỗi partition chỉ một consumer instance, không phải theo node".

Implications: có thể đi tiếp Bài 2 với tốc độ nhanh. Ở Bài 3-4 (leader, ISR, RF) kiểm lại phân biệt partition/replica bằng một câu hỏi truy hồi. Ứng viên GLOSSARY: partition, replica, consumer group (người học đã dùng đúng trong quiz).
