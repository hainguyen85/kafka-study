# Tóm tắt Bài 1: Bức tranh tổng thể về Kafka

[Bài học](../lessons/0001-kafka-big-picture.html) · [Tóm tắt Bài 2 →](0002-key-partition-offset.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Kafka là nơi các service ghi lại sự kiện để service khác đọc, thay vì gọi nhau trực tiếp. Ví dụ khách đặt hàng: service Order ghi sự kiện "đã đặt hàng" vào topic `orders` rồi làm việc tiếp, không chờ ai.

Topic được chia thành nhiều partition, rải trên các broker của cụm. Mỗi partition có bản sao ở broker khác, nên một broker chết không mất dữ liệu.

Bên đọc là các consumer group. Notification và Inventory mỗi bên là một group, và mỗi group nhận đủ mọi sự kiện theo tốc độ của mình. Trong một group, các instance chia nhau partition, mỗi partition chỉ một instance đọc. Sự kiện không bị xoá khi đọc, nên sau này thêm Analytics vẫn đọc lại từ đầu được.

Lợi ích: Order không cần biết ai nhận, bên nhận chậm hay chết không kéo Order theo, và thêm bên nhận mới không phải sửa Order.
