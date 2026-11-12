# Tóm tắt Bài 1: Bức tranh tổng thể về Kafka

[Bài học](../lessons/0001-kafka-big-picture.html) · [Tóm tắt Bài 2 →](0002-key-partition-offset.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Service Order là **producer**: khách đặt hàng xong, nó ghi event `OrderPlaced` vào topic `orders` rồi làm việc tiếp, không chờ ai. Topic nằm trên một **cluster** gồm nhiều **broker**; nó được chia thành các **partition**, mỗi đơn vào một partition. Event được ghi vào bản **leader** của partition đó, các broker khác giữ bản sao (follower) để một broker chết không mất dữ liệu. **Controller** (KRaft) giữ metadata của cụm: partition nào, leader ở đâu.

Notification và Inventory là hai **consumer group**. Mỗi group tự kéo event theo tốc độ của mình và nhận đủ mọi event; trong một group, các consumer chia nhau partition, mỗi partition đúng một consumer. Event không bị xoá khi đọc mà giữ theo retention, nên sau này thêm Analytics vẫn đọc lại được.

Khác gọi API trực tiếp ở chỗ Order không cần biết ai nhận, không bị service nhận chậm hay chết kéo theo, và thêm service nhận mới không phải sửa Order.

## Hỏi nhanh, đáp gọn

- **Kafka có phải message queue không?** Dùng được như queue (các consumer cùng group chia nhau partition) và như pub/sub (mỗi group nhận đủ), nhưng bản chất là một log chỉ ghi thêm: đọc không xoá, đọc lại được, song song bị giới hạn bởi số partition.
- **Một broker chết thì sao?** Partition có leader trên broker đó được bầu leader mới từ các bản sao còn bám kịp (ISR); cụm chạy tiếp. Chi tiết ở Bài 3 và 4.
