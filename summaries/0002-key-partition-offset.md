# Tóm tắt Bài 2: Key, partition, offset

[← Tóm tắt Bài 1](0001-kafka-big-picture.md) · [Bài học](../lessons/0002-key-partition-offset.html) · [Tóm tắt Bài 3 →](0003-build-cluster.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Mỗi message có một key. Producer băm key để chọn partition, nên cùng key luôn vào cùng partition. Kafka chỉ giữ thứ tự trong một partition, không phải cả topic. Vì vậy muốn sự kiện của một tài xế, một đơn hàng hay một tài khoản đến đúng thứ tự, hãy lấy chính mã đó làm key.

Offset là số thứ tự của message trong partition. Consumer chỉ cần nhớ một con số: mình đã đọc tới đâu.

Hai điều phải tính từ đầu. Một: số partition chỉ tăng, không giảm, và tăng thì key có thể chuyển sang partition khác, làm vỡ thứ tự. Hai: key quá "to", như mã shop trong ngày flash sale, dồn tải vào một partition. Thêm consumer cũng không giúp, vì mỗi partition chỉ một consumer trong group đọc.
