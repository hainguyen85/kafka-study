# Tóm tắt Bài 2: Key, partition, offset

[← Tóm tắt Bài 1](0001-kafka-big-picture.md) · [Bài học](../lessons/0002-key-partition-offset.html) · [Tóm tắt Bài 3 →](0003-build-cluster.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Topic được chia thành partition. Producer chọn partition theo **key**: có key thì băm key, nên **cùng key luôn vào cùng partition**; không có key thì producer dùng sticky partition, dồn vào một partition một lúc rồi đổi, và mình không kiểm soát được message nào vào đâu.

Kafka chỉ đảm bảo **thứ tự trong một partition**, không phải cả topic. Vì vậy muốn giữ thứ tự sự kiện của một đối tượng (tài xế, đơn hàng, tài khoản) thì dùng chính mã đối tượng đó làm key. **Offset** là số thứ tự trong một partition; vị trí của consumer là offset của message *kế tiếp* sẽ đọc, chỉ là một con số.

Hai bẫy khi chọn key và số partition. Một: số partition chỉ tăng, không giảm, và tăng thì key có thể sang partition khác trong khi dữ liệu cũ không được chia lại, nên thứ tự bị vỡ qua điểm tăng; phải tính số partition từ đầu. Hai: key quá "to" (như mã shop trong flash sale) dồn tải vào một partition, và thêm consumer không giúp vì mỗi partition chỉ một consumer trong group đọc.

Một message gồm key, value, timestamp và header. Broker chỉ lưu byte: serializer ở producer và deserializer ở consumer phải khớp định dạng, lệch là consumer lỗi giải mã. Header là chỗ đặt `eventId` hay trace id mà không phải sửa value.


## Hỏi nhanh, đáp gọn

- **Dùng một partition cho chắc thứ tự được không?** Được thứ tự toàn topic nhưng mất song song: một group chỉ có một consumer làm việc. Thường chỉ cần thứ tự theo đối tượng, nên chọn key đúng là đủ.
- **Không gửi key thì sao?** Message được rải theo sticky partition; không có thứ tự giữa các message, hợp với sự kiện độc lập nhau.
