# Tóm tắt Bài 8: Thiết kế topic

[← Tóm tắt Bài 7](0007-idempotent-consumer-transactions.md) · [Bài học](../lessons/0008-topic-design.html) · [Tóm tắt Bài 9 →](0009-final-design-test.md)

*Tóm tắt cả bài bằng lời, khoảng 60-90 giây. Dùng để ôn nhanh và tra cứu.*

Tạo một topic là bảy quyết định. **Key** theo đối tượng cần giữ thứ tự, tránh key nóng. **Số partition** theo mức song song cần có cộng dư địa, vì chỉ tăng được và tăng thì key đổi partition. **RF=3 và `min.insync.replicas=2`** cho dữ liệu không được mất.

**Retention hay compaction**: cần lịch sử thì `delete` với `retention.ms` đủ dài (mặc định 7 ngày); chỉ cần trạng thái mới nhất mỗi key thì `compact`, xoá bằng tombstone, và nhớ compaction không có hạn chót cứng. **Kích thước message** bị giới hạn ở bốn tầng độc lập, mặc định khoảng 1 MB; file lớn thì giữ ngoài Kafka và gửi tham chiếu. **Tên và topic phụ**: tắt auto-create, DLT đủ partition. **Schema**: coi định dạng là hợp đồng, có `eventId` và phiên bản ngay từ đầu.

Khó đổi nhất là key, số partition và schema. Trên dịch vụ managed, giá trị có thể bị khoá hoặc khác mặc định Apache, nên trước khi chốt phải hỏi platform team: RF và minISR có sửa được không, `unclean.leader.election.enable` là gì, quyền tạo topic và trần partition, trần kích thước message.

## Hỏi nhanh, đáp gọn

- **Chọn bao nhiêu partition?** Bắt đầu từ số consumer song song cần có ở đỉnh tải, cộng dư địa; đo thông lượng thật và kiểm quota của dịch vụ managed. Không có con số chung.
- **Compaction có đảm bảo mỗi key một bản ghi không?** Không tức thì: segment đang ghi không bị compact và thời điểm compact không cứng. Consumer phải chịu được việc thấy nhiều bản của một key.
