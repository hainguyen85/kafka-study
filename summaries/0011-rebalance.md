# Tóm tắt Bài 5b: Rebalance

[← Tóm tắt Bài 5](0005-consumer-offset-commit.md) · [Bài học](../lessons/0011-rebalance.html) · [Tóm tắt Bài 5c →](0012-delivery-semantics.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Rebalance là lúc group coordinator chia lại partition giữa các consumer. Nó xảy ra khi một consumer vào, rời êm, chết (coordinator chỉ biết sau `session.timeout.ms`, mặc định 45 giây, vì hết heartbeat), khi consumer không gọi `poll()` trong `max.poll.interval.ms`, hoặc khi topic thêm partition. Trong lúc chia, partition đang chuyển chủ không ai đọc.

Mặc định vẫn là **eager** (`RangeAssignor`): mọi consumer trả hết partition rồi mới chia lại, cả group dừng. **Cooperative** (`CooperativeStickyAssignor`, đặt cho mọi consumer) chỉ thu hồi partition thật sự đổi chủ. **Static membership** (`group.instance.id`) cho phép restart nhanh mà không rebalance, đổi lại consumer chết thật thì partition đứng tới hết session timeout. Giao thức mới `group.protocol=consumer` bỏ rào chắn toàn group, nếu broker hỗ trợ.

Rebalance gây trùng: consumer mới đọc từ offset đã commit, nên phần đã xử lý mà chưa commit bị làm lại. Commit trong `onPartitionsRevoked` giảm trùng khi rời êm; chết đột ngột thì không kịp. Vì vậy giảm rebalance là tối ưu, còn consumer idempotent mới là bảo đảm.

## Hỏi nhanh, đáp gọn

- **Deploy làm lag tăng vọt, xử lý sao?** Cooperative + static membership với tên pod ổn định; hoặc giao thức consumer nếu broker hỗ trợ.
- **Hạ `session.timeout.ms` cho phát hiện nhanh?** Phát hiện nhanh hơn, nhưng GC pause dài có thể bị coi là chết và gây rebalance oan.
- **Thêm consumer có làm nhanh hơn không?** Chỉ tới số partition; thừa thì ngồi chờ, và mỗi lần thêm bớt là một lần rebalance.
