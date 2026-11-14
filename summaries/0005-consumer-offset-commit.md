# Tóm tắt Bài 5: Consumer, offset, commit, rebalance

[← Tóm tắt Bài 4b](0010-idempotent-producer.md) · [Bài học](../lessons/0005-consumer-offset-commit.html) · [Tóm tắt Bài 5b →](0011-rebalance.md)

*Tóm tắt cả bài bằng lời, khoảng 60-90 giây. Dùng để ôn nhanh và tra cứu.*

Consumer giữ vị trí bằng **commit offset**: số offset của record kế tiếp cần đọc. Restart thì đọc tiếp từ offset đã commit. Câu hỏi "có mất không" chỉ là câu hỏi **commit xảy ra trước hay sau khi xử lý xong**. Commit trước, crash, là mất; commit sau, crash, là xử lý lại, tức at-least-once.

Kafka thuần mặc định auto-commit mỗi 5 giây, chạy trong `poll()`: nếu listener giao việc cho thread khác rồi return, offset có thể đi trước việc thật và crash là mất. Spring Kafka đổi mặc định: tắt auto-commit, container commit sau khi xử lý xong lô. Muốn kiểm soát từng record thì dùng `ack-mode=manual_immediate` và ack sau khi ghi DB xong.

Consumer còn bị coi là chết nếu không gọi `poll` trong `max.poll.interval.ms`: xử lý quá chậm thì bị đá khỏi group, rebalance, record bị xử lý lặp; sửa bằng giảm `max.poll.records` hoặc tăng khoảng thời gian. Deploy làm rebalance thì dùng static membership, cooperative assignor hoặc giao thức consumer mới. Và offset đã commit hết hạn sau 7 ngày khi group rỗng; lúc đó `auto.offset.reset=latest` sẽ nhảy tới cuối.

## Hỏi nhanh, đáp gọn

- **Tại sao không commit sau mỗi record cho chắc?** Được, đổi lại nhiều lần commit hơn nên chậm hơn; và vẫn có thể trùng một record nếu crash giữa xử lý và commit.
- **Muốn xử lý lại message cũ thì làm sao?** Dừng group rồi dùng `kafka-consumer-groups.sh --reset-offsets` (xem trước, rồi `--execute`).
- **Hết `auto.commit.interval.ms` giữa lúc đang xử lý thì có commit không?** Không. Bộ đếm chỉ được xét trong `poll()`; commit đi ở lần `poll()` sau, với vị trí đã trả cho ứng dụng. Xử lý cùng thread thì đó là at-least-once.
- **Tắt auto-commit thì `close()` có commit không?** Kafka client: không. Spring: container dừng bình thường vẫn commit offset của record đã xử lý (`commitPendingAcks`) trước khi đóng; crash thì không.
