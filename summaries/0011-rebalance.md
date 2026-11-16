# Tóm tắt Bài 5b: Rebalance

[← Tóm tắt Bài 5](0005-consumer-offset-commit.md) · [Bài học](../lessons/0011-rebalance.html) · [Tóm tắt Bài 5c →](0012-delivery-semantics.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Rebalance là lúc group chia lại partition giữa các consumer: khi có consumer vào, rời, chết, hoặc xử lý quá chậm. Trong lúc chia, partition đang đổi chủ không ai đọc.

Kiểu mặc định bắt cả group dừng mỗi lần chia. Deploy lần lượt mười pod là hai mươi lần cả group đứng hình. Có ba cách giảm: cooperative chỉ dừng partition thật sự đổi chủ; static membership cho pod restart nhanh giữ lại chỗ cũ mà không chia lại; và giao thức consumer mới, nếu broker hỗ trợ.

Consumer chết đột ngột thì cụm chỉ phát hiện sau khoảng 45 giây. Sau đó consumer mới đọc từ offset đã commit, nên phần đã xử lý mà chưa commit bị làm lại.

Kết luận: giảm rebalance là tối ưu hiệu năng, còn consumer chịu được trùng mới là bảo đảm.
