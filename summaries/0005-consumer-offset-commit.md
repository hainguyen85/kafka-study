# Tóm tắt Bài 5: Consumer, offset, commit, rebalance

[← Tóm tắt Bài 4b](0010-idempotent-producer.md) · [Bài học](../lessons/0005-consumer-offset-commit.html) · [Tóm tắt Bài 5b →](0011-rebalance.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Consumer nhớ mình đọc tới đâu bằng cách commit offset. Restart thì đọc tiếp từ chỗ đã commit. Vì vậy câu hỏi "có mất message không" thực chất là: commit trước hay sau khi xử lý xong? Commit trước rồi crash là mất. Xử lý xong mới commit thì crash chỉ làm xử lý lại, không mất.

Kafka thuần tự commit định kỳ. Xử lý trong cùng thread thì vẫn an toàn; nhưng giao việc cho thread khác rồi đọc tiếp thì offset có thể đi trước việc thật, và crash là mất. Spring Boot tắt tự commit của Kafka, container commit sau khi listener xử lý xong. Với sự kiện tiền, ack thủ công sau khi ghi DB xong.

Hai bẫy nữa. Xử lý một lô quá lâu thì consumer bị coi là chết và bị đá khỏi group. Còn group nghỉ quá bảy ngày thì offset đã commit bị xoá, và cấu hình `latest` sẽ bỏ qua toàn bộ dữ liệu tồn.
