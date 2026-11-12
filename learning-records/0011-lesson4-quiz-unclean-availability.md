# Bài 4: quiz 3/5; chưa rõ unclean leader election và "đổi sẵn sàng lấy bền"

2026-11-12. Người học làm quiz Bài 4 được 3/5 (sai câu 1: acks=all + minISR=1 khi ISR chỉ còn leader; câu 5: đặt tường minh enable.idempotence=true cùng acks=1). Người học nói rõ hai câu sai do bài chưa giải thích, và hỏi thêm: chưa hiểu `unclean.leader.election.enable`, chưa hiểu usecase (b) "đổi sẵn sàng lấy bền". Trước đó người học cũng hỏi B1-B3 là gì và "follower tự sync từ leader": đã sửa mô phỏng (đổi tên nút thành "Follower fetch từ leader", thêm phần đọc hình).

Đúng là bài thiếu: "acks=all chỉ chờ ISR hiện tại" chỉ được nói lướt; unclean election chỉ xuất hiện trong mô phỏng; điều kiện của idempotence và khác biệt tường minh/mặc định bị nhét trong ý timeout. Đã sửa mục 1: tách thành các ý riêng có ví dụ (ISR co lại → acks=all = 1 bản sao; minISR là sàn; định nghĩa sẵn sàng vs bền; unclean election hai lựa chọn kèm ví dụ; ba điều kiện của idempotence và tường minh vs mặc định), viết lại usecase (b) theo dòng thời gian.

Implications: người học nắm cơ chế partition/consumer tốt, nhưng các khái niệm "đánh đổi" (availability vs durability) cần ví dụ dòng thời gian cụ thể chứ không chỉ một câu. Khi viết bài 5-8: mỗi đánh đổi nêu bằng diễn biến từng bước. Làm lại câu 1 và 5 sau khi đọc mục 1 mới.

Cập nhật 2026-11-12: làm lại quiz Bài 4 được 4/5 (câu 1 và 5 đã đúng sau khi sửa bài). Sai câu 3 (NotEnoughReplicasAfterAppendException → trùng): ý này trong bài cũ chỉ có một câu ngắn. Đã viết lại thành so sánh trước/sau khi append và vai trò của idempotence.
