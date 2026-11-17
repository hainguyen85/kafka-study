# Tóm tắt Bài 8c: Đọc lại, audit, giữ dữ liệu

[← Tóm tắt Bài 8b](0014-throughput-latency.md) · [Bài học](../lessons/0015-replay-retention.html) · [Tóm tắt Bài 8d →](0016-schema-evolution.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Kafka không xoá message khi đã đọc mà giữ theo thời hạn của từng topic, mặc định bảy ngày. Thời hạn đó là cam kết với phía đọc: đọc chậm hơn là mất phần chưa đọc. Kafka xoá theo từng khối lớn nên dữ liệu có thể sống lâu hơn hạn một chút.

Nhờ giữ dữ liệu nên đọc lại được: sửa bug tính phí rồi chạy lại từ một mốc thời gian, hay chạy lại cả một phiên giao dịch. Có ba cách: lệnh quản trị khi group đang dừng, API của client, hoặc hàm có sẵn trong Spring. Bẫy lớn nhất: lệnh quản trị hiểu mốc thời gian là giờ UTC nếu không ghi múi giờ, nên ở Việt Nam phải ghi thêm "+07:00", nếu không sẽ bắt đầu lệch bảy tiếng.

Topic dạng compaction chỉ giữ giá trị mới nhất của mỗi key, hợp để nạp lại số dư hiện tại nhưng không phải lịch sử.

Cho audit, Kafka bảo đảm thứ tự trong một partition và vị trí của message không bao giờ đổi. Nhưng nó không bất biến: dữ liệu vẫn bị xoá theo thời hạn hoặc bởi người có quyền, nên phải giữ quyền xoá chặt và lưu lâu theo chính sách của ngân hàng.
