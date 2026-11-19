# Tóm tắt Bài 11: Bài toán notification hub

[← Tóm tắt Bài 10](0013-core-80-20.md) · [Bài học](../lessons/0017-notification-hub.html)

*Bài nói 1-2 phút để giải thích cho người khác. Hình kiến trúc, bảng từng chặng và mô phỏng nằm trong bài học.*

Ngân hàng có nhiều hệ thống tự gửi thông báo, mỗi hệ thống tự nối với từng nhà cung cấp, không ai thấy trạng thái và chi phí tập trung, và không có chỗ chung để OTP đi trước khuyến mãi. Hub là cổng duy nhất: nhận bản tin từ API Connect và từ Kafka, gửi ra năm kênh, hàng chục triệu bản tin mỗi ngày. Ba ràng buộc: tải rất lệch nên OTP không được chờ campaign; nhà cung cấp bên ngoài có thể sập; mất hay trùng đều là sự cố.

Kafka nằm giữa mọi chặng, để mỗi bên chạy theo tốc độ của mình. Nó có năm vai trò. Thứ nhất, lớp đệm bền: đỉnh tải hay nhà cung cấp sập chỉ làm hàng chờ dài ra, không làm mất. Thứ hai, làn ưu tiên: mỗi hàng được đọc lần lượt, nên OTP phải có hàng riêng, không thể "chen hàng" trong hàng chung. Thứ ba, cách ly nhà cung cấp: mỗi kênh một hàng; kênh nào sập thì tạm dừng kênh đó, lỗi tạm thời đi sang hàng thử lại, hết lượt thì vào DLQ có cảnh báo. Thứ tư, không mất: lưu nhiều bản sao rồi mới trả lời "đã nhận", xử lý xong mới đánh dấu "đã đọc". Thứ năm, không trùng: mỗi bản tin có một mã, hub kiểm mã đó khi nhận và trước khi gửi.

Giới hạn nằm ở chặng cuối: khi gọi nhà cung cấp mà hết giờ chờ, hub không biết tin đã đi chưa. Gửi lại thì có thể trùng, không gửi thì có thể mất. Chỉ khi nhà cung cấp chống gửi trùng hoặc cho tra trạng thái thì mới đúng một lần, nên đó là tiêu chí chọn nhà cung cấp.

Nút cổ chai không phải Kafka: 30 triệu tin một ngày chỉ khoảng 350 tin mỗi giây trung bình. Nút cổ chai là hạn mức và tốc độ của nhà cung cấp.

Năm chỗ hay bị hỏi sâu. Message queue: Kafka chính là queue của hub, mọi chặng giữa hai thành phần đi qua Kafka; chỗ cần trả lời ngay thì gọi trực tiếp. Retry: thang chờ tăng dần là của kênh, còn số lần và hạn chót đi theo từng tin, theo khách hàng và kênh; tới số lần thì tin vào DLQ. Nhà cung cấp sập hẳn thì ngắt mạch chứ không thử lại. DLQ: giữ lại bản tin không gửi được để người xử lý; vào khi lỗi vĩnh viễn hoặc hết lượt, báo thất bại về nguồn, có cảnh báo, sửa xong nguyên nhân thì gửi lại với cùng mã nên không trùng. Crash: lưu xong rồi mới đánh dấu "đã đọc", mọi bước làm lại được; chỗ khó duy nhất là chết ngay sau khi gửi. Trả kết quả: mọi trạng thái đi qua một luồng; nguồn chọn tự hỏi, được gọi lại, hoặc đọc topic kết quả của riêng mình, và phải bỏ qua kết quả trùng vì kết quả có thể tới hai lần.
