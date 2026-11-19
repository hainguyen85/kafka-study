# Tóm tắt Bài 11: Bài toán notification hub

[← Tóm tắt Bài 10](0013-core-80-20.md) · [Bài học](../lessons/0017-notification-hub.html)

*Bài nói 1-2 phút để giải thích cho người khác. Hình kiến trúc, bảng từng chặng và mô phỏng nằm trong bài học.*

Ngân hàng có nhiều hệ thống tự gửi thông báo, mỗi hệ thống tự nối với từng nhà cung cấp, không ai thấy trạng thái và chi phí tập trung, và không có chỗ chung để OTP đi trước khuyến mãi. Hub là cổng duy nhất: nhận bản tin từ API Connect và từ Kafka, gửi ra năm kênh, hàng chục triệu bản tin mỗi ngày. Ba ràng buộc: tải rất lệch nên OTP không được chờ campaign; nhà cung cấp bên ngoài có thể sập; mất hay trùng đều là sự cố.

Kafka nằm giữa mọi chặng, để mỗi bên chạy theo tốc độ của mình. Nó có năm vai trò. Thứ nhất, lớp đệm bền: đỉnh tải hay nhà cung cấp sập chỉ làm hàng chờ dài ra, không làm mất. Thứ hai, làn ưu tiên: Kafka đọc mỗi partition theo thứ tự, nên OTP phải có topic và nhóm consumer riêng, không thể "chen hàng" trong topic chung. Thứ ba, cách ly nhà cung cấp: mỗi kênh một topic và một nhóm consumer; kênh nào sập thì tạm dừng đọc kênh đó, lỗi tạm thời đi sang topic retry, hết lượt thì vào DLT có cảnh báo. Thứ tư, không mất: ba bản sao, chờ đủ bản sao mới trả lời "đã nhận", xử lý xong mới commit. Thứ năm, không trùng: mỗi bản tin có một mã, hub kiểm mã đó khi nhận và trước khi gửi.

Giới hạn trung thực nằm ở chặng cuối: khi gọi nhà cung cấp bị timeout, hub không biết tin đã đi chưa. Gửi lại thì có thể trùng, không gửi thì có thể mất. Chỉ khi nhà cung cấp có khoá chống trùng hoặc API tra trạng thái thì mới đúng một lần, nên đó là tiêu chí chọn nhà cung cấp.

Nút cổ chai không phải Kafka: 30 triệu tin một ngày chỉ khoảng 350 tin mỗi giây trung bình. Nút cổ chai là hạn mức và độ trễ của nhà cung cấp.

Năm chỗ hay bị hỏi sâu. Queue: có, dùng Kafka ở mọi chỗ hai bên chạy khác tốc độ; chỗ cần trả lời ngay thì dùng HTTP. Retry: thang chờ tăng dần là của kênh, còn số lần và hạn chót là của policy gắn vào từng bản tin, theo khách hàng và kênh; tới số lần thì đẩy thẳng sang DLT. Nhà cung cấp sập hẳn thì ngắt mạch chứ không retry. DLQ: với consumer thường đó là pattern ứng dụng; bản tin vào đó khi lỗi vĩnh viễn hoặc hết lượt, mang theo lý do và vị trí gốc, có cảnh báo, trả FAILED về nguồn, và được replay với cùng mã nên không trùng. Crash: ghi bền trước, commit sau, mọi bước chạy lại được; chỗ khó duy nhất là chết sau khi đã gọi nhà cung cấp. Trả kết quả: mọi trạng thái đi qua một topic; nguồn chọn tra cứu, webhook hoặc topic kết quả của riêng mình, và phải chống trùng vì kết quả cũng đến ít nhất một lần.
