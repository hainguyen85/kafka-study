# Bài 3: quiz 1/5 do bài thiếu kiến thức, không phải do người học

2026-11-12. Người học làm quiz Bài 3 trước khi dựng cụm, được 1/5 (sai câu 1, 2, 3, 5) và phản hồi "các câu sai đều không có trong bài học". Kiểm tra lại thì đúng phần lớn: câu 3 (ISR co lại sau replica.lag.time.max.ms) chỉ nằm trong hộp "Mong đợi" của bước thực hành chưa làm; câu 5 (vì sao minISR=2 không phải 3) không được giải thích ở đâu trong bài; câu 1 (quorum 3 chịu 1 lỗi) và câu 2 (ZooKeeper bị gỡ) có nhưng chỉ là câu trích tiếng Anh ngắn, không giải thích hệ quả.

Đã sửa: mục 1 của Bài 3 giải thích đa số quorum bằng số cụ thể (chết 1 / chết 2), thêm ý ISR co giãn theo replica.lag.time.max.ms, thêm ý minISR 1/2/3 với RF=3, làm rõ ZooKeeper bị gỡ và dùng --bootstrap-server. Rà các bài khác: bổ sung ở Bài 6 cách đọc FixedBackOff(interval, số lần thử lại).

Implications: điểm 1/5 không phản ánh năng lực; cho người học làm lại quiz Bài 3 sau khi đọc mục 1 mới và chạy cụm. Quy tắc mới (ghi ở NOTES): mỗi câu quiz phải trả lời được từ phần kiến thức của bài, không chỉ từ hộp "Mong đợi" của lab.
