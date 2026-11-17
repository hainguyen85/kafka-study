# Mở rộng mission ngoài "không mất"; bài tổng kết 80/20

2026-11-17. Người học hỏi: chỉ tập trung "không mất" có đủ không, có bỏ sót concept và ứng dụng khác của Kafka không, và trong tài chính, chứng khoán thì ưu tiên mission nào. Đối chiếu lộ trình: mất, trùng, lỗi, rebalance đã học kỹ; thứ tự mới ở mức cơ bản; quá tải và độ trễ, đọc lại và audit, hợp đồng sự kiện (schema) gần như chưa có.

Người học xác nhận:
1. Mở rộng mission: giữ "không mất" làm trục, thêm ba mảng (a) quá tải, thông lượng, độ trễ; (b) đọc lại, audit, retention; (c) hợp đồng sự kiện, đưa Schema Registry vào phạm vi. Bảo mật và DR vẫn ngoài phạm vi.
2. Nơi mới là ngân hàng có mảng chứng khoán: thêm usecase chứng khoán (market data, luồng lệnh). Bỏ chữ "oái oăm" khỏi tiêu đề usecase; làm một trang tổng hợp mọi usecase.
3. Mỗi mảng mới một bài theo khuôn hiện tại, research nguồn chính trước (đã giao research cho ba mảng).
4. Làm bài tổng kết theo nguyên tắc 80/20: 20% kiến thức giải thích 80% điều quan trọng, kèm các ngộ nhận phổ biến về phần 20% đó.

Cũng trong buổi này: Bài 3, 4, 5, 5b, 6, 7 đã cấu trúc lại theo khái niệm trong tiêu đề; người học xác nhận Bài 4, 6, 7 ổn.

Implications: bài tiếp theo không còn chỉ hỏi "có mất không" mà phải hỏi đủ "thứ tự, mất, trùng, tải, giữ bao lâu, ai đọc". Phần ưu tiên cho tài chính và chứng khoán trong buổi này là lập luận của giáo viên, chưa có nguồn chuẩn ngành: kiểm lại khi người học đã vào làm. Tên ngân hàng không ghi vào repo vì repo public.
