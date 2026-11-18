# Tóm tắt Bài 8: Thiết kế topic

[← Tóm tắt Bài 7](0007-idempotent-consumer-transactions.md) · [Bài học](../lessons/0008-topic-design.html) · [Tóm tắt Bài 8b →](0014-throughput-latency.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Topic được thiết kế trước khi viết code. Team ứng dụng quyết định và ghi vào tài liệu thiết kế topic; khi go-live, team vận hành tạo topic đúng theo tài liệu đó. Ba thứ khó đổi nhất cần chốt từ đầu.

Key: theo đối tượng cần giữ thứ tự, như mã tài khoản, và tránh key quá nóng. Số partition: theo mức song song cần có lúc đỉnh tải, cộng dư địa, vì chỉ tăng được và tăng thì key đổi partition. Schema: coi định dạng message là hợp đồng giữa các team, có mã sự kiện và số phiên bản ngay từ đầu.

Sau đó là độ bền và vòng đời dữ liệu: ba bản sao với `min.insync.replicas=2` cho dữ liệu không được mất. Cần lịch sử thì giữ theo thời gian; chỉ cần trạng thái mới nhất của mỗi key thì dùng compaction. Message mặc định giới hạn khoảng 1 MB; file lớn thì để ngoài Kafka và chỉ gửi tham chiếu.

Tên topic không đổi được: theo quy ước của nơi làm việc, phân cấp theo team hay mảng nghiệp vụ, và đừng dùng lẫn dấu chấm với gạch dưới. Tài liệu phải liệt kê cả topic phụ như DLT. Trên dịch vụ managed, nhiều giá trị bị khoá hoặc khác mặc định, nên hỏi platform team trước khi chốt, và sau khi topic được tạo thì kiểm lại từng dòng.
