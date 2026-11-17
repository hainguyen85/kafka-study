# Tóm tắt Bài 8: Thiết kế topic

[← Tóm tắt Bài 7](0007-idempotent-consumer-transactions.md) · [Bài học](../lessons/0008-topic-design.html) · [Tóm tắt Bài 8b →](0014-throughput-latency.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Tạo một topic là một quyết định thiết kế, không phải một thao tác. Ba thứ khó đổi nhất cần chốt từ đầu.

Key: theo đối tượng cần giữ thứ tự, như mã tài khoản, và tránh key quá nóng. Số partition: theo mức song song cần có lúc đỉnh tải, cộng dư địa, vì chỉ tăng được và tăng thì key đổi partition. Schema: coi định dạng message là hợp đồng giữa các team, có mã sự kiện và số phiên bản ngay từ đầu.

Sau đó là độ bền và vòng đời dữ liệu: ba bản sao với `min.insync.replicas=2` cho dữ liệu không được mất. Cần lịch sử thì giữ theo thời gian; chỉ cần trạng thái mới nhất của mỗi key thì dùng compaction. Message mặc định giới hạn khoảng 1 MB; file lớn thì để ngoài Kafka và chỉ gửi tham chiếu.

Trên dịch vụ managed, nhiều giá trị bị khoá hoặc khác mặc định, nên hỏi platform team trước khi chốt.
