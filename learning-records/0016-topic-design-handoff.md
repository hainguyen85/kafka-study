# Người học thiết kế topic; team riêng tạo topic production

2026-11-18. Người học không chắc mình là người tự tạo và cấu hình topic ở nơi mới (ghi nhận ngày 2026-11-10 ở record 0004 và MISSION.md là "tự tạo và cấu hình topic"). Người học xác nhận: Bài 8 chỉ tập trung thiết kế topic và các cấu hình cần thiết; khi go-live, một team riêng tạo và cấu hình topic trên production dựa vào tài liệu thiết kế topic.

Implications: kỹ năng đích của Bài 8 là viết được tài liệu thiết kế topic đủ để team khác tạo topic mà không cần hỏi lại (tên, key, partition, RF/minISR, retention hoặc compaction, kích thước message, schema, topic retry/DLT, tải dự kiến, ai ghi ai đọc), và kiểm lại topic sau khi tạo bằng `--describe`. Lab vẫn tự tạo topic trên cụm học. Ứng dụng không nên tự tạo topic production, kể cả topic retry/DLT của `@RetryableTopic`. Quy ước đặt tên topic là của nơi mới: hỏi platform team.
