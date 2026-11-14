# Sơ đồ cho usecase; thời điểm auto-commit và close()

2026-11-13. Người học yêu cầu vẽ sơ đồ kiến trúc/flow/sequence cho các usecase để trực quan, dễ nhớ và dùng cho thiết kế; chọn Mermaid để xem được trong HTML. Đã thêm 33 sơ đồ cho usecase ở Bài 2-7, 4b, 5b, 5c, một sơ đồ kiến trúc cho đáp án Bài 9 và một sequence cho thời điểm auto-commit ở Bài 5.

Cùng lúc người học hỏi (Bài 5): hết `auto.commit.interval.ms` mà chưa xử lý xong lô thì có commit không; tắt auto-commit thì `close()` có commit không. Câu trả lời trước đó trong bài ("commit định kỳ trong poll() là cách hiểu") chưa kiểm, và câu "bất kể đã xử lý xong hay chưa" dễ hiểu sai. Đã kiểm source: commit chỉ xét trong poll() (cả classic và KIP-848), close() chỉ commit khi auto-commit bật; Spring container commit offset đã xử lý khi dừng bình thường.

Implications: người học đào sâu tới mức "chính xác lúc nào" và đã phát hiện chỗ bài mơ hồ; với câu hỏi kiểu này, kiểm source code (primary) thay vì suy từ javadoc. Người học ghi nhớ tốt hơn qua hình: usecase mới phải có sơ đồ.
