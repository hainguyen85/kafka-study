# Tóm tắt Bài 8d: Hợp đồng sự kiện (schema)

[← Tóm tắt Bài 8c](0015-replay-retention.md) · [Bài học](../lessons/0016-schema-evolution.html) · [Tóm tắt Bài 9 →](0009-final-design-test.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Kafka chỉ lưu byte, nó không biết message của bạn có những trường nào. Khi nhiều team cùng đọc một luồng sự kiện, cái giữ cho họ không vỡ là một hợp đồng: schema được lưu trong một registry đặt ngoài Kafka. Mỗi message chỉ mang số định danh của schema, phía đọc dùng số đó để lấy schema về.

Registry kiểm tra mọi thay đổi trước khi cho phép. Mặc định nó cho phép những thay đổi mà phía đọc mới vẫn đọc được dữ liệu cũ, nên phải nâng cấp phía đọc trước, phía ghi sau. Thay đổi an toàn nhất là thêm hoặc bỏ một trường không bắt buộc, có giá trị mặc định. Thêm trường bắt buộc thì registry từ chối ngay; trong lab, phiên bản đó không gửi được message nào. Đổi kiểu dữ liệu của một trường thì nên làm topic mới.

Với ngân hàng dùng dịch vụ của AWS, registry thường là AWS Glue, miễn phí, phân quyền bằng IAM, và mặc định không tự đăng ký schema. Câu hỏi cho platform team: dùng registry nào, ai được đăng ký schema, và mức kiểm tra mặc định của tổ chức.
