# Workspace học (Kafka)

Thư mục này là workspace của `/teach`. Đọc `MISSION.md`, `NOTES.md`, `learning-records/` trước khi dạy. `NOTES.md` giữ sở thích học và quy trình; file này nói skill nào là chủ đạo và khi nào dùng thêm kỹ thuật phụ trợ.

## Skill chủ đạo: `teach`

`teach` quyết định cách dạy: cấu trúc workspace, lesson, reference, learning record, vùng phát triển gần, quy tắc quiz, trích nguồn, và cân bằng kiến thức - kỹ năng - trí tuệ (cộng đồng). Mọi buổi học đi theo `teach`.

Các skill bên dưới chỉ hỗ trợ một khâu trong buổi học. Chúng không đổi cấu trúc, định dạng hay triết lý của `teach`. Nếu hai bên khác nhau thì `teach` thắng.

## Skill phụ trợ (khi nào dùng)

Nếu có `.agents/skills/<tên>/SKILL.md` thì đọc và làm theo; nếu không thì làm theo phần tóm tắt dưới đây.

- **Mission mơ hồ hoặc muốn đổi** (`MISSION.md` trống, hoặc người học nói mục tiêu đã khác): làm như skill `grilling`.
  Hỏi theo vòng. Mỗi vòng hỏi hết các câu đã đủ điều kiện hỏi (không phụ thuộc câu còn mở), đánh số, kèm đáp án bạn đề xuất,
  sao cho "đồng ý" là chấp nhận đề xuất. Xong khi không còn nhánh nào chưa hỏi.
  Chỉ sửa `MISSION.md` sau khi người học xác nhận, và ghi một learning record (theo `teach`).
  Chỉ dùng cho mission và quyết định lớn, không dùng cho câu hỏi nhỏ trong bài.
- **Cần kiến thức mà `RESOURCES.md` chưa phủ** (ví dụ cấu hình mới, hành vi theo phiên bản): làm như skill `research`.
  Chủ đề mới hoặc nhiều claim: chạy một background agent với context mới. Kiểm tra nhỏ (một hai claim): tự đọc trực tiếp cũng được.
  Cả hai cách đều chỉ đọc primary source (kafka.apache.org, KIP, source code), không đọc bài viết thứ cấp.
  Mỗi claim kèm nguồn. Ghi kết quả vào `RESOURCES.md` theo `RESOURCES-FORMAT.md` của `teach`. Không dạy từ trí nhớ của bạn.
- **Người học dùng thuật ngữ mới, mơ hồ hoặc dùng sai** (ví dụ lẫn offset commit với offset log): làm như skill `domain-modeling`.
  Chỉ ra xung đột với `GLOSSARY.md`, đề xuất thuật ngữ chuẩn, đưa tình huống biên để người học tự phân biệt.
  Theo `teach` và `NOTES.md`: chỉ thêm thuật ngữ vào `GLOSSARY.md` khi người học dùng đúng (quiz hoặc bài tập). Theo `GLOSSARY-FORMAT.md` của `teach`.
  Glossary chỉ chứa định nghĩa, không chứa cấu hình hay quyết định triển khai. Bỏ qua phần ADR.
- **Người học nói "không hiểu", "nói lại đi"**: làm như skill `wait-what`. Cho một câu bối cảnh, rồi giải thích lại
  bằng câu ngắn, mỗi câu một ý, dùng đúng thuật ngữ trong `GLOSSARY.md` (chưa có thì dùng thuật ngữ của `reference/`).
  Không thêm khái niệm mới trong lần nói lại.
  (Skill gốc dùng ASD-STE100, chuẩn tiếng Anh; ở đây áp dụng tinh thần đó cho tiếng Việt.)
- **Bước chỉ người học làm được** (cài Docker Engine trong WSL2, dựng cụm 3 broker, mở UI): làm như skill `wizard`.
  Liệt kê các stage theo thứ tự cho người học xác nhận, rồi sinh script bash dẫn từng bước, chạy được trong WSL.
  Chỉ dùng khi bước dài và lặp lại được; một lệnh đơn thì để lesson hướng dẫn.
  Không dùng cho việc bạn tự làm được.

## Độ dài và độ sâu của lesson

- Theo `teach`: mỗi bài có một chiến thắng cụ thể, nằm trong vùng phát triển gần, vừa với trí nhớ làm việc của người học.
- Ngắn không có nghĩa là mỏng. Một bài đủ phải có:
  1. kiến thức tối thiểu để có kỹ năng đó, có trích nguồn;
  2. thực hành có phản hồi tức thì (code hoặc cấu hình chạy được trên cụm, quiz, thí nghiệm), không chỉ đọc;
  3. ít nhất một usecase "oái oăm" cho thấy cấu hình nào xử lý được vấn đề nào.
- Mục tiêu là một buổi học thực chất (gồm cả thời gian chạy code), không phải mẩu vài phút chỉ có lý thuyết.
- Nếu bài chứa quá nhiều khái niệm mới: tách thành hai bài, mỗi bài vẫn đủ ba phần trên. Không bỏ phần thực hành để giữ bài ngắn.
- Code minh hoạ đặt trong `labs/<số bài>-<tên>/`, chạy được. Lesson ghi lệnh chạy và kết quả mong đợi.

## Không làm

- Không dùng skill thuộc flow dựng phần mềm (`to-spec`, `to-tickets`, `tdd`, `implement`) trong workspace này.
- Kết quả `research` là nguyên liệu cho `RESOURCES.md`, không dán nguyên vào lesson.
