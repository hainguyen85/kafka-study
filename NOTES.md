# Ghi chú cho giáo viên (agent)

## Sở thích của người học
- Học bằng tiếng Việt; thuật ngữ Kafka giữ nguyên tiếng Anh (topic, partition, offset...).
- Thích usecase "oái oăm" (tình huống dễ sai) + code sinh sẵn để xem cấu hình chạy ra sao.
- Tech lead/mix; nơi làm mới là banking nhưng bài học KHÔNG cần gắn với banking. Dùng usecase đa dạng (gọi xe, e-commerce/flash sale, IoT, game, logistics, thanh toán...) để thấy nhiều dạng vấn đề; mục tiêu là hiểu cách cấu hình xử lý từng dạng vấn đề. Đôi khi thêm một dòng "nếu là banking thì..." khi thật sự hữu ích.
- Đã bảo tôi hỏi lại khi chưa rõ.
- Muốn hiểu nền tảng (bức tranh tổng thể) trước khi đi sâu từng thành phần: đừng nhảy vào chi tiết khi chưa dựng bản đồ.

## Hồ sơ người học (cập nhật 2026-11-11)
- Senior backend 15+ năm, Java + Spring Boot rất vững; mới học Kafka (concept + cấu hình). Không giải thích Spring/Java cơ bản, không ví dụ ngây thơ; nói thẳng đánh đổi và bẫy.
- Học full-time, muốn xong trong 2 ngày. Bài dày hơn nhưng vẫn một chiến thắng mỗi bài, thực hành là chính.
- Nơi làm mới dùng Kafka cloud service trên AWS (nhiều khả năng Amazon MSK, chờ xác nhận) nên không cần đi sâu dựng/vận hành cluster. Cụm Docker tự dựng chỉ để thực hành cấu hình topic/partition/offset/producer/consumer.
- Research về MSK: research/0003-msk.md (đang chạy). Ghi chú "Apache Kafka tự vận hành" trước đó có thể sai.

## Môi trường
- Windows 11 Home, WSL2 Ubuntu 24.04, chưa có Docker (2026-11-10). Java 25 + 8, Maven 3.8.6, Node, Python, Go.
- Kafka hiện tại theo kafka.apache.org/quickstart: 4.4.0 (cần Java 17+). Image: `apache/kafka`.
- Chọn: dựng cụm bằng Docker Engine trong WSL2, code Java với Spring Kafka (nơi mới dùng Spring Kafka; người học là người tạo topic; Apache Kafka tự vận hành; yêu cầu chính là không mất message).
- Nguồn Spring Kafka: research/0002-spring-kafka.md (đang chạy, chưa dùng để sinh code cho tới khi đọc xong).

## Lộ trình 2 ngày full-time (người học chấp nhận 2026-11-11)
Người học là senior: dạy dày, đi thẳng vào đánh đổi và bẫy. Hạ tầng không phải trọng tâm. Cụm Docker chỉ là sandbox.
Ngày 1: (1) Bài 1-2 gộp, đọc nhanh + quiz (30-45p) | (2) Sandbox: wizard + CLI + kill 1 broker, rút gọn (1h) | (3) Producer + độ bền, lab 04 (2h) | (4) Consumer, offset, commit, rebalance với Spring Kafka (2,5h) | (5) Truy hồi cuối ngày (30p)
Ngày 2: (6) Lỗi xử lý: poison pill, retry, DLT, max.poll.interval.ms (2h) | (7) Idempotent consumer, outbox, transactions, giới hạn exactly-once (2h) | (8) Thiết kế topic: partition, retention, compaction, schema (1 bài ngắn), điểm khác của dịch vụ managed (2h) | (9) Bài kiểm tra cuối "chém" + bảng câu hỏi ngày đầu (1,5h)
Bỏ: Share Groups, Connect, Streams, bảo mật, vận hành cụm.
Code usecase oái oăm: labs/05-scenarios (chạy được từng profile), sổ tay: reference/0003-tricky-scenarios.html.

## Quy ước
- GLOSSARY.md chưa tạo: chỉ thêm thuật ngữ khi người học dùng đúng (quiz/bài tập). Bảng tóm tắt bài 1-2 nằm ở reference/0001-core-concepts.html.
- Sau khi sửa lessons/ hoặc reference/: chạy `python scripts/html2md.py`.

## Quy trình theo AGENTS.md (ghi sau lần review 2026-11-10)
- Đổi mission: hỏi theo vòng kiểu `grilling` (đánh số, kèm đề xuất), chỉ sửa MISSION.md sau khi người học xác nhận.
- Bài 3 (cài Docker trong WSL2, dựng cụm): dùng `wizard` (liệt kê stage cho người học xác nhận, rồi sinh script bash chạy trong WSL).
- Kiến thức mới ngoài RESOURCES.md: chạy `research` (background agent), mỗi claim kèm nguồn. Mọi câu trong lesson về hành vi Kafka phải có nguồn hoặc ghi rõ là minh hoạ.
- Thuật ngữ chưa chốt, cần quyết khi người học bắt đầu dùng: "event" vs "message" vs "record" (docs dùng cả ba); "position" vs "committed offset". Các bài hiện dùng "event/message" lẫn nhau.
- Độ dài lesson (người học nhắc 2026-11-10): một chiến thắng mỗi bài, nhưng ngắn quá cũng không nên. Mỗi bài đủ kiến thức có nguồn + thực hành có phản hồi (code/cụm/quiz) + usecase oái oăm. Skill `teach` chủ đạo, các skill khác chỉ phụ trợ (xem AGENTS.md).
- Bài 1 và 2 hiện chỉ có khái niệm + mô phỏng + quiz, chưa có code chạy thật: từ Bài 3 trở đi mỗi bài kèm code trong `labs/`.
- Wizard bài 3: `scripts/wizard-03-docker-kafka.sh` (6 stage người học đã chấp nhận). Mới kiểm tra `bash -n`, chưa chạy thật. Lab 04 (`labs/04-producer-durability`) đã biên dịch được, chưa chạy với cụm thật: lesson bài 4 không được khẳng định kết quả dự kiến như đã quan sát (người học đã yêu cầu viết bài trước, xem dòng "Lesson 4-9 đã viết" bên dưới).
- Cụm lab là combined mode: kill 2 node = mất đa số controller (khác báo cáo research/0001 T2 giả định controller tách riêng). Thí nghiệm "NotEnoughReplicas" dùng topic min.insync.replicas=3 và kill 1 node.
- JAVA_HOME máy người học là JDK 8: Maven cho lab Spring Boot 4 phải chạy bằng JDK 17+ (đặt JAVA_HOME tạm thời).
- Lab 05 (2026-11-11): đã chạy trên broker nhúng: s7, s8, s8dlt, s9, s9dedup, s11, s12, s6fix. Còn việc: biên dịch lại và chạy lại s6 + s10 (đã sửa lỗi), chạy s13. Lần chạy harness bị dừng vì máy thiếu bộ nhớ: chỉ chạy lại khi người học đồng ý, và chạy từng nhóm nhỏ (vd. `s6 s6fix s10`).
- Đính chính research/0002: tham số Acknowledgment không phải null ở chế độ không manual mà ném IllegalStateException (xem cuối mục đính chính trong file).
- Lesson 4-9 đã viết (2026-11-11) theo yêu cầu "viết bài trước theo lộ trình", dù chưa có kết quả chạy thật từ cụm 3 broker. Bù lại: mọi "Mong đợi" chưa kiểm chứng được gắn nhãn "dự đoán/chưa chạy" trong từng bài; kết quả đã quan sát (broker nhúng) ghi rõ là đã quan sát. Bài 4 dùng `assets/ack-sim.js`, bài 5 dùng `assets/commit-sim.js` (cả hai là mô phỏng theo mô hình trong docs, không phải broker/client thật), bài 8 dùng lại `assets/partition-sim.js`.
- Việc còn lại sau khi người học chạy lab: sửa các câu "Mong đợi" theo kết quả thật (lab 04, s6, s10, s13), ghi learning record dựa trên quiz và kết quả thẻ thiết kế bài 9. Chưa ghi learning record cho bài 4-9 vì chưa có dữ liệu người học.
- Schema Registry: bài 8 chỉ nêu ranh giới, chưa dạy (chưa có nguồn chính). Nếu người học muốn: chạy `research` trước.
- Người học (2026-11-11): dịch vụ managed trên AWS là môi trường làm việc sắp tới; trong phạm vi bài học thì tự dựng cụm để học. Bài 4-9 dạy khái niệm trên cụm tự dựng, chỉ bài 8 và bảng reference/0004 nói về managed.
