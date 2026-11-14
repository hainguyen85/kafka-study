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
- Sau khi sửa lessons/, reference/, summaries/ hoặc portal/intro.md: chạy `python scripts/html2md.py` rồi `node scripts/build_portal.mjs` (portal đọc từ .md; quiz lấy từ .html). Bài mới: thêm vào PARTS trong build_portal.mjs; mô phỏng mới: thêm vào SIMS.

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
- 2026-11-12: người học lẫn partition với replica khi xem sơ đồ bài 1 (3 broker đều có P0-P2 → tưởng P0, P1, P2 cùng nội dung). Đã sửa sơ đồ (ghi nội dung đơn #1..#6 trong từng partition), thêm hộp "partition khác replica" và một câu quiz. Ứng viên GLOSSARY: partition, replica (chỉ thêm khi người học dùng đúng). Kiểm lại phân biệt này ở bài 3-4.
- 2026-11-12 (người học yêu cầu): mỗi bài có một file tóm tắt riêng `summaries/<cùng tên với lesson>.md` (CHỈ Markdown, viết tay): bài nói tóm tắt cả bài (60-90 giây) + mục "Hỏi nhanh, đáp gọn". Không có phần tự chấm. Người học dùng làm tài liệu ôn nhanh/tra cứu. Lesson link tới nó ở mục "Tự nói thành lời". Bài mới phải có file này; sửa bài thì sửa cả tóm tắt.
- 2026-11-12 (sau phản hồi quiz Bài 3): MỖI câu quiz phải trả lời được từ mục "Kiến thức tối thiểu" của chính bài đó, không chỉ từ hộp "Mong đợi" của bước lab (người học có thể làm quiz trước khi chạy lab). Câu trích tiếng Anh phải kèm giải thích hệ quả bằng tiếng Việt. Khi viết hoặc sửa bài: đối chiếu từng câu quiz với mục kiến thức.
- 2026-11-12 (người học yêu cầu): bỏ ZooKeeper khỏi bài học vì không phải concept cốt lõi. Nguyên tắc: không đưa lịch sử phiên bản/tutorial cũ vào lesson trừ khi nó đổi cách cấu hình hôm nay.
- 2026-11-12: thêm Bài 4b (`lessons/0010-idempotent-producer.html`, số file tăng dần nên là 0010 nhưng hiển thị "Bài 4b", nằm giữa Bài 4 và 5) theo yêu cầu người học: idempotent producer chi tiết + `assets/idempotence-sim.js`. Thí nghiệm ConfigException trong bài chưa chạy thử đúng lệnh.
- 2026-11-13: người học thấy "rebalance chưa được nói rõ": thêm Bài 5b (`lessons/0011-rebalance.html`, giữa Bài 5 và 6) + `assets/rebalance-sim.js`. Hỏi "khi nào dùng A/B/C" (mô phỏng commit): thêm bảng "Chọn chế độ nào?" vào Bài 5. Hỏi các ngữ nghĩa giao hàng và dùng gì trong tài chính: thêm bảng vào Bài 7 ý 1 (lập luận từ docs, ghi rõ không phải quy định ngành). Portal: bỏ nút "Bản HTML đầy đủ" theo yêu cầu.
- 2026-11-13: người học yêu cầu (1) giải thích kỹ at-most/at-least/exactly-once và dùng khi nào: thêm Bài 5c (`lessons/0012-delivery-semantics.html`, giữa 5b và 6), bảng ở Bài 7 chuyển sang 5c; (2) auto-commit commit khi nào, cụ thể trong Spring Boot: thêm mục "Commit offset xảy ra lúc nào, cụ thể" vào Bài 5 (Kafka client + bảng AckMode, trích docs); (3) lab producer/consumer phải dùng code + cấu hình Spring Boot: thêm `labs/03-spring-clients` (đã biên dịch, khởi động được, chưa chạy với cụm), thay console producer/consumer ở Bài 3 bước 3, Bài 5 bước 3, Bài 5b bước 1; chèn code + yml của lab 04 vào Bài 4 và của s6/s6fix vào Bài 5. Quy tắc mới: bước thực hành có producer/consumer dùng ứng dụng Spring Boot, kèm đoạn code và cấu hình ngay trong bài; CLI chỉ cho thao tác quản trị (describe, reset offset, kafka-configs).
- 2026-11-13: người học muốn mỗi usecase có sơ đồ kiến trúc/flow/sequence (dễ nhớ, dùng cho thiết kế). Quy ước: Mermaid, đặt trong `<figure class="diagram"><script type="text/mermaid">…</script><figcaption>Loại: ý chính</figcaption></figure>` ở cuối usecase; trang nạp `../assets/diagram.js` (Mermaid 11.4.1 từ jsdelivr, màu theo style.css, bấm để phóng to). html2md chuyển thành ```mermaid; portal tự vẽ. Tô màu nút bằng `class X bad|ok|warn` kèm classDef màu sáng (component đổi theo theme). Flowchart rộng thì dùng TB; sequence giữ ít bên tham gia, nhãn ngắn. Sơ đồ mới phải render thử (headless) trước khi chèn. Thời điểm commit đã kiểm trong source kafka-clients 4.2.0 và Spring Kafka 4.1.2 (RESOURCES.md).
- 2026-11-14: portal đã publish làm Claude artifact (riêng tư): https://claude.ai/artifact/43MsJAeF4uiugJMGdXJwYr. Cập nhật: `python scripts/html2md.py` → `node scripts/build_portal.mjs` → `node scripts/build_portal_artifact.mjs` (ra `dist/kafka-portal.html`, dist/ bị gitignore) → publish lại; từ phiên khác phải truyền URL trên. Route portal là `#<id>` (không còn `#/`), không dùng confirm/alert; link tới file trong repo thành chữ thường trên artifact.
