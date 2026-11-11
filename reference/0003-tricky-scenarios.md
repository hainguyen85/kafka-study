<!-- Tự sinh từ 0003-tricky-scenarios.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bảng tra · [Kafka managed trên AWS](0004-managed-kafka-msk-vs-confluent.md) · [Lab 05](../labs/05-scenarios/README.md) · [Lab 04](../labs/04-producer-durability/README.md)

# Sổ tay: usecase oái oăm và cách xử lý

Triệu chứng, nguyên nhân, cấu hình hoặc thiết kế để xử lý, code để thử, và **trạng thái kiểm chứng trung thực**. Cập nhật 2026-11-11. Danh mục gốc (16 tình huống, có nguồn) nằm ở [research/0001, mục 2](../research/0001-curriculum-coverage.md).

## Trạng thái kiểm chứng: đọc trước

- **Đã chạy**: code đã chạy thật trên broker Kafka nhúng 1 node (KRaft) và kết quả bên dưới là kết quả quan sát được. Broker nhúng chỉ có 1 node nên **không kiểm tra được độ bền đa broker** (acks, ISR, kill broker).
- **Có code**: code biên dịch được nhưng chưa chạy với cụm 3 broker (máy chưa có Docker). Phần "kết quả dự kiến" trong README là dự đoán từ tài liệu.
- **Đã sửa, chưa chạy lại**: lần chạy thử phát hiện lỗi trong code của mình, đã sửa nhưng chưa biên dịch và chạy lại (harness bị dừng khi máy thiếu bộ nhớ).
- **Chưa có code**: chỉ mô tả từ tài liệu chính thức.

## A. Ghi message (producer)

| Mã | Tình huống | Nguyên nhân | Xử lý | Code | Trạng thái |
|---|---|---|---|---|---|
| P1 | Flash sale: đơn "đã nhận" nhưng biến mất sau khi broker leader chết | `acks=1` chỉ leader xác nhận; hoặc ISR co lại còn mình leader và `min.insync.replicas=1` | `acks=all` (mặc định client 4.x), RF=3, `min.insync.replicas=2` đặt ở topic. **MSK Standard:** `unclean.leader.election.enable` mặc định **true**, phải đặt false | Lab 04: `risky` so với `safe`, rồi `verify` | Có code |
| P2 | IoT: cụm "sống" nhưng producer không ghi được | ISR nhỏ hơn `min.insync.replicas` sau khi follower bị loại (`replica.lag.time.max.ms` 30 giây) → `NotEnoughReplicasException`; producer thử lại tới hết `delivery.timeout.ms` | Đặt `min.insync.replicas` ≤ RF-1 (AWS khuyến nghị); chọn đánh đổi bền/sẵn sàng theo domain | Lab 04: `strict` (topic minISR=3, kill 1 broker) | Có code |
| P3 | Game: partition "chết" thay vì mất dữ liệu | Khi mọi replica ISR chết, Kafka chờ replica nhất quán (`unclean.leader.election.enable=false`) | Giữ false; chấp nhận mất sẵn sàng. Bật true là đổi dữ liệu lấy sẵn sàng | - | Chưa có code |
| P4 | Logistics: DELIVERED đến trước PICKED_UP dù cùng key | Retry + `max.in.flight>1` khi idempotence tắt có thể đảo thứ tự | Giữ `enable.idempotence=true` (mặc định 4.x), in-flight ≤ 5. Đặt tường minh idempotence=true cùng acks≠all sẽ ném `ConfigException` (đã kiểm chứng bằng client thật) | - | Chưa có code (khó tái hiện tất định) |
| P5 | Quét tài liệu: message 2 MB, sửa xong tầng này lại lỗi tầng khác | Bốn tầng giới hạn độc lập: `max.request.size` (producer), `max.message.bytes` (topic/broker), `replica.fetch.max.bytes`, fetch size của consumer (không phải giới hạn cứng) | Nâng đồng bộ từng tầng; nén; hoặc giữ file ngoài Kafka rồi gửi tham chiếu | Lab 05: `s11` | **Đã chạy** |

**Kết quả quan sát S11:** lần 1 (mặc định): `RecordTooLargeException: The message is 2000089 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.` Lần 2 (producer 3 MB, topic mặc định): `RecordTooLargeException: The request included a message larger than the max message size the server will accept.` Lần 3 (topic `max.message.bytes=3000000`): thành công. Lần 4: consumer mặc định (`max.partition.fetch.bytes` 1 MB) vẫn đọc được record 2 MB.

## B. Đọc message (consumer)

| Mã | Tình huống | Nguyên nhân | Xử lý | Code | Trạng thái |
|---|---|---|---|---|---|
| C1 | Gửi email: vừa mất vừa trùng sau crash | Auto-commit đẩy offset đi trước khi việc xử lý bất đồng bộ xong | `enable.auto.commit=false`, ack thủ công **sau** khi xử lý xong (`ack-mode=manual_immediate`) → at-least-once, rồi chống trùng ở consumer (E2) | Lab 05: `s6`, `s6fix` | `s6fix`: **Đã chạy** (MẤT 0, TRÙNG 0 với crash giả lập đóng êm). `s6`: **Đã sửa, chưa chạy lại** (lần chạy đầu lỗi vì khai báo tham số `Acknowledgment` ở chế độ không manual) |
| C2 | OCR/xử lý ảnh: bị đá khỏi group, rebalance liên hồi, xử lý lặp | Không gọi `poll()` trong `max.poll.interval.ms` (mặc định 300000) → coi là chết | Giảm `max.poll.records` hoặc tăng `max.poll.interval.ms`; đẩy việc nặng sang worker (và pause/resume) | Lab 05: `s7` | **Đã chạy**: cả 3 offset đầu đều được xử lý **3 lần** |
| C3 | Gọi xe: mỗi lần deploy cả group dừng vài giây | Giao thức rebalance cũ (classic) có rào chắn toàn group | Static membership (`group.instance.id` khác nhau mỗi instance; AWS khuyên luôn đặt); assignor cooperative sticky; hoặc `group.protocol=consumer` (KIP-848, Kafka ≥ 4.0) | - | Chưa có code (cần ≥ 2 process) |
| C4 | Job ETL nghỉ lễ quay lại: không đọc được gì hoặc nhảy tới cuối | Offset đã commit hết hạn sau `offsets.retention.minutes` (7 ngày); khi đó `auto.offset.reset` (mặc định `latest`) quyết định | `earliest` hoặc `none`; tăng retention theo chu kỳ job | - | Chưa có code |

## C. Lỗi và trùng

| Mã | Tình huống | Nguyên nhân | Xử lý | Code | Trạng thái |
|---|---|---|---|---|---|
| E1 | Một message hỏng (poison pill) | Mặc định Spring Kafka `DefaultErrorHandler` thử lại 10 lần giao rồi **chỉ log ERROR và bỏ qua**: message mất im lặng | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `FixedBackOff`; topic `*-dlt` phải có ít nhất số partition của topic gốc. Đây là tính năng của Spring Kafka, docs Apache chỉ có DLQ cho Connect và Share Groups | Lab 05: `s8` so với `s8dlt` | **Đã chạy**: `s8` xử lý m0-m4, m6-m9, POISON không ở đâu cả; `s8dlt`: POISON nằm trong DLT |
| E2 | Đặt vé/tích điểm: bị cộng gấp đôi dù producer đã bật idempotence | Idempotence chỉ chống trùng trong một session của producer; ứng dụng tự gửi lại thì broker thấy hai record khác nhau | Chống trùng ở consumer bằng mã sự kiện, kiểu `UNIQUE(eventId)` trong cùng transaction với tác dụng phụ (outbox/inbox là pattern ứng dụng, không có docs chính thức) | Lab 05: `s9` so với `s9dedup` | **Đã chạy**: `s9` điểm {20,40,60,80,100} thay vì {10..50}; `s9dedup` đúng. Dedup trong RAM chỉ để minh hoạ |
| E3 | Kho + đơn hàng: consume-transform-produce chỉ được ghi 1 lần | Transaction + `isolation.level=read_committed`; consumer mặc định `read_uncommitted` thấy cả bản ghi của transaction đã abort. Phạm vi exactly-once chỉ trong Kafka | `spring.kafka.producer.transaction-id-prefix` (khác nhau mỗi instance), consumer `read-committed`, `enable-auto-commit=false` | Lab 05: `s10` | **Đã sửa, chưa chạy lại**. Lần chạy đầu cho thấy `send` bất đồng bộ bị hủy cả lô khi transaction abort (`TransactionAbortedException`), nên phải chờ `.get()` mới thấy bản aborted |

## D. Thiết kế dữ liệu

| Mã | Tình huống | Nguyên nhân | Xử lý | Code | Trạng thái |
|---|---|---|---|---|---|
| D1 | IoT: thêm partition làm thứ tự theo thiết bị bị vỡ | Key được băm theo số partition; Kafka không phân phối lại dữ liệu cũ và không giảm được số partition | Tính số partition từ đầu; dùng `auto.offset.reset` hợp lý khi tăng | Lab 05: `s12` | **Đã chạy**: tăng 3 lên 6 partition, **9/20 key đổi partition** |
| D2 | Bảng giá sản phẩm: kỳ vọng mỗi key một bản ghi nhưng vẫn thấy nhiều | Compaction chạy ngầm trên segment đã đóng, không có hạn chót cứng; tombstone bị xoá sau `delete.retention.ms` | `cleanup.policy=compact`; hiểu consumer chậm có thể bỏ lỡ lệnh xoá. **Managed:** Express chỉ compact sau 256 MB/partition; Serverless 120 partition compact | Lab 05: `s13` | **Có code, chưa chạy** |
| D3 | Microservice dùng chung cụm: gõ sai tên topic, không ai báo lỗi | Apache bật auto-create: topic mới lấy 1 partition và RF=1. MSK và Confluent Cloud tắt auto-create | Tạo topic có chủ đích (`NewTopic` / IaC) và kiểm tra quyền tạo topic | Lab 03 wizard (stage 5), `TopicsConfig` | Có code |

## Cách dùng sổ tay khi "chém"

- Mở bằng **triệu chứng** ("message biến mất", "bị cộng gấp đôi", "partition chậm") thay vì bằng tên cấu hình. Người nghe thường kể triệu chứng trước.
- Nói **đánh đổi** của cách xử lý: `min.insync.replicas=2` đổi sẵn sàng lấy bền; ack thủ công đổi tốc độ lấy an toàn; DLT đổi độ phức tạp lấy dấu vết.
- Gắn với dịch vụ thật: xem [bảng MSK / Confluent Cloud](0004-managed-kafka-msk-vs-confluent.md) để biết cấu hình nào bị khoá hoặc mặc định khác.
