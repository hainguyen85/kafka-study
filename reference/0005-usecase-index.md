<!-- Tự sinh từ 0005-usecase-index.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bảng tra · [Sổ tay usecase (triệu chứng, nguyên nhân, code)](0003-tricky-scenarios.md)

# Tổng hợp usecase

48 usecase từ 12 bài, theo thứ tự học. Bấm tên usecase để mở đúng chỗ trong bài, nơi có sơ đồ và giải thích đầy đủ.

Trang này do `scripts/build_usecases.py` sinh từ các bài học; cột "Cách xử lý" lấy câu bắt đầu bằng "Xử lý" hoặc "Sửa" trong usecase.

## Bài 2: Key, partition, offset

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Gọi xe: tài xế "nhảy lùi" trên bản đồ](../lessons/0002-key-partition-offset.md#uc-a) | Mỗi vài giây, app tài xế gửi DriverLocationUpdated vào topic driver-locations (3 partition). Team làm bản đồ muốn hiển thị vị trí mới nhất của từng tài xế, nhưng lại dùng mã cuốc xe làm key (và giữa hai cuốc, key trống). | key = mã tài xế. |
| [(b) Flash sale: một shop làm nghẽn một partition](../lessons/0002-key-partition-offset.md#uc-b) | Topic orders dùng mã shop làm key để giữ thứ tự đơn theo shop. Ngày flash sale, một shop nhận 70% đơn, nên 70% message dồn vào một partition. | chọn key theo đối tượng thật sự cần giữ thứ tự và tránh key quá "to"; thêm consumer không giúp được partition nóng (Bài 8). |

## Bài 3: Dựng cụm 3 broker

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Kho lạnh mất điện cả giá đỡ (IoT)](../lessons/0003-build-cluster.md#uc-a) | Hệ thống cảm biến kho lạnh chạy 3 node Kafka combined. Hai node cắm chung một ổ điện và cùng tắt. | đặt các node ở các khu vực mất điện khác nhau, và ở môi trường quan trọng thì tách controller khỏi broker. |
| [(b) Topic "ma" do gõ sai tên (microservices dùng chung cụm)](../lessons/0003-build-cluster.md#uc-b) | Service A gửi vào order-evnts (gõ nhầm), service B đọc order-events. Không ai báo lỗi vì broker tự tạo order-evnts với 1 partition và RF=1 (giá trị mặc định ở mục 1). | tắt auto.create.topics.enable và tạo topic có chủ đích (lab của bạn đã làm đúng việc này; Stage 5 của wizard có kiểm tra). |

## Bài 4: Producer và độ bền

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Flash sale: "đơn đã nhận" rồi biến mất](../lessons/0004-producer-durability.md#uc-a) | Giao diện báo đặt hàng thành công vì producer nhận ack. Sau sự cố một broker, kho đếm thiếu đơn. | acks=all + RF=3 + minISR=2 ở cấp topic, kiểm bằng --describe. |
| [(b) Telemetry kho lạnh: cụm sống mà không ghi được](../lessons/0004-producer-durability.md#uc-b) | Topic cảm biến RF=3, minISR=2, acks=all. Hai broker giữ partition đó chết (giả sử cụm có controller riêng nên vẫn bầu được leader). | chọn theo loại dữ liệu: telemetry mất vài điểm không sao thì min.insync.replicas=1; sổ cái, thanh toán giữ min.insync.replicas=2 và báo lỗi cho người dùng. |
| [(c) Spring: lỗi gửi nằm im trong log](../lessons/0004-producer-durability.md#uc-c) | Service gọi template.send(...) không đọc future. Topic bị từ chối (ISR thấp), ứng dụng vẫn trả 200 cho người dùng. | whenComplete hoặc .get(timeout) khi cần chắc chắn, và quyết định ứng dụng làm gì khi lỗi (trả lỗi cho người dùng, hoặc ghi outbox, Bài 7). |
| [(d) API thanh toán treo 60 giây khi cụm có sự cố](../lessons/0004-producer-durability.md#uc-d) | Controller REST nhận yêu cầu, gọi template.send(...) rồi trả 202. Một sáng, mạng tới cụm Kafka đứt. | Hạ max.block.ms xuống mức API chịu được (vài giây) để lỗi nhanh thay vì treo lâu. |
| [(e) Nếu là ngân hàng](../lessons/0004-producer-durability.md#uc-e) | Cấu hình tham chiếu cho sự kiện tiền. | Một dòng cho buổi họp: "Topic giao dịch: RF=3, minISR=2, acks=all, idempotence bật, unclean.leader.election.enable=false, và producer đọc kết quả gửi". Đó là câu trả lời chuẩn cho "không mất"; phần còn lại (trùng) giải ở consumer. |

## Bài 4b: Idempotent producer

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Logistics: DELIVERED đến trước PICKED_UP](../lessons/0010-idempotent-producer.md#uc-a) | Team giao hàng đặt acks: "1" cho nhanh. Idempotence tắt âm thầm (điều kiện 1 bị vi phạm). | acks=all và ghi tường minh enable.idempotence=true. |
| [(b) Đặt vé: restart rồi gửi lại](../lessons/0010-idempotent-producer.md#uc-b) | Service đặt vé gửi BookingCreated, bị deploy lại trước khi nhận kết quả. Lúc khởi động nó gửi lại các booking "chưa chắc đã gửi". | eventId sinh một lần cho mỗi booking, consumer chống trùng theo nó (Bài 7), hoặc outbox để chỉ một tiến trình chịu trách nhiệm gửi. |
| [(c) "Ta tăng max.in.flight lên 10 cho throughput"](../lessons/0010-idempotent-producer.md#uc-c) | Nếu đặt tường minh idempotence: lỗi khởi động. Nếu không: idempotence tắt âm thầm, và với retry thì thứ tự không còn được đảm bảo. | giữ max.in.flight.requests.per.connection ≤ 5 và ghi tường minh enable.idempotence=true, để cấu hình sai lộ ra ngay lúc khởi động. |

## Bài 5: Consumer, offset, commit, rebalance

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Gửi email/SMS thông báo: vừa mất vừa trùng](../lessons/0005-consumer-offset-commit.md#uc-a) | Listener đẩy việc gửi email sang thread pool rồi return để "nhanh". Auto-commit đẩy offset đi; crash giữa chừng làm vài email không bao giờ đi (mất), và nếu bạn commit cuối lô thì vài cái khác đi hai lần. | ack sau khi xong (Thử 3), nhưng nếu việc gửi cần song song thì phải đợi worker xong trước khi ack (hoặc quản lý offset theo worker). |
| [(b) OCR chứng từ: bị đá khỏi group](../lessons/0005-consumer-offset-commit.md#uc-b) | Mỗi tài liệu 12 giây, max.poll.records=500 mặc định, max.poll.interval.ms 5 phút: một lô 500 record cần 100 phút. Consumer bị coi là chết, rebalance, và cả lô bị xử lý lại bởi người khác: "xử lý lặp liên hồi". | hạ max.poll.records cho vừa với thời gian xử lý, hoặc tăng max.poll.interval.ms (đánh đổi: phát hiện consumer thật sự chết chậm hơn). |
| [(c) ETL báo cáo tuần: nghỉ lễ quay lại thì "không có gì để đọc"](../lessons/0005-consumer-offset-commit.md#uc-c) | Group rỗng quá 7 ngày: offset đã commit bị xoá; auto.offset.reset=latest làm job nhảy tới cuối và bỏ qua mọi thứ đã tích. Xử lý: earliest (đọc lại hết) hoặc none (ném lỗi cho bạn biết); tăng offsets.retention.minutes theo chu kỳ job. | earliest (đọc lại hết) hoặc none (ném lỗi cho bạn biết); tăng offsets.retention.minutes theo chu kỳ job. |
| [(d) Gọi xe: deploy làm cả đội điều phối đứng hình](../lessons/0005-consumer-offset-commit.md#uc-d) | Rolling restart 10 instance, mỗi lần một instance rời/vào là một lần rebalance; giao thức classic có rào chắn toàn group. Xử lý theo mức: static membership (instance về lại đúng chỗ cũ, đừng để trùng group.instance.id), cooperative assignor, … | static membership (instance về lại đúng chỗ cũ, đừng để trùng group.instance.id), cooperative assignor, group.protocol=consumer (cần broker hỗ trợ; Kafka Connect chưa dùng được). |
| [(e) Nếu là ngân hàng](../lessons/0005-consumer-offset-commit.md#uc-e) | Cấu hình tham chiếu cho sự kiện tiền. | "Service nhận sự kiện chuyển tiền: tắt auto-commit, ack sau khi ghi DB xong, chống trùng bằng mã sự kiện": ba cụm từ này là câu trả lời đầy đủ cho "restart có sao không". |

## Bài 5b: Rebalance

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Gọi xe: rolling deploy 10 pod làm điều phối đứng hình](../lessons/0011-rebalance.md#uc-a) | Deploy thay lần lượt 10 pod. Mỗi pod tắt rồi bật là hai lần rebalance; với eager, mỗi lần cả group dừng. | cooperative (chỉ partition đổi chủ dừng), static membership với tên pod ổn định (restart nhanh không rebalance), hoặc giao thức consumer nếu broker hỗ trợ. |
| [(b) Autoscale theo lag thành "bão rebalance"](../lessons/0011-rebalance.md#uc-b) | Lag tăng, autoscaler thêm pod; thêm pod gây rebalance, lag tăng tiếp; lag giảm thì bớt pod, lại rebalance. Group dành nhiều thời gian chia lại hơn là đọc. | ngưỡng scale có độ trễ (cooldown), không scale vượt số partition (consumer thừa chỉ ngồi chờ, Bài 1), dùng cooperative. |
| [(c) Consumer chết đột ngột: 45 giây im lặng rồi xử lý trùng](../lessons/0011-rebalance.md#uc-c) | Pod bị OOM kill. Coordinator không nhận heartbeat nhưng chỉ loại nó sau session.timeout.ms. | consumer phải idempotent (Bài 7); chỉ hạ session.timeout.ms khi chấp nhận rủi ro rebalance oan do GC pause. |
| [(d) Nếu là ngân hàng](../lessons/0011-rebalance.md#uc-d) | Cấu hình tham chiếu cho sự kiện tiền. | "Service đọc giao dịch dùng cooperative + static membership, commit khi bị thu hồi partition, và consumer idempotent vì rebalance vẫn có thể gây xử lý lại." Câu cuối quan trọng nhất: giảm rebalance là tối ưu, chống trùng mới là bảo đảm. |

## Bài 5c: At-most-once, at-least-once, exactly-once

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) "Exactly-once" mà khách nhận hai SMS](../lessons/0012-delivery-semantics.md#uc-a) | Listener chạy trong transaction Kafka, gửi SMS rồi ghi sự kiện ra topic. Lần đầu ghi topic lỗi, transaction abort, record được giao lại, SMS đi lần hai. | tách việc gửi SMS sang consumer riêng đọc topic đã commit, và cho nó chống trùng. |
| [(b) At-most-once ngoài ý muốn](../lessons/0012-delivery-semantics.md#uc-b) | Team nghĩ mình đang at-least-once vì cấu hình producer chuẩn, nhưng listener giao việc cho thread pool rồi return (chế độ B ở Bài 5). Offset đi trước việc xử lý; crash là mất. | ack sau khi xử lý xong (chế độ C ở Bài 5), và kiểm cả nửa đọc chứ không chỉ cấu hình producer. |
| [(c) At-least-once với consumer không idempotent](../lessons/0012-delivery-semantics.md#uc-c) | Rebalance (Bài 5b) giao lại 300 record đã xử lý chưa commit; consumer cộng tiền lần nữa. Không có lỗi nào trong log. | chống trùng theo eventId trong cùng transaction DB với tác dụng phụ (Bài 7). |

## Bài 6: Poison pill, retry, DLT

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Cổng thanh toán: một message JSON thiếu trường chặn cả partition](../lessons/0006-error-handling-dlt.md#uc-a) | Nếu bạn tự viết consumer thuần mà không xử lý, poll ném RecordDeserializationException (có offset() của record hỏng) và, nếu không bỏ qua record đó, lần poll kế tiếp lặp lại lỗi tại cùng offset, lag tăng mãi (đây là suy luận, docs không mô tả cách bỏ qua). … | retry có giới hạn, rồi DLT, rồi cảnh báo theo độ sâu DLT. |
| [(b) Lỗi tạm thời và lỗi vĩnh viễn cần cách xử lý khác nhau](../lessons/0006-error-handling-dlt.md#uc-b) | Dịch vụ phụ trợ chết 2 giây (tạm thời) khác với dữ liệu sai định dạng (vĩnh viễn). Retry 3 lần × 300ms chữa được loại đầu; loại sau nên thành "không retry" để khỏi lãng phí. | lỗi tạm thời ngắn thì retry có back-off; lỗi vĩnh viễn thì không retry, đưa thẳng sang DLT; lỗi tạm thời kéo dài thì cân nhắc @RetryableTopic nếu không cần thứ tự theo key. |
| [(c) DLT không phải thùng rác](../lessons/0006-error-handling-dlt.md#uc-c) | Có DLT mà không ai nhìn nó là mất message có tên đẹp. Cần: cảnh báo khi DLT có record, quy trình "replay" (đưa record về topic gốc sau khi sửa lỗi), và giữ retention của DLT đủ dài. | cảnh báo khi DLT có record, có quy trình replay về topic gốc, và giữ retention của DLT đủ dài. |
| [(d) DLT cùng số partition: bẫy khi tăng partition](../lessons/0006-error-handling-dlt.md#uc-d) | Bài 2 đã nói thêm partition làm đổi mapping key. Hệ quả mới ở đây: topic gốc tăng partition mà quên tăng DLT thì recoverer không gửi được record có partition vượt (nó kiểm tra và đặt partition là null theo docs). | tăng partition của DLT cùng lúc với topic gốc, hoặc chủ động để Kafka tự chọn partition của DLT. |
| [(e) Nếu là ngân hàng](../lessons/0006-error-handling-dlt.md#uc-e) | Cấu hình tham chiếu cho sự kiện tiền. | Mặc định "10 lần rồi bỏ" là không thể chấp nhận cho sự kiện tiền. Cấu hình tối thiểu: DefaultErrorHandler + DLT có min.insync.replicas=2 + cảnh báo + quy trình replay, và listener idempotent (Bài 7) vì retry nghĩa là giao nhiều lần. |

## Bài 7: Idempotent consumer, outbox, transaction

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Đặt vé: ghi DB rồi gửi Kafka (dual write)](../lessons/0007-idempotent-consumer-transactions.md#uc-a) | Dịch vụ lưu đơn vào DB rồi send sự kiện. Crash giữa hai việc: DB có đơn, Kafka không có sự kiện (mất), hoặc ngược lại nếu bạn gửi trước. | outbox: ghi sự kiện vào bảng outbox trong cùng transaction DB với dữ liệu, relay gửi lên Kafka; consumer vẫn phải idempotent. |
| [(b) Kho + đơn hàng: transaction chỉ phủ trong Kafka](../lessons/0007-idempotent-consumer-transactions.md#uc-b) | Consume "đơn mới" → produce "đã giữ hàng" trong một transaction Kafka là exactly-once trong Kafka. Nhưng bước "trừ tồn kho trong DB" ở giữa nằm ngoài. | at-least-once cộng idempotent consumer cho bước ghi DB; transaction Kafka chỉ dùng cho luồng topic → topic. |
| [(c) Producer chết giữa transaction: consumer đứng yên](../lessons/0007-idempotent-consumer-transactions.md#uc-c) | Producer transactional bị kill -9 khi transaction đang mở. Consumer read_committed không đọc qua LSO cho tới khi coordinator đóng transaction (sau transaction.timeout.ms, mặc định 60 giây). | hạ transaction.timeout.ms nếu nghiệp vụ không chờ được lâu, nhưng không thấp hơn thời gian một transaction thật. |
| [(d) transactional.id trùng giữa hai instance](../lessons/0007-idempotent-consumer-transactions.md#uc-d) | Hai pod cùng prefix: producer nào khởi tạo sau sẽ "fence" producer trước (ProducerFencedException, lỗi fatal chỉ còn close()). Hệ quả vận hành: prefix cần khác nhau mỗi instance, nhưng ổn định qua restart nếu muốn giữ tính chống trùng qua restart. | prefix transactional.id khác nhau mỗi instance và ổn định qua restart (ví dụ tên pod của StatefulSet). |
| [(e) Nếu là ngân hàng](../lessons/0007-idempotent-consumer-transactions.md#uc-e) | Cấu hình tham chiếu cho sự kiện tiền. | "At-least-once + idempotent consumer với UNIQUE(eventId) trong cùng transaction DB, kèm outbox ở phía phát" là thiết kế mặc định cho sự kiện tiền. Transaction Kafka chỉ thêm khi có luồng topic → topic thật sự. |

## Bài 8b: Quá tải, thông lượng, độ trễ

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Chứng khoán: một mã vốn hoá lớn làm nghẽn bảng giá](../lessons/0014-throughput-latency.md#uc-a) | Topic giá dùng mã chứng khoán làm key để giữ thứ tự tick theo mã. Phiên có tin tức, một mã chiếm phần lớn tick, nên một partition và một consumer gánh gần hết tải; thêm consumer không giúp vì mỗi partition chỉ một consumer trong group đọc. | consumer hiển thị giá tụt quá ngưỡng thì seekToEnd để nhảy tới giá mới nhất (tick cũ không còn giá trị); bật nén để tăng thông lượng; muốn chia tải của mã nóng thì phải tách key (ví dụ mã + loại dữ liệu), đổi lại mất … |
| [(b) Chứng khoán: mở cửa phiên, luồng lệnh tăng gấp nhiều lần](../lessons/0014-throughput-latency.md#uc-b) | Lúc mở cửa, lệnh đặt dồn trong vài phút; consumer chuyển lệnh sang hệ thống khớp lệnh không theo kịp, lag tăng. Có người đề xuất hạ acks=1 "cho nhanh". | giữ acks=all và idempotence; tăng thông lượng bằng gom lô và nén; cảnh báo theo thời gian để đọc hết (EstimatedMaxTimeLag trên MSK) thay vì số offset; khi hệ thống khớp lệnh chậm thì pause() container thay vì để … |
| [(c) Ngân hàng: API chuyển tiền treo khi broker bị throttle](../lessons/0014-throughput-latency.md#uc-c) | API chuyển tiền gọi KafkaTemplate.send() trên thread HTTP. Một ngày cụm managed chạm trần quota, broker làm chậm producer chứ không báo lỗi; bộ đệm 32 MB đầy dần, rồi mỗi send() chặn thread tới 60 giây. | hạ max.block.ms xuống mức API chịu được; quyết định trước khi Kafka không nhận thì trả 503 hay ghi outbox (Bài 7); theo dõi bufferpool-wait-ratio và produce-throttle-time-avg để thấy throttle trước khi API treo. |
| [(d) Nhiều team chung cụm: một team đọc lại cả tuần làm chậm mọi người](../lessons/0014-throughput-latency.md#uc-d) | Team báo cáo reset offset để đọc lại dữ liệu cả tuần. Dữ liệu cũ không còn trong page cache nên broker phải đọc đĩa, và group đó kéo hết băng thông; consumer của team khác bắt đầu có lag dù tải của họ không đổi. | đặt consumer_byte_rate cho principal của team đọc lại (quota tính theo từng broker), theo dõi throttle-time để thấy quota có tác dụng; trên MSK, việc đặt quota thường do platform team giữ quyền. |

## Bài 8c: Đọc lại, audit, giữ dữ liệu

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Ngân hàng: bug tính phí chạy 3 ngày, dựng lại số dư](../lessons/0015-replay-retention.md#uc-a) | Phát hiện service tính phí sai từ 00:00 ngày 14/11. Topic account-transactions (key = số tài khoản, retention 30 ngày) vẫn còn đủ dữ liệu. | dừng group, --reset-offsets --to-datetime 2026-11-14T00:00:00.000+07:00 --dry-run, kiểm tra offset mới, rồi --execute; consumer phải idempotent theo eventId (Bài 7) để đọc lại không trừ phí hai lần. |
| [(b) Ngân hàng: nạp lại lịch sử 2 năm, dữ liệu biến mất sau vài phút](../lessons/0015-replay-retention.md#uc-b) | Team migrate nạp giao dịch 2 năm trước vào một topic retention 7 ngày, giữ nguyên CreateTime gốc của từng giao dịch. Mọi segment có timestamp lớn nhất đã quá 7 ngày, nên ngay khi segment đóng và broker kiểm tra (mỗi 5 phút), cả segment bị xoá. | đặt message.timestamp.type=LogAppendTime cho topic nạp lại (timestamp là giờ ghi, giờ nghiệp vụ để trong payload), hoặc đặt retention.ms phù hợp với dữ liệu lịch sử. |
| [(c) Ngân hàng: tài khoản đã đóng vẫn hiện trong cache](../lessons/0015-replay-retention.md#uc-c) | Topic account-balance (compact) ghi tombstone khi tài khoản đóng. Một service dựng cache từ topic này chạy chậm hơn 1 ngày (ví dụ chỉ chạy mỗi tuần): tombstone đã bị dọn sau delete.retention.ms, nên service có thể không bao giờ thấy lệnh xoá, và cache còn giữ … | tăng delete.retention.ms của topic lớn hơn khoảng thời gian chậm nhất của consumer, hoặc cho service dựng cache đọc lại từ đầu mỗi lần khởi động (seekToBeginning). |
| [(d) Chứng khoán: chạy lại phiên 14/11 nhưng bắt đầu nhầm 7 tiếng](../lessons/0015-replay-retention.md#uc-d) | Team quant muốn chạy lại thuật toán trên dữ liệu phiên 14/11 (09:00 tới 15:00 giờ Việt Nam). Lệnh reset ghi --to-datetime 2026-11-14T09:00:00.000, không có múi giờ: công cụ hiểu là 09:00 UTC, tức 16:00 giờ Việt Nam, nên bỏ qua cả phiên. | luôn ghi +07:00 và chạy --dry-run trước; dừng ở 15:00 bằng offsetsForTimes (tìm offset của mốc 15:00 ở từng partition) hoặc logic ứng dụng dừng khi gặp timestamp sau 15:00, vì CLI không có tuỳ chọn "dừng ở thời điểm". |
| [(e) Audit: "Kafka là bất biến, không ai sửa được"](../lessons/0015-replay-retention.md#uc-e) | Trong họp, có người đề xuất dùng Kafka làm audit trail vì "log bất biến". Đúng là Kafka chỉ ghi thêm, không có API sửa record, offset không đổi. | chỉ cấp quyền Delete trên Topic cho vai trò quản trị; giám sát thay đổi cleanup.policy và retention.ms; dùng LogAppendTime nếu cần giờ ghi do broker đặt; retention dài theo chính sách lưu trữ của ngân hàng (hỏi platform … |

## Bài 8d: Hợp đồng sự kiện (schema)

| Usecase | Chuyện gì xảy ra | Cách xử lý |
|---|---|---|
| [(a) Ngân hàng: thêm kênh giao dịch vào sự kiện thanh toán](../lessons/0016-schema-evolution.md#uc-a) | Team Core Banking phát PaymentCompleted cho năm team khác. Nghiệp vụ cần thêm channel (mobile, web, quầy). | thêm channel là field tuỳ chọn ["null","string"] với "default": null: an toàn ở mọi mode, consumer cũ bỏ qua field mới, consumer mới đọc message cũ với giá trị null. |
| [(b) Ngân hàng: bỏ một field "không ai dùng" làm vỡ team khác](../lessons/0016-schema-evolution.md#uc-b) | Team Core muốn bỏ branchCode (đang bắt buộc) vì nghĩ không ai dùng. BACKWARD cho phép thay đổi này, nên registry nhận. | với BACKWARD, nâng mọi consumer trước rồi mới nâng producer; trước khi bỏ field, hỏi các team đang đọc subject đó, hoặc dùng FULL để registry chặn cả chiều này. |
| [(c) Ngân hàng: nhiều loại sự kiện tài khoản trong một topic](../lessons/0016-schema-evolution.md#uc-c) | AccountOpened, Deposited, Withdrawn, AccountClosed phải cùng một topic, cùng key là số tài khoản để giữ thứ tự (Bài 2). Với TopicNameStrategy mặc định, subject accounts-value chỉ chứa một chuỗi version; đăng ký loại record thứ hai sẽ bị coi là "thay đổi" của … | dùng TopicRecordNameStrategy (mỗi loại record một subject trong phạm vi topic), hoặc schema references; tránh RecordNameStrategy nếu tên record có thể trùng giữa các topic, vì nó kiểm tra trên mọi topic. |
| [(d) Chứng khoán: đổi kiểu giá từ số nguyên sang số thập phân](../lessons/0016-schema-evolution.md#uc-d) | Topic lệnh chứng khoán đang có price kiểu long (đơn vị đồng). Sàn chuyển sang giá có phần lẻ, team muốn đổi sang kiểu khác. | thêm field mới tuỳ chọn có default cho giá kiểu mới và giữ field cũ trong giai đoạn chuyển; nếu bắt buộc đổi hẳn thì làm theo docs: "create a brand-new topic" và chuyển consumer sang topic mới. |
| [(e) Consumer đọc topic Avro bằng deserializer chuỗi](../lessons/0016-schema-evolution.md#uc-e) | Một team viết consumer nhanh bằng StringDeserializer để "xem thử" topic thanh toán. Mỗi message có 5 byte đầu là magic và schema id, rồi tới dữ liệu Avro nhị phân: chuỗi đọc ra là rác. | mọi consumer của topic dùng deserializer cùng hệ registry, bọc bằng ErrorHandlingDeserializer để lỗi giải mã đi thẳng vào DLT (Bài 6) thay vì chặn vòng poll(). |
