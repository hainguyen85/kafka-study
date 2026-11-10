<!-- Tự sinh từ 0001-kafka-big-picture.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bài 1 · [Bảng tra: bức tranh & khái niệm](../reference/0001-core-concepts.md)

# Bài 1: Kafka là gì và nó giải quyết vấn đề gì?

Bức tranh toàn cảnh trước, từng bộ phận sau.

> **Liên quan tới mission** Mục tiêu của bạn là hiểu *cách cấu hình Kafka để giải quyết từng loại vấn đề*, rồi áp vào bất kỳ lĩnh vực nào (kể cả ngân hàng sau này). Các bài dùng usecase từ nhiều lĩnh vực. Trước khi bàn key, acks hay rebalance, bạn cần một bản đồ trong đầu: Kafka giải quyết vấn đề gì, gồm những thành phần nào, và một event đi qua nó ra sao. Mọi bài sau sẽ treo vào bản đồ này.

**Thắng lợi của bài này:** sau ~15 phút bạn kể lại được, không nhìn tài liệu, hành trình của một event đơn hàng từ service gửi tới các service nhận, gọi đúng tên 5 thành phần, và giải thích Kafka khác "gọi API trực tiếp" ở đâu.

## 1. Vấn đề mà Kafka sinh ra để giải quyết

Kịch bản minh hoạ (do mình dựng): một cửa hàng online. Khi khách đặt hàng xong, service **Order** cần báo cho **Notification** (gửi email xác nhận), **Inventory** (trừ kho) và **Analytics** (cập nhật thống kê doanh thu).

*Bên trái: thêm một service nhận thì phải sửa Order, và một service nhận chậm hay chết sẽ kéo theo Order. Bên phải: Kafka đứng giữa.*

Tài liệu chính thức mô tả đúng ý này: "producers and consumers are fully decoupled and agnostic of each other" và "producers never need to wait for consumers." [[Kafka intro]](https://kafka.apache.org/intro) Trang giới thiệu liệt kê nhiều lĩnh vực dùng Kafka; một mục trong đó là "To process payments and financial transactions in real-time, such as in stock exchanges, banks, and insurances."

## 2. Ý tưởng lõi: một cuốn nhật ký chỉ ghi thêm vào cuối

Đừng hình dung Kafka là hộp thư lấy thư ra là hết. Tài liệu thiết kế viết rằng nó "more akin to a database log than a traditional messaging system." [[Kafka design: Motivation]](https://kafka.apache.org/44/design/design/) Ba hệ quả bạn sẽ dùng mãi:

1. **Event không bị xoá khi đã đọc**: "events are not deleted after consumption". Bạn đặt thời gian giữ lại (retention). [[Kafka intro]](https://kafka.apache.org/intro) Nhờ vậy một service mới *có thể* đọc lại lịch sử, và một service lỗi có thể đọc lại sau khi sửa bug. (Lưu ý: "có thể" chứ không tự động. Consumer group mới mặc định bắt đầu từ cuối, vì `auto.offset.reset` mặc định là `latest`; muốn đọc từ đầu phải cấu hình khác. [[Consumer configs]](https://kafka.apache.org/44/configuration/consumer-configs/) Bài 5 sẽ làm rõ.)
2. **Consumer tự kéo (pull), không bị đẩy.** "A pull-based system has the nicer property that the consumer simply falls behind and catches up when it can." [[Kafka design: push vs pull]](https://kafka.apache.org/44/design/design/) Service nhận chậm không làm sập ai.
3. **Mỗi bên đọc theo tốc độ và vị trí của riêng mình.** Vị trí đó chỉ là một con số (offset), bài 2 sẽ đi sâu.

## 3. Năm thành phần trong bức tranh

*P0, P1, P2 là 3 partition của topic `orders`. Sơ đồ rút gọn: các consumer trong một group chia nhau các partition; chi tiết ở các bài sau.*

| Thành phần | Vai trò (một câu) | Nguồn |
|---|---|---|
| **Producer** | Ứng dụng client ghi (publish) event vào Kafka. | [intro](https://kafka.apache.org/intro) |
| **Broker** | Máy chủ thuộc tầng lưu trữ ("the storage layer, called the brokers"). Nhiều broker hợp thành cluster. | [intro](https://kafka.apache.org/intro) |
| **Topic & partition** | Topic chia thành partition rải trên các broker. Mỗi partition có một *leader* nhận ghi và các *follower* sao chép từ leader. | [design: replication](https://kafka.apache.org/44/design/design/) |
| **Consumer & group** | Ứng dụng client đọc (subscribe) event. Trong một group, mỗi partition do đúng một consumer đọc ("consumed by exactly one consumer within each subscribing consumer group"). Các group khác nhau đọc độc lập. | [design: consumer](https://kafka.apache.org/44/design/design/) |
| **Controller (KRaft)** | Nhóm server chạy đồng thuận (quorum) để giữ metadata của cụm, không cần hệ thống điều phối bên ngoài. Thường 3 hoặc 5 controller. | [KRaft overview](https://kafka.apache.org/44/operations/kraft/) |

Bạn chưa cần nhớ chi tiết từng ô: bảng này là "khung treo". Các bài sau điền dần nội dung vào đúng từng ô.

## 4. Hành trình của một đơn hàng

1. **Order** (producer) tạo event `OrderPlaced` và gửi vào topic `orders`.
2. Event được ghi vào **leader** của một partition; các **follower** sao chép lại. Tài liệu: một ghi được coi là committed khi mọi bản sao trong tập ISR đã nhận. [[design: replicated logs]](https://kafka.apache.org/44/design/design/)
3. Order **không chờ ai đọc**; nó làm việc tiếp.
4. Group **Notification** và group **Inventory** mỗi nhóm **tự kéo** event, mỗi nhóm nhận đủ mọi event, theo tốc độ của mình.
5. Event vẫn nằm đó sau khi đọc, cho tới hết thời gian retention. Thêm group **Analytics** vào sau cũng đọc được.
6. Nếu một broker chết, các server còn lại tiếp quản ("if any of its servers fails, the other servers will take over their work"). [[intro]](https://kafka.apache.org/intro)

## 5. Bản đồ: câu hỏi trong họp → bài sẽ học

| Bạn sẽ nghe | Khái niệm | Bài |
|---|---|---|
| "Sự kiện của cùng một đơn hàng sao bị lộn thứ tự?" | key, partition, offset | [Bài 2](0002-key-partition-offset.md) |
| "Dựng cụm thế nào? Broker chết thì sao?" | cluster, leader, KRaft | Bài 3 |
| "Broker chết thì có mất đơn hàng không?" | acks, ISR, min.insync.replicas | Bài 4 |
| "Sao khách nhận hai email xác nhận?" / "sao kho bị trừ hai lần?" | consumer group, commit offset, at-least-once | Bài 5 |
| "Một message lỗi làm kẹt cả hàng" | poison pill, retry, DLQ, rebalance | Bài 6 |

## 6. Tự nói thành lời (30 giây, rồi mới làm quiz)

Che màn hình hoặc nhìn ra chỗ khác. Nói to (hoặc viết ra giấy): *"Order gửi event, Kafka ... , Notification và Inventory ..."* Gọi đúng tên 5 thành phần. Khoảng trống bạn gặp chính là chỗ cần xem lại. Cố truy hồi trước khi nhìn lại luôn giúp nhớ lâu hơn là đọc lại.

## 7. Kiểm tra

**1. Service Order vừa ghi event OrderPlaced vào Kafka. Nó cần làm gì tiếp theo với Notification?**

- **A.** Chờ Notification đọc xong rồi mới tiếp tục
- **B.** Không cần chờ, hai bên độc lập nhau
- **C.** Gọi REST sang Notification để xác nhận lại
- **D.** Xoá event ngay khi Notification đọc xong hẳn

<details><summary>Đáp án</summary>

**B. Không cần chờ, hai bên độc lập nhau**

Producer và consumer được tách rời hoàn toàn; producer không phải chờ consumer.

</details>

**2. Notification và Inventory là hai consumer group khác nhau cùng đọc topic orders. Điều gì đúng?**

- **A.** Hai group chia nhau, mỗi message chỉ một group
- **B.** Group nào đọc trước thì xoá message khỏi topic
- **C.** Chỉ group đầu tiên đăng ký mới được đọc
- **D.** Mỗi group nhận đủ mọi message, độc lập nhau

<details><summary>Đáp án</summary>

**D. Mỗi group nhận đủ mọi message, độc lập nhau**

Event không bị xoá khi đọc. Mỗi group có vị trí đọc riêng và nhận đủ mọi event.

</details>

**3. Trong cùng một consumer group, một partition được bao nhiêu consumer đọc tại một thời điểm?**

- **A.** Chính xác một consumer trong group đó
- **B.** Tất cả consumer trong group cùng lúc
- **C.** Hai consumer, một chính một dự phòng
- **D.** Tuỳ broker, chọn ngẫu nhiên mỗi lần

<details><summary>Đáp án</summary>

**A. Chính xác một consumer trong group đó**

Tài liệu thiết kế: mỗi partition được đọc bởi đúng một consumer trong mỗi group đăng ký.

</details>

**4. Một event đã được mọi consumer đọc xong. Kafka làm gì với nó?**

- **A.** Xoá ngay để giải phóng dung lượng ổ đĩa
- **B.** Chuyển sang một topic khác để lưu trữ lại
- **C.** Giữ lại theo thời gian retention bạn cấu hình
- **D.** Đánh dấu đã xử lý rồi ẩn khỏi consumer

<details><summary>Đáp án</summary>

**C. Giữ lại theo thời gian retention bạn cấu hình**

Event không bị xoá sau khi đọc; nó được giữ theo thời gian retention.

</details>

**5. Producer ghi message của partition P0. Nó ghi vào đâu?**

- **A.** Broker nào đang ít việc nhất lúc gửi
- **B.** Chỉ broker đang giữ leader của partition đó
- **C.** Mọi broker có bản sao, ghi đồng thời
- **D.** Broker controller, sau đó nó chuyển cho follower

<details><summary>Đáp án</summary>

**B. Chỉ broker đang giữ leader của partition đó**

Producer chỉ ghi vào leader; follower sao chép từ leader.

</details>

**6. Controller trong KRaft giữ thông tin nào?**

- **A.** Nội dung các message của mọi topic
- **B.** Mật khẩu đăng nhập của các service
- **C.** Siêu dữ liệu (metadata) của cả cụm
- **D.** Danh sách đơn hàng của mọi khách

<details><summary>Đáp án</summary>

**C. Siêu dữ liệu (metadata) của cả cụm**

Controller tạo quorum để giữ metadata cụm. Nội dung message nằm ở các broker.

</details>

## Nguồn nên đọc

> **Chính:** [Apache Kafka: Introduction](https://kafka.apache.org/intro), đọc từ đầu đến hết mục Main Concepts and Terminology (khoảng 10 phút).
>
> **Bổ trợ:** [Confluent Developer: Apache Kafka 101](https://developer.confluent.io/courses/apache-kafka/events/), module Introduction (3 phút), Brokers (4), Replication (3). Sau khi làm xong bài này, mục Design trong [tài liệu thiết kế](https://kafka.apache.org/44/design/design/) sẽ dễ đọc hơn nhiều.

**Bài tiếp theo:** [Bài 2: key, partition, offset](0002-key-partition-offset.md), phóng to vào cách Kafka quyết định message nằm ở đâu và thứ tự được giữ tới đâu. Bảng tra: [bức tranh & khái niệm](../reference/0001-core-concepts.md).

> **Còn chỗ nào mơ hồ?** Hỏi thẳng giáo viên (agent) trong phiên này, ví dụ: "Kafka khác RabbitMQ chỗ nào?", "vì sao không dùng database làm hàng đợi?". Dán dòng kết quả quiz vào cuộc trò chuyện để bài sau điều chỉnh đúng mức.
