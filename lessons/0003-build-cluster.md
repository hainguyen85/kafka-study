<!-- Tự sinh từ 0003-build-cluster.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / [Bài 2](0002-key-partition-offset.md) / Bài 3 · [Bảng tra](../reference/0001-core-concepts.md)

# Bài 3: Dựng cụm 3 broker và nhìn tận mắt leader, ISR

Đọc về leader và replica thì dễ quên. Kill một broker và thấy leader đổi thì nhớ lâu.

> **Liên quan tới mission** Mục tiêu của bạn là "chém" được về Kafka sau onboarding. Câu như "broker chết thì chuyện gì xảy ra?" gần như chắc chắn xuất hiện trong buổi họp. Sau bài này bạn trả lời được bằng thứ bạn đã thấy tận mắt, không phải bằng sách.

**Thắng lợi của bài này:** bạn có cụm 3 broker chạy trên máy, tạo được topic có RF=3, đọc được `--describe`, và giải thích được vì sao kill 1 broker thì cụm vẫn chạy còn kill 2 thì không.

## 1. Kiến thức tối thiểu (có nguồn)

1. **Mỗi node có thể vừa là broker vừa là controller.** `process.roles` nhận `broker`, `controller` hoặc cả hai (combined mode). Combined mode đơn giản cho lab nhưng "is not recommended in critical deployment environments." [[KRaft overview]](https://kafka.apache.org/44/operations/kraft/)
2. **Controller giữ metadata của cụm theo kiểu đồng thuận (quorum).** Cần đa số controller còn sống: "3 controllers tolerate 1 failure and 5 tolerate 2". Chịu N lỗi cần 2N+1 controller. [[KRaft overview]](https://kafka.apache.org/44/operations/kraft/)
3. **Mỗi partition có một leader và các follower.** Producer chỉ ghi vào leader. Một ghi được coi là committed khi mọi replica trong ISR đã nhận; chỉ thành viên của ISR mới được bầu làm leader. [[Design: replication]](https://kafka.apache.org/44/design/design/)
4. **Giá trị mặc định phía broker rất "mỏng":** `num.partitions=1`, `default.replication.factor=1`, `min.insync.replicas=1` và `auto.create.topics.enable=true`. Vì bạn là người tạo topic, bạn phải tự đặt những giá trị này. [[Broker configs]](https://kafka.apache.org/44/configuration/broker-configs/) [[Topic configs]](https://kafka.apache.org/44/configuration/topic-configs/)
5. **Hộp "đọc tutorial cũ thì coi chừng":** từ Kafka 4.0, "ZooKeeper mode has been removed". Tutorial dùng `--zookeeper` hay `zookeeper.connect` không còn áp dụng. [[Upgrade notes]](https://kafka.apache.org/44/getting-started/upgrade/)

## 2. Thực hành (mỗi bước có kết quả mong đợi)

Ghi lại điều bạn *thật sự thấy*. Các "mong đợi" dưới đây suy ra từ tài liệu; nếu bạn thấy khác, đó là thông tin quý, hãy báo lại cho giáo viên.

### Bước 1. Chạy wizard dựng cụm

Mở Ubuntu (WSL), vào thư mục repo (đường dẫn dạng `/mnt/d/Workspace/study/kafka`) và chạy:

```
bash scripts/wizard-03-docker-kafka.sh
```

Wizard dẫn 6 stage: kiểm tra systemd, cài Docker Engine, chạy docker không cần sudo, chạy cụm, tạo topic và kiểm tra, kết nối từ Windows. Nó hỏi mật khẩu sudo và xác nhận ở từng bước.

Mong đợi: cuối cùng wizard báo "Setup complete" và liệt kê những việc còn lại phải làm tay (nếu có).

Nếu wizard yêu cầu `wsl.exe --shutdown` (bật systemd lần đầu), làm theo rồi chạy lại. Script mới được kiểm tra cú pháp, chưa chạy thật trên máy bạn: nếu nó lỗi ở đâu, dán lỗi cho giáo viên.

Tôi đã chạy xong wizard

### Bước 2. Đọc `--describe`

```
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic lab-orders
```

Mong đợi: 3 dòng partition. Mỗi dòng có `Leader` là một trong 1,2,3; `Replicas` gồm 3 broker; `Isr` gồm 3 broker. Dòng đầu có `min.insync.replicas=2`.

Câu tự hỏi: leader của 3 partition có cùng một broker không? Vì sao điều đó (không) quan trọng?

Tôi đã đọc được Leader, Replicas, Isr

### Bước 3. Gửi và nhận message bằng dòng lệnh

Terminal A (consumer), terminal B (producer):

```
# A
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic lab-orders --from-beginning
# B (gõ vài dòng, mỗi dòng một message, Ctrl-C để thoát)
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic lab-orders
```

Mong đợi: dòng gõ ở B xuất hiện ở A. Dừng A rồi chạy lại với `--from-beginning`: các dòng cũ vẫn còn (event không bị xoá khi đã đọc, như Bài 1).

Tôi đã thấy message đi qua cụm

### Bước 4. Kill một broker, xem leader và ISR đổi

```
docker kill kafka-3
# chạy ngay, rồi chạy lại sau ~40 giây
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic lab-orders
```

Mong đợi: partition từng do broker 3 làm leader chuyển sang broker khác. Lần chạy đầu `Isr` có thể vẫn còn 3; sau khoảng 30 giây (`replica.lag.time.max.ms` mặc định 30000) broker 3 bị loại khỏi `Isr`. Producer ở terminal B vẫn gửi được. [[Broker configs]](https://kafka.apache.org/44/configuration/broker-configs/)

Tôi đã thấy leader đổi và ISR co lại

### Bước 5. Bật lại broker

```
docker start kafka-3
# chờ vài chục giây rồi describe lại
```

Mong đợi: `Isr` trở lại 3 broker. Leader có thể quay về broker ưu tiên (`auto.leader.rebalance.enable` mặc định bật, theo broker configs); ghi lại bạn có thấy không.

Tôi đã thấy ISR phục hồi

### Bước 6. Kill hai broker: cụm "sống" mà không dùng được

```
docker kill kafka-2 kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic lab-orders
# xong thì bật lại
docker start kafka-2 kafka-3
```

Mong đợi (suy ra từ tài liệu KRaft, chưa kiểm chứng): vì cả 3 node đều là controller, mất 2 node là mất đa số quorum, nên lệnh quản trị treo hoặc lỗi và cụm không bầu được leader mới. Ghi lại chính xác bạn thấy gì.

Tôi đã thử kill hai broker và ghi lại kết quả

## 3. Usecase oái oăm

### (a) Kho lạnh mất điện cả giá đỡ (IoT)

Hệ thống cảm biến kho lạnh chạy 3 node Kafka combined. Hai node cắm chung một ổ điện và cùng tắt. Còn đúng một node "sống", nhưng dashboard trống và không cảm biến nào ghi được. Lý do: mất đa số controller thì cụm không đổi được metadata, không bầu được leader. Sống một node không có nghĩa là cụm còn dùng được. Cách giảm đau (khái niệm): đặt các node ở các khu vực mất điện khác nhau, và tách controller khỏi broker ở môi trường quan trọng (tài liệu khuyến nghị tránh combined mode).

### (b) Topic "ma" do gõ sai tên (microservices dùng chung cụm)

Service A gửi vào `order-evnts` (gõ nhầm), service B đọc `order-events`. Không ai báo lỗi vì broker tự tạo `order-evnts` với 1 partition và RF=1 (giá trị mặc định ở mục 1). Message không mất, nhưng không ai đọc, và topic đó không chịu nổi việc broker chết. Cách xử lý: tắt `auto.create.topics.enable` và tạo topic có chủ đích (lab của bạn đã làm đúng việc này; Stage 5 của wizard có kiểm tra).

## 4. Tự nói thành lời

Không nhìn tài liệu, giải thích trong 30 giây: *"Cụm 3 node, một node chết thì ... vì ...; hai node chết thì ... vì ..."* Dùng đúng các từ: leader, follower, ISR, controller, quorum.

## 5. Kiểm tra

**1. Cụm lab có 3 node, mỗi node vừa là broker vừa là controller. Cụm chịu mất tối đa mấy node mà vẫn bầu được leader mới?**

- **A.** Một node, vì cần đa số controller sống
- **B.** Hai node, vì chỉ cần một controller sống
- **C.** Không node nào, mọi bản sao phải sống
- **D.** Ba node, vì dữ liệu đã sao lưu

<details><summary>Đáp án</summary>

**A. Một node, vì cần đa số controller sống**

Quorum cần đa số controller còn sống: 3 controller chịu được 1 lỗi (chịu N lỗi cần 2N+1 controller).

</details>

**2. Một tutorial bảo chạy kafka-topics.sh kèm --zookeeper. Với Kafka 4.4, điều nào đúng?**

- **A.** Vẫn chạy bình thường nếu cài thêm ZooKeeper
- **B.** Tutorial đã lỗi thời vì ZooKeeper bị gỡ
- **C.** Chỉ chạy được trên broker dùng combined mode
- **D.** Chạy được nhưng chậm hơn so với bootstrap

<details><summary>Đáp án</summary>

**B. Tutorial đã lỗi thời vì ZooKeeper bị gỡ**

Từ Kafka 4.0 chỉ còn chế độ KRaft; ZooKeeper bị gỡ. Dùng --bootstrap-server.

</details>

**3. Một follower bị kill. Khi nào broker loại nó khỏi ISR?**

- **A.** Ngay lập tức khi container dừng chạy
- **B.** Sau khi producer gửi thêm một message
- **C.** Sau khi nó tụt lại quá replica.lag.time.max.ms
- **D.** Khi controller khởi động lại toàn cụm

<details><summary>Đáp án</summary>

**C. Sau khi nó tụt lại quá replica.lag.time.max.ms**

Follower bị loại khỏi ISR khi tụt lại quá replica.lag.time.max.ms (mặc định 30000, tức 30 giây), không phải ngay lập tức.

</details>

**4. Producer gõ nhầm tên topic order-evnts; broker để auto.create.topics.enable mặc định. Chuyện gì xảy ra?**

- **A.** Producer báo lỗi vì topic không tồn tại
- **B.** Broker tự sửa lại tên về order-events đúng
- **C.** Message được chuyển thẳng vào topic dead letter
- **D.** Broker tạo topic mới với 1 partition, RF=1

<details><summary>Đáp án</summary>

**D. Broker tạo topic mới với 1 partition, RF=1**

Tự tạo topic bật sẵn; topic mới lấy num.partitions=1 và default.replication.factor=1 mặc định, nên lỗi gõ tên không ai thấy.

</details>

**5. Vì sao topic lab-orders (RF=3) đặt min.insync.replicas=2 chứ không đặt 3?**

- **A.** Vì Kafka không cho đặt giá trị bằng ba
- **B.** Để mất một broker vẫn ghi được an toàn
- **C.** Để mỗi message được ghi nhanh gần gấp đôi
- **D.** Vì controller cần đúng hai bản sao dữ liệu

<details><summary>Đáp án</summary>

**B. Để mất một broker vẫn ghi được an toàn**

Với acks=all, ghi bị từ chối khi ISR nhỏ hơn giá trị này. Đặt 2 cho phép mất 1 broker mà vẫn ghi được; đặt 3 thì mất 1 broker là ngừng ghi.

</details>

## Nguồn nên đọc

> **Chính:** [KRaft overview](https://kafka.apache.org/44/operations/kraft/) (khoảng 10 phút), và mục "Replication" trong [Design](https://kafka.apache.org/44/design/design/).
>
> **Bổ trợ:** [Confluent Developer: Kafka 101](https://developer.confluent.io/courses/apache-kafka/events/), module Brokers và Replication.
>
> **Mã nguồn bài này:** `labs/03-cluster/docker-compose.yml` (dựa trên [ví dụ chính thức](https://github.com/apache/kafka/tree/trunk/docker/examples)) và `scripts/wizard-03-docker-kafka.sh`.

**Bài tiếp theo:** [Bài 4, producer và độ bền](0004-producer-durability.md): mô phỏng để tự làm mất message, rồi chạy `labs/04-producer-durability` (Spring Kafka) trên cụm này.

> **Còn chỗ nào mơ hồ?** Hỏi thẳng giáo viên (agent), ví dụ "ISR khác replica ở chỗ nào?", "vì sao combined mode không khuyến nghị?". Nếu wizard lỗi, dán nguyên thông báo lỗi. Dán dòng kết quả quiz để bài sau điều chỉnh đúng mức.
