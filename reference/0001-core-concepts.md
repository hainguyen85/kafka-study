<!-- Tự sinh từ 0001-core-concepts.html bằng scripts/html2md.py. Đừng sửa tay. -->

[Kafka](../README.md) / Bảng tra · từ [Bài 1](../lessons/0001-kafka-big-picture.md) và [Bài 2](../lessons/0002-key-partition-offset.md)

# Bảng tra: bức tranh tổng thể và khái niệm

Một trang, in được. Nguồn: [Kafka intro](https://kafka.apache.org/intro), [Kafka design](https://kafka.apache.org/44/design/design/), [KRaft overview](https://kafka.apache.org/44/operations/kraft/).

## Bức tranh một dòng

Producer ghi event vào topic; topic chia thành partition rải trên các broker (mỗi partition có leader + follower); nhiều consumer group đọc độc lập, mỗi group chia nhau các partition; controller (KRaft) giữ metadata cụm. Event không bị xoá khi đã đọc, chỉ hết hạn theo retention.

## Thành phần

| Thành phần | Vai trò |
|---|---|
| **Producer** | Client ghi event vào Kafka; không phải chờ consumer. |
| **Broker / cluster** | Server lưu trữ; nhiều broker hợp thành cluster. |
| **Leader / follower** | Mỗi partition: leader nhận ghi, follower sao chép từ leader. |
| **ISR** | Tập replica đang bắt kịp leader; ghi được coi là committed khi mọi thành viên ISR đã nhận. |
| **Consumer / consumer group** | Client kéo (pull) event; trong một group, mỗi partition do đúng một consumer đọc; group khác nhau đọc độc lập. |
| **Controller (KRaft)** | Quorum (thường 3 hoặc 5) giữ metadata cụm; `process.roles` = broker, controller, hoặc cả hai (combined, không khuyến nghị cho môi trường quan trọng). |
| **Retention** | Thời gian giữ event; event không bị xoá khi đã đọc. |

## Khái niệm dữ liệu

| Thuật ngữ | Nghĩa ngắn gọn | Nhớ gì khi thiết kế |
|---|---|---|
| **Event / message** | Một sự việc đã xảy ra (key, value, thời gian). | Là "sự thật đã xảy ra", nên đặt tên ở thì quá khứ: `OrderPlaced`. |
| **Topic** | Nhóm các event cùng loại, giống thư mục chứa file. | Chia thành partition. |
| **Partition** | Một "ngăn" của topic, chỉ ghi thêm vào cuối, nằm trên một broker (có bản sao ở broker khác). | Thứ tự chỉ đảm bảo *trong* partition. |
| **Key** | Giá trị đi kèm message; cùng key thì cùng partition. | Chọn mã đối tượng cần giữ thứ tự (tài xế, đơn hàng, khách hàng, thiết bị). |
| **Offset** | Số thứ tự của message trong một partition. | Hai partition đều có offset 0; offset không so sánh được giữa partition. |
| **Position** | Offset của message kế tiếp consumer sẽ đọc, mỗi partition một số. | Lưu rẻ; có thể tua lại để đọc lại dữ liệu. |
| **Replication factor** | Số bản sao của mỗi partition. | Production thường dùng 3 (nguồn: intro). |

## Bẫy đã gặp trong lessons

- **Key sai** (mã cuốc xe thay vì mã tài xế) → sự kiện của cùng đối tượng rơi vào nhiều partition → xử lý sai thứ tự mà không có lỗi nào báo.
- **Tăng số partition** → key có thể đổi partition → event cũ và mới của cùng đối tượng mất thứ tự chung.
- **Key "nóng"** (một shop chiếm 70% đơn khi flash sale) → dồn vào một partition → điểm nghẽn; thêm consumer không giúp.
