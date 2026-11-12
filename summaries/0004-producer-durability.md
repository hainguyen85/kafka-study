# Tóm tắt Bài 4: Producer và độ bền

[← Tóm tắt Bài 3](0003-build-cluster.md) · [Bài học](../lessons/0004-producer-durability.html) · [Tóm tắt Bài 4b →](0010-idempotent-producer.md)

*Tóm tắt cả bài bằng lời, khoảng 60-90 giây. Dùng để ôn nhanh và tra cứu.*

"Không mất" phía ghi là bốn núm vặn. Một, `acks=all`: leader chỉ ack khi mọi replica trong ISR đã nhận. Hai, `min.insync.replicas=2` đặt **ở topic**, với RF=3: nếu ISR nhỏ hơn 2, broker từ chối ghi bằng `NotEnoughReplicasException` thay vì ghi vào một bản sao rồi báo thành công. Ba, retry trong `delivery.timeout.ms`. Bốn, idempotence để retry không tạo bản trùng trong một session của producer. Client 4.x đã mặc định `acks=all` và idempotence; thứ mỏng là mặc định của topic, và đó là việc của người tạo topic.

Bẫy hay gặp: `acks=all` mà `min.insync.replicas=1` thì khi ISR co còn leader, một ack chỉ là một bản sao. `acks=1` thì leader chết trước khi follower kịp chép là mất message đã báo thành công. Và `unclean.leader.election.enable` phải là false, nếu không Kafka có thể bầu một replica thiếu dữ liệu làm leader; trên MSK Standard giá trị mặc định là true nên phải hỏi.

Đánh đổi: minISR=2 nghĩa là mất 2 trên 3 broker thì ngừng ghi, producer treo tới hết timeout: đổi sẵn sàng lấy bền. Cuối cùng, phía Spring: `KafkaTemplate.send` trả future; không đọc future thì lỗi chỉ nằm trong log và ứng dụng vẫn báo thành công cho người dùng.

## Hỏi nhanh, đáp gọn

- **Idempotence có chống được trùng khi service restart rồi gửi lại không?** Không. Nó chỉ phủ retry trong một session của producer; gửi lại ở tầng ứng dụng là record mới, phải chống trùng ở consumer (Bài 7).
- **Telemetry có cần cấu hình như vậy không?** Không nhất thiết. Nếu mất vài điểm chấp nhận được, có thể chọn minISR=1 hoặc acks=1 để đổi lấy sẵn sàng và độ trễ. Quyết định theo domain.
