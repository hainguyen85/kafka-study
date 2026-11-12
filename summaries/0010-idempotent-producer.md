# Tóm tắt Bài 4b: Idempotent producer

[← Tóm tắt Bài 4](0004-producer-durability.md) · [Bài học](../lessons/0010-idempotent-producer.html) · [Tóm tắt Bài 5 →](0005-consumer-offset-commit.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Muốn không mất thì producer phải gửi lại khi không nhận được ack, và nếu lần trước thật ra đã ghi thì log có hai bản. Idempotent producer chặn đúng loại trùng này: broker cấp cho mỗi producer một **producer ID**, mỗi batch mang một **sequence number** tăng dần; retry mang lại sequence cũ nên broker nhận ra và không ghi thêm. Cùng cơ chế đó giữ được **thứ tự**: batch có sequence nhảy cóc bị từ chối cho tới khi batch trước tới.

Nó **không** chặn trùng do **ứng dụng** gửi lại: restart là session mới, producer ID mới, nên gửi lại cùng nội dung là bản ghi mới với broker. Loại trùng này phải chống ở consumer bằng mã sự kiện.

Idempotence mặc định bật, nhưng cần ba điều kiện: `acks=all`, `retries` > 0, `max.in.flight.requests.per.connection` ≤ 5. Vi phạm mà không ghi tường minh thì nó **tắt âm thầm**; ghi tường minh `enable.idempotence=true` thì thành `ConfigException` lúc khởi động. Vì vậy trong Spring Boot mình ghi tường minh qua `spring.kafka.producer.properties[enable.idempotence]`, cùng `acks: all`, để ai đổi `acks` cho "nhanh" sẽ thấy lỗi ngay.

## Hỏi nhanh, đáp gọn

- **Bật idempotence là hết trùng?** Hết trùng do retry của producer trong một session. Không hết trùng do ứng dụng gửi lại hay do consumer xử lý lại.
- **Vì sao giới hạn in-flight là 5?** Mô tả config nói broker chỉ giữ thông tin 5 batch gần nhất của mỗi producer.
- **Liên quan gì tới transaction?** Đặt `transactional.id` thì idempotence được bật kèm, và ID đó có ý nghĩa qua nhiều session (Bài 7).
