# Tóm tắt Bài 3: Dựng cụm 3 broker

[← Tóm tắt Bài 2](0002-key-partition-offset.md) · [Bài học](../lessons/0003-build-cluster.html) · [Tóm tắt Bài 4 →](0004-producer-durability.md)

*Tóm tắt cả bài bằng lời, khoảng 60 giây. Dùng để ôn nhanh và tra cứu.*

Cụm lab có 3 node chạy **combined mode**: mỗi node vừa là broker vừa là controller. Controller giữ metadata bằng **quorum**, cần đa số còn sống: 3 controller chịu được 1 lỗi. Combined mode tiện cho lab; tài liệu không khuyến nghị nó cho môi trường quan trọng.

Mỗi partition có một **leader** nhận ghi và các **follower** sao chép. **ISR** là tập replica đang bám kịp leader; follower tụt quá `replica.lag.time.max.ms` (30 giây) thì bị loại khỏi ISR, và chỉ thành viên ISR mới được làm leader. Kill một broker: các partition nó làm leader được chuyển leader sang broker khác, cụm chạy tiếp. Kill hai broker: còn một node sống nhưng mất đa số controller, cụm không bầu được leader nên coi như không dùng được.

Mặc định phía broker rất mỏng: `default.replication.factor=1`, `min.insync.replicas=1`, `num.partitions=1`, tự tạo topic bật sẵn. Vì vậy mình tạo topic có chủ đích, RF=3 và `min.insync.replicas=2`, và tắt auto-create để gõ sai tên không đẻ ra topic RF=1.

`bootstrap.servers` chỉ là cửa vào: client hỏi một broker để biết cả cụm, rồi nói chuyện thẳng với leader của từng partition theo địa chỉ broker quảng bá (`advertised.listeners`); vì vậy trong container dùng `kafka-1:19092`, từ Windows dùng `localhost:29092`. Topic đã tạo thì sửa cấu hình bằng `kafka-configs.sh --alter --add-config`, không cần xoá tạo lại.


## Hỏi nhanh, đáp gọn

- **Vì sao min.insync.replicas=2 mà không phải 3?** Đặt 2 thì mất một broker vẫn ghi được an toàn; đặt 3 thì mất một broker là ngừng ghi (thấy ở Bài 4).
- **Production có chạy combined mode không?** Tài liệu không khuyến nghị; ở nơi làm mới cụm do dịch vụ managed và devops lo, mình chỉ cần biết hệ quả.
