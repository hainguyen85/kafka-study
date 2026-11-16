# Tóm tắt Bài 3: Dựng cụm 3 broker

[← Tóm tắt Bài 2](0002-key-partition-offset.md) · [Bài học](../lessons/0003-build-cluster.html) · [Tóm tắt Bài 4 →](0004-producer-durability.md)

*Bài nói 1-2 phút để giải thích cho người khác. Cấu hình chi tiết và câu hỏi hay gặp nằm trong bài học.*

Cụm lab có ba node, mỗi node vừa là broker lưu dữ liệu vừa là controller giữ metadata. Controller hoạt động theo đa số: ba node chịu được mất một.

Mỗi partition có một leader nhận ghi và các follower sao chép. Những bản sao đang bám kịp leader gọi là ISR. Leader chết thì một thành viên ISR lên thay và cụm chạy tiếp. Mất hai trên ba node thì khác: còn một node sống nhưng mất đa số controller, cụm không bầu được leader, coi như dừng.

Điều quan trọng nhất: mặc định của broker rất mỏng, chỉ một bản sao, và gõ sai tên topic là broker tự tạo topic mới. Nên luôn tạo topic có chủ đích, ba bản sao, `min.insync.replicas=2`, và tắt tự tạo topic.

Client chỉ cần vài địa chỉ để vào cụm; sau đó nó tự tìm leader của từng partition.
