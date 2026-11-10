# Học Apache Kafka

Thư mục học Apache Kafka: broker, topic, partition, producer, consumer, consumer group, và các phần quanh nó (Kafka Connect, Kafka Streams, Schema Registry…) nếu mission cần. Việc dạy do skill [`teach`](https://github.com/mattpocock/skills/tree/main/skills/productivity/teach) của bộ Matt Pocock đảm nhận: học qua nhiều buổi, thư mục này lưu tiến độ.

**Xem bài học ở đâu cũng được:** sau khi có bài đầu tiên, mở `INDEX.md`. Mỗi bài/bảng tra có hai bản song song: `.html` (đầy đủ, quiz tương tác, mở bằng trình duyệt) và `.md` (đọc ngay trên GitHub).

## Bắt đầu

Mở thư mục này trong Claude Code hoặc Antigravity, rồi gọi:

```text
/teach Tôi muốn học Kafka để <mục tiêu cụ thể: thiết kế hệ thống event-driven, vận hành cụm, phỏng vấn…>.
```

Buổi đầu, agent hỏi **vì sao** bạn học (mission) trước khi dạy gì. Trả lời càng cụ thể (dự án nào, vai trò gì, trong bao lâu), bài học càng sát.

Mỗi buổi sau chỉ cần `/teach` (hoặc `/teach <chủ đề muốn học hôm nay>`). Agent đọc các file dưới đây để biết bạn đang ở đâu và dạy tiếp.

## Agent tạo ra những gì

| File / thư mục | Vai trò |
|---|---|
| `MISSION.md` | Vì sao bạn học; mọi bài học bám theo đây |
| `RESOURCES.md` | Nguồn đáng tin: tài liệu chính thức kafka.apache.org, docs.confluent.io… |
| `lessons/NNNN-*.html` | Mỗi bài một chủ đề hẹp, có quiz hoặc bài thực hành, mở bằng trình duyệt |
| `reference/*.html` | Bảng tra nhanh (lệnh `kafka-topics.sh`, cấu hình producer/consumer, glossary…), in được |
| `learning-records/NNNN-*.md` | Điều bạn đã nắm, để buổi sau dạy đúng mức |
| `assets/` | Stylesheet và widget dùng chung cho các bài (quiz, checklist) |
| `NOTES.md` | Sở thích học của bạn, ghi chú của agent |

## Thực hành

Phần kỹ năng cần một cụm Kafka để chạy thử. Cách dựng cụm local (Docker Compose chế độ KRaft, hoặc chạy trên k8s đã học ở `study\k8s-argo`) sẽ do agent đề xuất trong bài học, dựa trên mission và máy của bạn.

## Skill đã cài

`teach` bản ngày 2026-10-31 (commit `4ae2317` của `mattpocock/skills`), chép từ `D:\Workspace\study\k8s-argo`. Có hai bản giống nhau:

- `.agents/skills/teach/` cho Antigravity;
- `.claude/skills/teach/` cho Claude Code.

Cập nhật skill: `npx skills@latest update -p -y` (cập nhật bản trong `.agents/skills/`), rồi chép đè sang `.claude/skills/teach/`.

`assets/` và `scripts/html2md.py` cũng chép từ `study\k8s-argo`, để bài học hai repo cùng giao diện và cùng cách sinh `.md`.

## Đồng bộ .html và .md

Sau khi thêm hoặc sửa bài trong `lessons/` hoặc `reference/`, chạy `python scripts/html2md.py` để sinh lại các file `.md` và `INDEX.md`. Không sửa `.md` bằng tay.
