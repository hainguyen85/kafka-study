# Lab 03: cụm Kafka 3 broker (KRaft) bằng Docker

Trạng thái: **chưa chạy thử** (lúc viết máy chưa có Docker). Bài 3 sẽ dẫn bạn cài Docker Engine trong WSL2 rồi chạy file này. Khi chạy lần đầu, các bước "kiểm tra" bên dưới chính là bài kiểm tra file có đúng không; nếu lệch, báo lại để sửa.

## Cách nhanh: wizard

Trong cửa sổ Ubuntu (WSL), chạy `bash scripts/wizard-03-docker-kafka.sh` ở thư mục gốc repo (đường dẫn trong WSL dạng `/mnt/d/Workspace/study/kafka`). Wizard dẫn 6 bước: kiểm tra systemd, cài Docker Engine, chạy docker không cần sudo, chạy cụm, tạo topic và kiểm tra, kết nối từ Windows. Script mới kiểm tra cú pháp (`bash -n`), chưa chạy thật.

## Chạy tay

```bash
cd labs/03-cluster
docker compose up -d
docker compose ps          # 3 container kafka-1/2/3 ở trạng thái running
```

## Kiểm tra (kết quả mong đợi)

```bash
# Tạo topic: bạn là người tạo topic nên đặt RF và min.insync.replicas tường minh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --create --topic lab-orders --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --describe --topic lab-orders
# Mong đợi: 3 partition, mỗi partition có Leader, Replicas gồm 3 broker, Isr gồm 3 broker
# và Configs có min.insync.replicas=2
```

Kiểm tra tắt tự tạo topic: gửi vào topic chưa tồn tại phải **lỗi**, không tạo ra topic mới.

```bash
docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic khong-ton-tai <<< "thu"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list
# Mong đợi: danh sách không có "khong-ton-tai". Nếu có: biến KAFKA_AUTO_CREATE_TOPICS_ENABLE chưa có tác dụng.
```

## Dọn dẹp

```bash
docker compose down        # giữ lại hay không dữ liệu tuỳ thư mục log trong container
docker compose down -v     # xoá hẳn
```

## Từ ứng dụng trên Windows

`bootstrap.servers` = `localhost:29092,localhost:39092,localhost:49092`. Nếu không kết nối được từ Windows (WSL2 chưa chuyển cổng ra localhost), chạy ứng dụng trong WSL hoặc dùng IP của WSL; cách xử lý sẽ nằm trong bài 3.

## Nguồn

- Ví dụ compose chính thức: https://github.com/apache/kafka/tree/trunk/docker/examples/docker-compose-files/cluster/combined/plaintext
- Quickstart (image `apache/kafka:4.4.0`): https://kafka.apache.org/quickstart
- KRaft: https://kafka.apache.org/44/operations/kraft/
- Chưa kiểm chứng: image `apache/kafka` có ánh xạ `KAFKA_AUTO_CREATE_TOPICS_ENABLE` sang `auto.create.topics.enable` hay không (quy ước đặt tên biến theo mẫu của ví dụ chính thức, chưa có tài liệu xác nhận riêng). Bước kiểm tra phía trên sẽ cho biết.
