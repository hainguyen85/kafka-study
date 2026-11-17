# Lab 08d: Spring Kafka + Schema Registry (Avro)

Dùng cho Bài 8d. Spring Boot 4.1.2, Spring Kafka 4.1.2, `io.confluent:kafka-avro-serializer` 8.3.2 (repo `https://packages.confluent.io/maven/`). Cần cụm `labs/03-cluster` và service `schema-registry` (compose profile `schema`, cổng host 18081).

**Trạng thái (2026-11-17):** đã chạy với cụm thật: v1, v2 gửi được; v3 bị registry từ chối (`READER_FIELD_MISSING_DEFAULT_VALUE`); consumer đọc lẫn message v1 và v2. Kết quả ghi trong Bài 8d.

## Chạy

```bash
# WSL: bật registry
cd /mnt/d/Workspace/study/kafka/labs/03-cluster && docker compose --profile schema up -d schema-registry

# Windows / Git Bash
export JAVA_HOME="C:/Program Files/Eclipse Adoptium/jdk-25.0.4.7-hotspot"   # JDK 17+
mvn -q -DskipTests package
J="java -jar target/schema-0.0.1-SNAPSHOT.jar"
$J --spring.profiles.active=produce --lab.schema=v1    # v1 | v2 | v3
$J --spring.profiles.active=consume
```

| File | Nội dung |
|---|---|
| `schemas/payment-v1.avsc` | Bản gốc: eventId, accountId, amount, currency |
| `schemas/payment-v2.avsc` | Thêm `channel` tuỳ chọn, default null: an toàn ở mọi mode |
| `schemas/payment-v3.avsc` | Thêm `branchCode` bắt buộc, không default: phá vỡ BACKWARD |
| `application.yml` | `schema.registry.url`, `KafkaAvroSerializer`, `ErrorHandlingDeserializer` bọc `KafkaAvroDeserializer` |
| `SchemaLabApplication.java` | Producer dựng `GenericRecord` từ file `.avsc`; consumer in field của writer schema |

Muốn làm lại từ đầu: xoá subject `curl -X DELETE http://localhost:18081/subjects/payments-avro-value` (xoá mềm) và xoá topic `payments-avro`.
