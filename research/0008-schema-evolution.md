# 0008 - Hợp đồng sự kiện / schema: Schema Registry, compatibility, Glue, Spring Kafka

Ngày: 2026-11-17. Phiên bản đối chiếu: Apache Kafka 4.4 (Javadoc), Confluent Platform docs "current" (bản 8.4 khi đọc) cho Schema Registry, AWS Glue Developer Guide (bản hiện hành ngày đọc), Spring for Apache Kafka 4.1.2, Spring Boot docs (messaging/kafka).

**Phạm vi:** Kafka tự làm gì với schema (không gì cả: chỉ byte + serializer); khái niệm Confluent Schema Registry (subject, version, schema ID, compatibility và biến thể `_TRANSITIVE`, default, subject name strategy); Avro / Protobuf / JSON Schema; wire format (schema ID nằm ở đâu); AWS Glue Schema Registry cho MSK (compatibility mode, cách client tích hợp); cấu hình Spring Kafka/Spring Boot với serializer có registry (chỉ những gì docs cho thấy); thay đổi an toàn và thay đổi phá vỡ.

**Quy ước:** link `[tên]` trỏ mục "Sources" cuối file. Câu trong ngoặc kép là trích nguyên văn (đọc HTML gốc qua `curl` + `pandoc`). **(suy ra)** = tôi tự ghép. Docs Confluent chỉ dùng cho Schema Registry (theo yêu cầu); thông tin Confluent Cloud lấy lại từ `research/0004`.

**Nguồn đã đọc:** [kafka-intro], [kafka-serializer], [kafka-design], [producer-cfg], [sr-index], [sr-concepts], [sr-evolution], [sr-serdes], [sr-avro], [sr-config], [sr-contracts], [glue-sr], [glue-works], [glue-serde], [glue-java], [glue-integrations], [glue-migration], [sk-serdes], [sk-errh], [boot-kafka]; báo cáo nội bộ `research/0002-spring-kafka.md`, `research/0004-confluent-cloud.md`.

---

## Tóm tắt

1. Apache Kafka **không biết schema**: event có "a key, value, timestamp, and optional metadata headers" ([kafka-intro]); `Serializer` là "An interface for converting objects to bytes" ([kafka-serializer]). Broker chỉ giải nén batch để kiểm tra cấu trúc batch ([kafka-design]). Hợp đồng dữ liệu nằm ngoài Kafka.
2. Confluent Schema Registry "lives outside of and separately from your Kafka brokers", lưu schema trong topic compact `_schemas` (một partition) ([sr-concepts]). Subject = phạm vi tiến hoá; mặc định `TopicNameStrategy` -> `<topic>-key`, `<topic>-value` ([sr-concepts], [sr-serdes]).
3. Compatibility mặc định **BACKWARD (không transitive)**: chỉ so với version liền trước ([sr-evolution]). Docs giải thích lý do chọn BACKWARD: để "rewind consumers to the beginning of the topic". BACKWARD nghĩa là **nâng cấp consumer trước, producer sau** ([sr-evolution]).
4. Wire format Confluent: byte 0 là magic `0`, byte 1-4 là schema ID (big-endian), Protobuf thêm message index, rồi tới dữ liệu ([sr-serdes]). Từ Confluent Platform 8.1.1 có lựa chọn đặt **GUID 16 byte trong header** (`__value_schema_id`) thay vì tiền tố payload ([sr-serdes]).
5. Serializer Confluent **mặc định tự đăng ký schema** (`auto.register.schemas=true`) ([sr-config]). Serializer AWS Glue thì ngược lại: `SCHEMA_AUTO_REGISTRATION_SETTING` "If not passed, uses "false"" ([glue-java]).
6. Glue có 8 mode: `NONE`, `DISABLED`, `BACKWARD`, `BACKWARD_ALL`, `FORWARD`, `FORWARD_ALL`, `FULL`, `FULL_ALL` (`_ALL` tương ứng `_TRANSITIVE` của Confluent, suy ra từ định nghĩa) ([glue-sr]). Serializer Glue mặc định `Compatibility.BACKWARD` khi tự tạo schema ([glue-java]). Glue miễn phí, xác thực bằng IAM ([glue-sr], [glue-works]).
7. Spring Kafka/Spring Boot **không có** trang nào về Schema Registry; cấu hình đi qua `spring.kafka.*.properties[...]` ([boot-kafka]). Lỗi deserialize (schema không tìm thấy, payload sai) phải bọc bằng `ErrorHandlingDeserializer`; `DefaultErrorHandler` coi `DeserializationException` là lỗi fatal, không retry ([sk-serdes], [sk-errh]).
8. Thay đổi an toàn nhất (theo bảng Avro/Protobuf): **thêm hoặc bỏ field optional** (Avro: union có `null`, kèm `default`) - hợp lệ ở cả BACKWARD, FORWARD, FULL ([sr-evolution]). Đổi kiểu (ví dụ Number -> String) là phá vỡ; docs khuyên tạo topic mới hoặc nâng cấp đồng loạt ([sr-evolution]). Docs Confluent đã đọc **không** có quy tắc cho "đổi tên field".

---

## 1. Kafka tự làm gì với schema

- Event: "Conceptually, an event has a key, value, timestamp, and optional metadata headers" ([kafka-intro]).
- `Serializer<T>`: "An interface for converting objects to bytes"; Kafka kèm sẵn `StringSerializer`, `ByteArraySerializer`, `LongSerializer`, `UUIDSerializer`... ([kafka-serializer]). Producer chọn qua `key.serializer`, `value.serializer` ([producer-cfg]).
- Broker: batch được giải nén "in order to validate it. For example, it validates that the number of records in the batch is same as what batch header states" ([kafka-design]). (suy ra) Broker Apache không đọc nội dung value, không kiểm tra schema.
- Kiểm tra schema ID ở broker là tính năng của **Confluent Server**: "A Confluent Enterprise license is required ... for broker-side Schema ID Validation on Confluent Server" ([sr-index]). Trên Confluent Cloud chỉ có ở Dedicated (theo `research/0004`). MSK: không thấy tính năng tương đương trong docs đã đọc.
- (suy ra) Không có registry thì "hợp đồng" chỉ là thoả thuận giữa các team. Docs Confluent mô tả đúng vấn đề: "if an upstream team can make arbitrary changes to the data format at their discretion, then it becomes very difficult to ensure that all downstream consumers will (continue to) be able to interpret the data. What's missing is a 'contract' for data between the producers and the consumers" ([sr-concepts]).

---

## 2. Confluent Schema Registry: khái niệm

### 2.1 Thành phần

| Khái niệm | Định nghĩa trong docs | Nguồn |
|---|---|---|
| Schema | "the structure definition itself, expressed in Avro, Protobuf, or JSON". Hai lần đăng ký cùng một định nghĩa "share the same schema ID, even when registered under different subjects". | [sr-concepts] |
| Subject | "a named scope for schema evolution. Each subject holds an ordered sequence of schema versions and has its own compatibility configuration." Mặc định = tên topic + `-key`/`-value`. | [sr-concepts] |
| Version | Vị trí trong subject. Đăng ký lại schema y hệt "doesn't create a new version". | [sr-concepts] |
| Schema ID | "the globally unique identifier for a schema definition. Producers embed the schema ID in each message, and consumers use the ID to fetch the schema". ID tăng đơn điệu, không nhất thiết liên tiếp. | [sr-concepts] |
| Lưu trữ | Topic `_schemas` (mặc định), "with a single partition, as a highly available write-ahead log", `cleanup.policy=compact`. | [sr-concepts] |
| Bản quyền | "Schema Registry is licensed under the Confluent Community License"; plugin bảo mật, schema ID validation phía broker, Schema Linking cần Enterprise. | [sr-index] |

### 2.2 Định dạng hỗ trợ

- Avro, Protobuf, JSON Schema; "Schema Registry supports multiple formats at the same time" (subject này Avro, subject kia Protobuf) ([sr-concepts]).
- Serializer Java: `io.confluent.kafka.serializers.KafkaAvroSerializer` (ví dụ đầy đủ trong [sr-avro]); Protobuf `KafkaProtobufSerializer`, JSON Schema `KafkaJsonSchemaSerializer` (tên lớp lấy từ `research/0004`).
- Avro cần schema cả khi đọc nên payload gọn: "metadata such as the field names don't have to be explicitly encoded in the data" ([sr-concepts]).
- Avro: `specific.avro.reader` mặc định false -> nhận `GenericRecord`; docs khuyên "Use specific records for production applications" ([sr-avro]). Code sinh bởi `avro-maven-plugin` thêm thuộc tính Java như `"avro.java.string":"String"` "that can prevent schema evolution"; đặt `avro.remove.java.properties=true` để bỏ ([sr-avro]).
- Repo Maven của Confluent: `https://packages.confluent.io/maven/` ([sr-contracts]). (Giải quyết mục "repo URL: chưa xác minh" của `research/0004`.)

### 2.3 Subject name strategy

| Strategy | Subject | Một topic nhiều loại record? | Nguồn |
|---|---|---|---|
| `TopicNameStrategy` (default) | `<topic>-key` / `<topic>-value` | "Not generally, because a new record type could break Schema Registry compatibility checks on the topic." | [sr-serdes] |
| `RecordNameStrategy` | tên đầy đủ của record (Avro: namespace + name; Protobuf: message name; JSON Schema: title) | Có; compatibility kiểm theo tên record **trên mọi topic** | [sr-serdes] |
| `TopicRecordNameStrategy` | `<topic>-<fully qualified record name>` | Có; kiểm theo từng topic | [sr-serdes] |

- Cấu hình: `key.subject.name.strategy`, `value.subject.name.strategy`; class đầy đủ có tiền tố `io.confluent.kafka.serializers.subject.` ([sr-serdes]).
- Lưu ý: "The subject name strategy configured on a topic in the broker for schema ID validation doesn't propagate to clients" ([sr-serdes]). Console/Control Center chỉ hiện subject theo `TopicNameStrategy` ([sr-serdes]).
- Lựa chọn khác để nhiều loại event trong một topic: schema references với `auto.register.schemas=false` + `use.latest.version=true` ([sr-serdes]).

### 2.4 Đăng ký schema từ client

| Config serializer | Default | Ý nghĩa | Nguồn |
|---|---|---|---|
| `auto.register.schemas` | `true` | Serializer tự đăng ký schema suy ra từ object. | [sr-config], [sr-serdes] |
| `use.latest.version` | `false` | Chỉ có tác dụng khi `auto.register.schemas=false`; dùng version mới nhất của subject thay vì schema suy ra từ object. Không ghi đè schema ID đã nằm trong message cũ. | [sr-config], [sr-serdes] |
| `latest.compatibility.strict` | `true` | Khi `use.latest.version=true`: kiểm version mới nhất có backward compatible với schema của object không, sai thì ném lỗi. | [sr-serdes] |
| `normalize.schemas` | (không ghi default trong trang đã đọc) | Chuẩn hoá để hai schema tương đương về ngữ nghĩa được coi là một. | [sr-serdes] |

- Khi gửi tới topic `t`, serializer Avro đăng ký schema dưới `t-key`, `t-value` "if the compatibility test passes" ([sr-avro]).
- (suy ra) Production nên tắt `auto.register.schemas` và đăng ký schema qua pipeline CI (Maven plugin `kafka-schema-registry-maven-plugin` có xuất hiện trong [sr-protobuf]), để producer không tự đẩy schema mới lên. Docs đã đọc không có câu "tắt ở production" -> xem Chưa kiểm chứng.

---

## 3. Compatibility

### 3.1 Định nghĩa (Confluent)

| Kiểu | So với | Ai đọc được gì | Thứ tự nâng cấp | Nguồn |
|---|---|---|---|---|
| `BACKWARD` (**default**) | version liền trước | consumer dùng schema X đọc được dữ liệu X và X-1 | "upgrade all consumers before you start producing new events" | [sr-evolution] |
| `BACKWARD_TRANSITIVE` | mọi version trước | consumer X đọc được X, X-1, X-2... | như trên | [sr-evolution] |
| `FORWARD` | version liền trước | dữ liệu viết bằng X đọc được bằng consumer X hoặc X-1 | nâng producer trước, và đảm bảo dữ liệu cũ không còn cho consumer | [sr-evolution] |
| `FORWARD_TRANSITIVE` | mọi version trước | dữ liệu X đọc được bằng consumer X, X-1, X-2... | như trên | [sr-evolution] |
| `FULL` | version liền trước | cả hai chiều giữa X và X-1 | producer và consumer nâng độc lập | [sr-evolution] |
| `FULL_TRANSITIVE` | mọi version trước | cả hai chiều với mọi version | như trên | [sr-evolution] |
| `NONE` | không kiểm | - | "you need to be cautious about when to upgrade clients" | [sr-evolution] |

- Lý do default là BACKWARD: "so that you can rewind consumers to the beginning of the topic. With FORWARD compatibility mode, you aren't guaranteed the ability to read old messages." ([sr-evolution]). (suy ra) Gắn trực tiếp với replay ở `research/0007`: muốn replay cả lịch sử bằng code mới thì cần `BACKWARD_TRANSITIVE` (hoặc `FULL_TRANSITIVE`), vì BACKWARD thường chỉ đảm bảo đọc được X-1.
- Transitive: "it checks compatibility of a new schema against all previously registered schemas; otherwise, it checks compatibility of a new schema only against the latest schema" ([sr-evolution]).
- Protobuf: "best practice for Protobuf is to use BACKWARD_TRANSITIVE, as adding new message types is not forward compatible" ([sr-evolution]).
- Đặt theo global hoặc theo subject qua REST `/config` và `/config/{subject}` ([sr-evolution]).
- Confluent Cloud: default cũng BACKWARD (theo `research/0004`).

### 3.2 Bảng thay đổi được phép (Avro và Protobuf, trích từ [sr-evolution])

| Thay đổi | Avro BW | Avro FW | Avro Full | Protobuf BW | Protobuf FW | Protobuf Full |
|---|---|---|---|---|---|---|
| Thêm field optional | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| Bỏ field optional | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| Thêm field required | | ✔ | | | | |
| Bỏ field required | ✔ | | | | | |
| Thêm nhánh union/oneof | ✔ | | | ✔ | | |
| Bỏ nhánh union/oneof | | ✔ | | | ✔ | |
| Nới kiểu scalar | ✔ | | | ✔ | ✔ | ✔ |
| Thu hẹp kiểu scalar | | ✔ | | ✔ | ✔ | ✔ |

- JSON Schema phức tạp hơn: phụ thuộc `compatibilityPolicy` (`LENIENT`/`STRICT`) và `additionalProperties` (open/closed); ví dụ với STRICT + closed model, "Add required field" và "Remove required field" đều không hợp lệ ở mọi kiểu ([sr-evolution]).

### 3.3 Quy tắc cụ thể cho Avro

- Thêm field mới phải có default để BACKWARD: ví dụ docs thêm `{"name": "favorite_color", "type": "string", "default": "green"}`; "Had the default value been omitted in the new field, the new schema would not be backward compatible" ([sr-evolution]).
- Field optional trong Avro = union có `null`: "it is common to put "null" first and use "default": null for fields that are intended to be optional for schema evolution"; Avro "requires that the default value conform to the first branch of the union" ([sr-evolution]).
- "In Avro and Protobuf, you can define fields with default values. In that case, adding or removing a field with a default value is a fully compatible change." ([sr-evolution]).
- Thay đổi không tương thích (ví dụ "modifying a field type from Number to String"): "upgrade all producers and consumers to the new schema version at the same time or, more likely, create a brand-new topic" ([sr-evolution]).
- Đổi tên field: **không có** trong các trang Confluent đã đọc. (suy ra) Về mặt kiểm tra, đổi tên tương đương bỏ field cũ + thêm field mới; chỉ an toàn khi cả hai đều optional/có default, và dữ liệu cũ sẽ mất giá trị ở field mới. Avro có cơ chế alias nhưng nằm ở spec Avro (không thuộc danh sách nguồn được phép) -> Chưa kiểm chứng.

### 3.4 Thay đổi phá vỡ có kiểm soát (Data Contracts, tính năng trả phí)

- Hai cách: "Inter-topic migrations" (topic mới, chuyển consumer, phải tương quan offset hai topic) và "Intra-topic migrations" (quy tắc chuyển đổi khi đọc) ([sr-contracts]).
- Migration rules (JSONata) theo "application major version" + `compatibilityGroup`; có từ Confluent Platform 7.4 ([sr-contracts]).
- "Schema rules are only available on Confluent Enterprise and Confluent Cloud with the Stream Governance Advanced package." ([sr-contracts]).

---

## 4. Wire format: schema ID nằm ở đâu

Theo [sr-serdes] (mục Wire format):

| Byte | Thành phần | Mô tả |
|---|---|---|
| 0 | Version byte (magic) | `0` khi dùng schema ID (mặc định) |
| 1-4 | Schema ID | 4 byte, "big-endian ordering" |
| 5-x | Message index | Chỉ Protobuf: chỉ message nào trong file `.proto`; mã hoá zigzag varint, `[0]` rút gọn thành một byte `0` |
| x+1... | Data | Avro binary / Protobuf / JSON |

- Áp dụng cho cả key và value ([sr-serdes]).
- Biến thể header (từ Confluent Platform 8.1.1): GUID 16 byte trong header `__key_schema_id` / `__value_schema_id`, payload chỉ còn dữ liệu; bật bằng `value.schema.id.serializer=io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer`. Deserializer 8.1.1+ "look for the schema GUID in the header first", bản cũ chỉ tìm ở tiền tố payload ([sr-serdes]).
- (suy ra) Hệ quả: consumer dùng `StringDeserializer`/`JsonDeserializer` thường đọc topic Avro của Confluent sẽ thấy 5 byte rác đầu payload; mọi consumer của topic phải dùng deserializer cùng hệ registry.
- Tra schema ID: `kafka-avro-console-consumer ... --property print.schema.ids=true` ([sr-serdes]).

---

## 5. AWS Glue Schema Registry (cho MSK)

### 5.1 Khái niệm

- Hỗ trợ "AVRO (v1.11.4)", JSON Schema (Draft-04/06/07), Protobuf proto2/proto3 "without support for extensions or groups", và "Java language support"; có auto-registration, IAM, nén ZLIB tuỳ chọn; "The Schema registry is serverless and free to use" ([glue-sr]).
- Registry = "a logical container of schemas" có ARN để phân quyền; có `default-registry` ([glue-sr]).
- Compatibility gắn với **schema** (không có khái niệm subject): "There are 8 compatibility modes: NONE, DISABLED, BACKWARD, BACKWARD_ALL, FORWARD, FORWARD_ALL, FULL, FULL_ALL" ([glue-sr]).
  - `DISABLED`: "prevents versioning for a particular schema. No new versions can be added." (Confluent không có mode này.)
  - `BACKWARD`: "This compatibility choice is recommended because it allows consumers to read both the current and the previous schema version."
  - `BACKWARD_ALL` / `FORWARD_ALL` / `FULL_ALL`: kiểm với **mọi** version trước.
- Checkpoint: version dùng làm mốc kiểm tra; mặc định là version đầu; sửa schema trên console "will change the checkpoint to the latest version by default" ([glue-sr]).
- Giới hạn cứng: 100 registry/region, 10000 schema version/region/account, schema tối đa 170 KB ([glue-sr]).

### 5.2 Luồng serialize/deserialize

Theo [glue-works]:
- Producer: nếu chưa có schema và không cấu hình tên, schema "is created with the same name as the topic name"; record không khớp schema đã đăng ký thì serializer ném exception và "the application will fail to deliver the record".
- Serializer "decorates each record with the schema version ID", tuỳ chọn nén, rồi gửi; cache schema mặc định 24 giờ.
- Consumer đọc schema version ID từ payload, lần đầu gọi registry rồi cache.
- Bảo mật: "The Schema Registry supports identity-based IAM policies"; schema mã hoá at rest bằng KMS.
- Wire format chi tiết của Glue **không** có trong docs AWS đã đọc; trang migration chỉ nói deserializer đọc "the magic byte that contains the Avro Schema ID for the AWS Glue Schema Registry schema" ([glue-migration]).

### 5.3 Client Java

Theo [glue-java]:
- Maven: `software.amazon.glue:schema-registry-serde:1.1.5`; Java 8+.
- Producer: `value.serializer=GlueSchemaRegistryKafkaSerializer`, `AWSSchemaRegistryConstants.AWS_REGION`, `DATA_FORMAT` (`JSON`/`AVRO`).
- Default quan trọng (ghi ngay trong code mẫu): `SCHEMA_AUTO_REGISTRATION_SETTING` "If not passed, uses "false""; `SCHEMA_NAME` mặc định = tên topic; `REGISTRY_NAME` mặc định `default-registry`; `CACHE_TIME_TO_LIVE_MILLIS` 86400000; `CACHE_SIZE` 200; `COMPATIBILITY_SETTING` "If not passed, uses Compatibility.BACKWARD"; `COMPRESSION_TYPE` mặc định không nén.
- Consumer: `value.deserializer=GlueSchemaRegistryKafkaDeserializer`, `AWS_REGION`, `AVRO_RECORD_TYPE` (Avro), tuỳ chọn `SECONDARY_DESERIALIZER` cho giai đoạn chuyển đổi.
- IAM tối thiểu: serializer cần `glue:GetSchemaByDefinition` (thêm `CreateSchema`, `RegisterSchemaVersion`, `PutSchemaVersionMetadata` nếu tự đăng ký); deserializer cần `glue:GetSchemaVersion` ([glue-serde]).
- Chuyển từ registry khác (ví dụ Confluent): consumer dùng `AWSKafkaAvroDeserializer` với `SECONDARY_DESERIALIZER=KafkaAvroDeserializer`; đổi consumer trước, producer sau; topic giữ vô hạn thì "you will not be able to retire the third-party schema registry" ([glue-migration]).
- Docs dùng hai tên lớp: `GlueSchemaRegistryKafkaSerializer`/`Deserializer` ([glue-java]) và `AWSKafkaAvroSerializer`/`Deserializer` ([glue-migration]). Quan hệ giữa hai bộ tên chưa kiểm.

---

## 6. Spring Kafka / Spring Boot với serializer có registry

- Docs Spring Kafka 4.1.2 (trang Serialization) chỉ mô tả serializer của Spring (`JacksonJsonSerializer`, `ToStringSerializer`, `ErrorHandlingDeserializer`...); **không** có ví dụ Schema Registry hay Avro ([sk-serdes]).
- Spring Boot: thuộc tính không có sẵn thì dùng `spring.kafka.properties[...]` (chung cho mọi client), `spring.kafka.producer.properties[...]`, `spring.kafka.consumer.properties[...]` ([boot-kafka]).
- (suy ra) Ghép Spring Boot [boot-kafka] với Confluent [sr-avro]:

```yaml
spring:
  kafka:
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        "[auto.register.schemas]": false
        "[use.latest.version]": true
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        "[spring.deserializer.value.delegate.class]": io.confluent.kafka.serializers.KafkaAvroDeserializer
        "[specific.avro.reader]": true
    properties:
      "[schema.registry.url]": https://<registry-host>
```
  Tên `spring.deserializer.value.delegate.class` lấy từ `research/0002` (đọc source `ErrorHandlingDeserializer`); docs [sk-serdes] gọi nó là hằng `ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS`. Với Glue, thay class và thêm các khoá `AWSSchemaRegistryConstants` (suy ra, chưa chạy thử).
- Lỗi deserialize: "When a deserializer fails to deserialize a message, Spring has no way to handle the problem, because it occurs before the poll() returns. To solve this problem, the ErrorHandlingDeserializer has been introduced." ([sk-serdes]). `DefaultErrorHandler` "retries all exceptions except for fatal ones (like DeserializationException, MessageConversionException, etc.)" ([sk-errh]). Gửi bản ghi lỗi vào DLT: value là `byte[]`, docs gợi ý `DelegatingByTypeSerializer` với `ByteArraySerializer` ([sk-serdes]).
- So sánh với JSON không registry của Spring: `JacksonJsonSerializer` mặc định thêm header kiểu (`ADD_TYPE_INFO_HEADERS` true); deserializer mặc định chỉ tin package `java.util, java.lang` (`TRUSTED_PACKAGES`) ([sk-serdes]). (suy ra) Header kiểu chứa tên lớp Java, ràng buộc consumer vào lớp của producer; không có kiểm tra tương thích khi deploy.

---

## 7. Thay đổi an toàn và phá vỡ (tổng hợp)

| Thay đổi | BACKWARD (default Confluent/Glue) | Ghi chú | Nguồn |
|---|---|---|---|
| Thêm field optional có default (Avro: `["null", T]`, `"default": null`) | An toàn (cả FORWARD, FULL) | Cách tiến hoá nên dạy đầu tiên | [sr-evolution], [glue-sr] |
| Thêm field required (không default) | **Bị từ chối** | Consumer mới không đọc được message cũ thiếu field | [sr-evolution], [glue-sr] |
| Bỏ field optional | An toàn | | [sr-evolution] |
| Bỏ field required | Được (BACKWARD) nhưng hỏng FORWARD/FULL | Consumer cũ còn cần field -> phải nâng consumer trước | [sr-evolution], [glue-sr] |
| Đổi kiểu (Number -> String) | Phá vỡ | Topic mới hoặc nâng cấp đồng loạt | [sr-evolution] |
| Đổi tên field | Không có trong docs đã đọc | (suy ra) = bỏ + thêm | - |
| Thêm message type mới (Protobuf) | Không forward compatible | Dùng `BACKWARD_TRANSITIVE` | [sr-evolution] |

---

## 8. Usecase gợi ý: sự kiện ngân hàng nhiều team

### U1. Team Core Banking phát `PaymentCompleted`, năm team khác tiêu thụ
- Topic `payments.completed`, `TopicNameStrategy` -> subject `payments.completed-value` ([sr-serdes]).
- Compatibility: dùng `BACKWARD_TRANSITIVE` (không phải default BACKWARD) vì các team có thể replay cả năm dữ liệu khi dựng lại báo cáo ([sr-evolution], gắn với `research/0007`).
- Thêm `channel` (mobile/web/branch): khai báo `["null","string"]`, `"default": null` -> đăng ký được; consumer cũ bỏ qua field này (Avro FORWARD cũng hợp lệ cho thay đổi này) ([sr-evolution]).
- Câu hỏi "oái oăm": team Core muốn bỏ field `branchCode` (đang required) vì "không ai dùng". BACKWARD cho phép, nhưng consumer cũ chưa nâng cấp sẽ vỡ khi đọc message mới ("there is no assurance that consumers using older schemas can read data produced using the new schema") ([sr-evolution]). Thứ tự đúng: các team consumer nâng trước, producer sau.

### U2. Một topic, nhiều loại event của cùng tài khoản (giữ thứ tự)
- `AccountOpened`, `Deposited`, `Withdrawn`, `AccountClosed` cùng key = số tài khoản để giữ thứ tự (bài partition/key).
- `TopicNameStrategy` "Not generally" chứa nhiều loại record ([sr-serdes]) -> chọn `TopicRecordNameStrategy` (kiểm theo topic + tên record) hoặc schema references với `auto.register.schemas=false`, `use.latest.version=true` ([sr-serdes]).
- Câu hỏi "oái oăm": chọn `RecordNameStrategy` thì compatibility kiểm theo tên record **trên mọi topic**: team khác dùng lại tên `com.bank.Deposited` ở topic khác sẽ bị ràng buộc tiến hoá chung ([sr-serdes]).

### U3. Ngân hàng chạy MSK, chưa chốt registry
- Glue: miễn phí, IAM, không subject; auto-register mặc định tắt; BACKWARD mặc định khi serializer tạo schema ([glue-sr], [glue-java]).
- Confluent Schema Registry tự host: Confluent Community License; tự vận hành ([sr-index]). Confluent Cloud: có sẵn trong Stream Governance (theo `research/0004`).
- Câu hỏi "oái oăm": hai team dùng hai registry khác nhau trên cùng topic -> consumer phải hiểu cả hai wire format; Glue có `SECONDARY_DESERIALIZER` cho đúng tình huống chuyển đổi này ([glue-migration]).
- Câu hỏi cho platform team: registry nào, ai có quyền đăng ký schema (IAM `glue:RegisterSchemaVersion` hay RBAC Confluent), compatibility mặc định của tổ chức, có cho auto-register trên production không.

---

## Điểm nên dạy (80/20)

1. Kafka chỉ lưu byte; schema và hợp đồng nằm ở serializer + registry bên ngoài broker.
2. Subject là đơn vị tiến hoá; mặc định `<topic>-value`; mỗi topic một loại record trừ khi đổi strategy.
3. Default compatibility là BACKWARD **không transitive**; BACKWARD = nâng consumer trước; muốn replay toàn lịch sử thì dùng `_TRANSITIVE`.
4. Luật vàng: thêm/bỏ field **optional có default** là an toàn ở mọi mode; thêm field required là phá vỡ BACKWARD; đổi kiểu là phá vỡ -> topic mới.
5. Wire format Confluent: magic byte + 4 byte schema ID trước payload; mọi consumer phải dùng deserializer cùng registry.
6. Confluent tự đăng ký schema mặc định (`auto.register.schemas=true`), Glue thì mặc định tắt; production nên đăng ký qua CI.
7. Glue cho MSK: 8 mode (`_ALL` = transitive, thêm `DISABLED`), gắn theo schema, xác thực IAM, miễn phí.
8. Trong Spring: cấu hình qua `spring.kafka.*.properties[...]`, bọc deserializer bằng `ErrorHandlingDeserializer`; lỗi deserialize là fatal, không retry.

## Chưa kiểm chứng / mâu thuẫn

1. Quy tắc đổi tên field (Avro aliases): không có trong trang Confluent đã đọc; spec Avro nằm ngoài danh sách nguồn được phép.
2. Khuyến nghị "tắt `auto.register.schemas` ở production": docs chỉ mô tả các trường hợp nên tắt (schema references, tránh "Schema not found"), không có câu khuyến nghị chung.
3. Wire format chi tiết của AWS Glue (vị trí, độ dài version ID): docs AWS chỉ nói "decorates each record with the schema version ID" và "magic byte".
4. Quan hệ giữa `GlueSchemaRegistryKafkaSerializer` và `AWSKafkaAvroSerializer`, và phiên bản `schema-registry-serde` mới nhất (docs ghi 1.1.5): chưa kiểm trên Maven Central.
5. Cấu hình YAML ở mục 6 chưa chạy thử với Spring Boot 4.1.2; tên khoá `spring.deserializer.value.delegate.class` lấy từ `research/0002`.
6. Mâu thuẫn trong [sr-evolution]: một chỗ ghi "For Kafka Streams, only BACKWARD compatibility is supported", chỗ khác ghi "For Kafka Streams only FULL, TRANSITIVE, and BACKWARD compatibility is supported". Cùng đoạn đó nói "For a plain consumer, it is safe to upgrade the consumer to the new schema after the producer is upgraded", ngược với mục "Order of upgrading clients" (BACKWARD: nâng consumer trước). Không ảnh hưởng bài học nếu theo mục "Order of upgrading clients".
7. Phần tóm tắt đầu trang [sr-evolution] ghi Backward = "add optional fields, remove fields", Forward = "remove optional fields, add fields", hơi khác bảng chi tiết (bảng cho phép bỏ field optional ở cả FW). Dạy theo bảng.
8. MSK có tính năng kiểm tra schema phía broker hay không: không thấy trong docs đã đọc.
9. Confluent Cloud default BACKWARD và tên lớp serializer Protobuf/JSON Schema: lấy từ `research/0004`, không đọc lại.

---

## Sources

- [kafka-intro] https://kafka.apache.org/44/getting-started/introduction/ - event = key, value, timestamp, headers
- [kafka-serializer] https://kafka.apache.org/44/javadoc/org/apache/kafka/common/serialization/Serializer.html - "An interface for converting objects to bytes", các serializer có sẵn
- [kafka-design] https://kafka.apache.org/44/design/design/ - broker giải nén batch để kiểm tra
- [producer-cfg] https://kafka.apache.org/44/configuration/producer-configs/ - `key.serializer`, `value.serializer`
- [sr-index] https://docs.confluent.io/platform/current/schema-registry/index.html - tổng quan, giấy phép (Community License, Enterprise cho schema ID validation)
- [sr-concepts] https://docs.confluent.io/platform/current/schema-registry/fundamentals/index.html - schema, subject, version, schema ID, `_schemas`, định dạng hỗ trợ
- [sr-evolution] https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html - kiểu compatibility, bảng thay đổi, default BACKWARD, thứ tự nâng cấp, Avro default/union
- [sr-serdes] https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html - subject name strategy, `auto.register.schemas`, `use.latest.version`, wire format, GUID header
- [sr-avro] https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html - `KafkaAvroSerializer`, `schema.registry.url`, `specific.avro.reader`, `avro.remove.java.properties`
- [sr-protobuf] https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html - Protobuf serdes, `kafka-schema-registry-maven-plugin`
- [sr-config] https://docs.confluent.io/platform/current/schema-registry/connect.html - Configuration Options: default `auto.register.schemas=true`, `use.latest.version=false`
- [sr-contracts] https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html - migration rules, giấy phép, repo Maven Confluent
- [glue-sr] https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html - định dạng, registry, 8 compatibility mode, quota
- [glue-works] https://docs.aws.amazon.com/glue/latest/dg/schema-registry-works.html - luồng serialize/deserialize, IAM, KMS
- [glue-serde] https://docs.aws.amazon.com/glue/latest/dg/schema-registry-gs-serde.html - IAM cho serializer/deserializer
- [glue-java] https://docs.aws.amazon.com/glue/latest/dg/schema-registry-gs-serde-java.html - Maven, thuộc tính producer/consumer và default
- [glue-integrations] https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html - tích hợp MSK/Apache Kafka
- [glue-migration] https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations-migration.html - chuyển từ registry khác, `SECONDARY_DESERIALIZER`
- [sk-serdes] https://docs.spring.io/spring-kafka/reference/kafka/serdes.html - JSON serializer của Spring, `ErrorHandlingDeserializer`
- [sk-errh] https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html - `DeserializationException` là fatal trong `DefaultErrorHandler`
- [boot-kafka] https://docs.spring.io/spring-boot/reference/messaging/kafka.html - Additional Kafka Properties (`spring.kafka.*.properties[...]`)
