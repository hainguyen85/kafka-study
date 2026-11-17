# 0007 - Đọc lại, audit, giữ dữ liệu: retention, compaction, tiered storage, replay, timestamp

Ngày: 2026-11-17. Phiên bản đối chiếu: Apache Kafka 4.4 (docs `kafka.apache.org/44/`, source tag `4.4.0`), Spring for Apache Kafka 4.1.2, Amazon MSK Developer Guide (bản hiện hành ngày đọc).

**Phạm vi:** retention theo thời gian/kích thước và cách xoá theo segment; log compaction, tombstone, `delete.retention.ms`; tiered storage (KIP-405, trạng thái ở 4.x, MSK); replay bằng `kafka-consumer-groups.sh --reset-offsets`, `seek`/`offsetsForTimes`, và tương đương trong Spring Kafka; timestamp `CreateTime`/`LogAppendTime`; dùng Kafka làm event log/audit trail: docs đảm bảo gì và giới hạn ở đâu.

**Quy ước:** link `[tên]` trỏ mục "Sources" cuối file. Câu trong ngoặc kép là trích nguyên văn (đọc HTML gốc qua `curl` + `pandoc`). **(suy ra)** = tôi tự ghép. Default ghi kèm phiên bản.

**Nguồn đã đọc:** [topic-cfg], [broker-cfg], [consumer-cfg], [design], [impl-log], [msg-format], [ops-basic], [tiered], [tiered-cfg], [kip-405], [ts-ga], [upgrade], [intro], [uses], [consumer-javadoc], [producer-record], [admin-javadoc], [acl], [src-gor], [src-utils], [sk-seek], [sk-initial-offset], [sk-annot], [msk-tiered], [msk-tiered-retention]; báo cáo nội bộ `research/0003-msk.md`, `research/0004-confluent-cloud.md`.

---

## Tóm tắt

1. Kafka **không xoá message khi đã đọc**: "events are not deleted after consumption. Instead, you define for how long Kafka should retain your events through a per-topic configuration setting" ([intro]). Replay là tính năng chủ đích: rewind "violates the common contract of a queue, but turns out to be an essential feature for many consumers" ([design]).
2. Xoá **theo cả segment**, không theo từng record: "Data is deleted one log segment at a time"; với retention theo thời gian, "the largest timestamp in a segment file ... defining the retention time for the entire segment" ([impl-log]). Default 4.4: `retention.ms`=604800000 (7 ngày), `retention.bytes`=-1, `segment.bytes`=1 GiB, `segment.ms`=7 ngày, broker kiểm tra mỗi 5 phút (`log.retention.check.interval.ms`=300000) ([topic-cfg], [broker-cfg]).
3. Compaction đảm bảo giữ **ít nhất bản ghi cuối của mỗi key**, không đổi thứ tự, offset không đổi; tombstone (value `null`) bị dọn sau `delete.retention.ms` (default 1 ngày) nên consumer đọc lại từ đầu chậm hơn mức này "may miss delete markers" ([design], [topic-cfg]).
4. Tiered storage (KIP-405) "marked production-ready since Kafka 3.9" ([kip-405]); Apache **không kèm** sẵn plugin lưu trữ từ xa ("Kafka server doesn't provide out-of-the-box implementation of RemoteStorageManager"); không hỗ trợ topic compact ([tiered]). MSK có sẵn cho Standard brokers (cụm provisioned), tối thiểu 3 ngày ở tầng rẻ ([msk-tiered]).
5. Reset offset bằng CLI: một group mỗi lần, mặc định `--dry-run`, consumer phải **không active** ([ops-basic]). **Bẫy:** `--to-datetime` không ghi múi giờ thì source code tự thêm `Z`, tức hiểu là **UTC** ([src-utils]). Ở Việt Nam (UTC+7) phải ghi `2026-11-17T09:00:00.000+07:00`.
6. `offsetsForTimes` trả "the earliest offset whose timestamp is greater than or equal to the given timestamp" ([consumer-javadoc]); Spring Kafka có `seekToTimestamp` trong `ConsumerSeekCallback` (từ 2.3) và trong `AbstractConsumerSeekAware` (từ 2.6) ([sk-seek]).
7. Timestamp mặc định là `CreateTime` (producer gán, mặc định là giờ máy producer); `LogAppendTime` thì broker ghi đè bằng giờ broker ([producer-record], [topic-cfg]). Từ 4.0, `message.timestamp.after.max.ms` = 1 giờ: record có timestamp tương lai quá 1 giờ bị từ chối khi `CreateTime` ([upgrade]).
8. Làm audit log: docs đảm bảo thứ tự **trong một partition** ([intro]) và offset là định danh vĩnh viễn ([design]); docs **không** dùng chữ "immutable". Dữ liệu vẫn có thể mất theo retention, compaction, `deleteRecords` (cần quyền Delete trên Topic) hoặc xoá topic ([impl-log], [admin-javadoc], [acl]).

---

## 1. Retention: giữ bao lâu, xoá thế nào

### 1.1 Cấu hình (default Kafka 4.4)

| Config (topic / broker) | Default 4.4 | Ý nghĩa theo docs | Nguồn |
|---|---|---|---|
| `cleanup.policy` / `log.cleanup.policy` | `delete` | `delete` bỏ segment cũ; `compact` giữ bản cuối mỗi key; có thể ghép `delete,compact`. Từ 4.2: "An empty list means infinite retention". | [topic-cfg], [upgrade] |
| `retention.ms` / `log.retention.ms` (null -> `log.retention.minutes` -> `log.retention.hours`=168) | 604800000 (7 ngày) | "This represents an SLA on how soon consumers must read their data. If set to -1, no time limit is applied." Từ 4.x: "it triggers the rolling of new segment if the retention.ms condition is satisfied". | [topic-cfg], [broker-cfg] |
| `retention.bytes` / `log.retention.bytes` | -1 (không giới hạn) | Giới hạn **mỗi partition**: "multiply it by the number of partitions to compute the topic retention in bytes". | [topic-cfg] |
| `segment.bytes` / `log.segment.bytes` | 1073741824 (1 GiB); tối thiểu 1 MiB từ 4.0 | "Retention and cleaning is always done a file at a time so a larger segment size means fewer files but less granular control over retention." | [topic-cfg], [upgrade] |
| `segment.ms` / `log.roll.ms` (`log.roll.hours`=168) | 604800000 (7 ngày) | Ép roll segment theo thời gian "to ensure that retention can delete or compact old data". | [topic-cfg], [broker-cfg] |
| `log.retention.check.interval.ms` (broker) | 300000 (5 phút) | Tần suất kiểm tra log nào đủ điều kiện xoá. | [broker-cfg] |
| `file.delete.delay.ms` / `log.segment.delete.delay.ms` | 60000 | Chờ trước khi xoá file khỏi filesystem. | [topic-cfg] |
| `offsets.retention.minutes` (broker) | 10080 (7 ngày) | Offset đã commit của group bị xoá khi group rỗng quá thời gian này (và các trường hợp khác). | [broker-cfg] |

### 1.2 Cách xoá thực tế

- "Data is deleted one log segment at a time. The log manager applies two metrics to identify segments which are eligible for deletion: time and size. For time-based policies, the record timestamps are considered, with the largest timestamp in a segment file (order of records is not relevant) defining the retention time for the entire segment. Size-based retention is disabled by default." Nếu bật cả hai, segment đủ điều kiện theo một trong hai đều bị xoá ([impl-log], mục Deletes).
- (suy ra) Hệ quả cho bài học:
  - Dữ liệu có thể sống **lâu hơn** `retention.ms`: phải chờ segment roll (đầy 1 GiB hoặc 7 ngày), rồi chờ lần kiểm tra 5 phút, rồi `file.delete.delay.ms`. Đừng hứa với audit "xoá đúng ngày thứ 7".
  - Dữ liệu cũng có thể sống **ngắn hơn** mong đợi khi producer gán timestamp cũ (`CreateTime` cũ, ví dụ nạp lại dữ liệu lịch sử): largest timestamp của segment đã quá hạn nên segment bị xoá ngay sau khi roll. `message.timestamp.before.max.ms` default `Long.MAX` nên broker **không** từ chối timestamp cũ ([topic-cfg]).
- Khi consumer đứng ở offset đã bị xoá: "when the client attempts to consume a non-existent offset it is given an OutOfRangeException and can either reset itself or fail" ([impl-log]); hành vi reset theo `auto.offset.reset` (default `latest`; giá trị `earliest`, `latest`, `by_duration:<ISO8601>`, `none`) ([consumer-cfg]). (suy ra) Với ngân hàng, `latest` ở đây là **bỏ qua âm thầm** phần chưa đọc.
- Offset của group cũng hết hạn: group rỗng quá `offsets.retention.minutes` (7 ngày) thì mất offset, lần chạy sau đi theo `auto.offset.reset` ([broker-cfg], [consumer-cfg]).

---

## 2. Log compaction và tombstone

### 2.1 Dùng để làm gì

- "Log compaction ensures that Kafka will always retain at least the last known value for each message key within the log of data for a single topic partition." Dùng cho "restoring state after application crashes or system failure, or reloading caches" ([design], mục Log Compaction).
- Phù hợp với "the log of changes to keyed, mutable data (for example, the changes to a database table)" và các usecase database change subscription, event sourcing, journaling ([design]).

### 2.2 Đảm bảo (nguyên văn 4 điều của docs)

1. Consumer theo kịp phần head "will see every message that is written; these messages will have sequential offsets". `min.compaction.lag.ms` cho cận dưới thời gian một message còn chưa bị compact; `max.compaction.lag.ms` cho cận trên thời gian chờ trước khi đủ điều kiện compact.
2. "Ordering of messages is always maintained. Compaction will never re-order messages, just remove some."
3. "The offset for a message never changes. It is the permanent identifier for a position in the log."
4. Consumer đọc từ đầu "will see at least the final state of all records in the order they were written"; tombstone chỉ được thấy nếu tới head trong thời gian nhỏ hơn `delete.retention.ms` ("the default is 24 hours"): "it is possible for a consumer to miss delete markers if it lags by more than delete.retention.ms."
([design], mục What guarantees does log compaction provide?)

### 2.3 Chi tiết vận hành

- Tombstone: "A message with a key and a null payload will be treated as a delete from the log." Tombstone cũng bị dọn sau một thời gian ([design]).
- Offset bị compact mất vẫn là vị trí hợp lệ: đọc từ offset đã mất thì nhận message kế tiếp còn lại ([design], ví dụ offset 36, 37, 38).
- **Segment đang ghi (active) không bao giờ bị compact**; record chỉ đủ điều kiện sau khi segment roll ("it grows to segment.bytes, or its age reaches the smaller of segment.ms and max.compaction.lag.ms") ([design]).
- Hạn compaction "is not a hard guarantee since it is still subjected to the availability of log cleaner threads"; theo dõi `uncleanable-partitions-count`, `max-clean-time-secs`, `max-compaction-delay-secs` ([design]).
- Default 4.4: `delete.retention.ms`=86400000 (1 ngày), `min.compaction.lag.ms`=0, `max.compaction.lag.ms`=`Long.MAX`, `min.cleanable.dirty.ratio`=0.5 ([topic-cfg]). `log.cleaner.enable` deprecated, "will be removed in Kafka 5.0", đừng đặt false ([broker-cfg]).
- Compaction giữ lại offset/sequence đầu-cuối của batch để khôi phục trạng thái producer idempotent; có thể sửa `baseTimestamp` của batch chứa tombstone thành "delete horizon" ([msg-format]).
- (suy ra) Topic `compact` **không** phải lịch sử đầy đủ: không dùng một mình làm audit trail; dùng làm "trạng thái mới nhất" (ví dụ số dư hiện tại) song song với một topic `delete` giữ đủ sự kiện.

---

## 3. Tiered storage

### 3.1 Apache Kafka (4.4)

- Trạng thái: KIP-405 "has been merged and marked production-ready since Kafka 3.9" ([kip-405]). Trang GA release notes nói tính năng bật ở cả mức cụm và mức topic, client không cần sửa ("no changes are required in Kafka clients to read from that topic") ([ts-ga]).
- Hai tầng: local (đĩa broker) và remote ("such as HDFS or S3") chứa segment đã đóng ([tiered]).
- Bật: broker `remote.log.storage.system.enable=true` (default false); topic `remote.storage.enable=true` (default false). Apache **không kèm** implementation: "Kafka server doesn't provide out-of-the-box implementation of RemoteStorageManager"; bản `LocalTieredStorage` chỉ dùng thử/test ([tiered], [topic-cfg], [broker-cfg]).
- Retention hai lớp: `local.retention.ms`/`local.retention.bytes` (default -2 = dùng `retention.ms`/`retention.bytes`), phải <= retention tổng ([topic-cfg]).
- Giới hạn (4.4): "No support for compacted topics"; phải tắt tiered trên mọi topic trước khi tắt ở broker; admin action cần client >= 3.0 ([tiered]). GA notes thêm: mỗi Fetch request chỉ phục vụ **một partition** từ remote, "This can limit client throughput" ([ts-ga]).
- Thay đổi 4.x đáng chú ý: 4.3.2 sửa "a critical tiered storage bug in which size-based retention could delete data that was still within retention from both local and remote tiers after a leader election (KAFKA-20732)"; 4.4 sửa local retention với segment có timestamp tương lai; 4.4 thêm `remote.copy.lag.bytes`/`remote.copy.lag.ms` (KIP-1241) ([upgrade]).

### 3.2 Amazon MSK

- Chỉ cho Standard brokers, cụm provisioned, Kafka >= 3.6.0 (hoặc 2.8.2.tiered); "The minimum retention period in low-cost storage is 3 days"; không hỗ trợ topic compact; tắt được cho từng topic nhưng "cannot be re-enabled for a topic"; không có ở GovCloud ([msk-tiered]).
- Đọc từ tầng rẻ: "you can expect an increase in read latency for the first few bytes", sau đó đọc tuần tự gần như tầng chính; AWS nêu lợi ích "reprocess old data in its exact production order with your existing stream processing code and Kafka APIs" ([msk-tiered]).
- AWS khuyên không dùng `read_committed` khi đọc từ remote tier trừ khi thật sự dùng transaction ([msk-tiered]).
- Segment được chép lên tầng rẻ ngay khi **đóng** (theo `segment.bytes`/`segment.ms`), không đợi local retention ([msk-tiered-retention]).
- Confluent Cloud: theo `research/0004`, `retention.ms=-1` là Infinite Storage. Không đọc lại trong phiên này.

---

## 4. Replay: đặt lại vị trí đọc

### 4.1 CLI: `kafka-consumer-groups.sh --reset-offsets`

Theo [ops-basic] (mục Managing Consumer Groups):
- "This option supports one consumer group at the time"; phạm vi `--all-topics` hoặc `--topic`; "first make sure that the consumer instances are inactive".
- Chế độ: `--dry-run` ("the default"), `--execute`, `--export` (CSV).
- Kịch bản: `--to-datetime 'YYYY-MM-DDThh:mm:ss.sss'`, `--to-earliest`, `--to-latest`, `--shift-by <n>` (âm hoặc dương), `--from-file <csv>`, `--to-current`, `--by-duration 'PnDTnHnMnS'`, `--to-offset`.
- Offset ngoài khoảng được kẹp về đầu/cuối hợp lệ ("if offset end is at 10 and offset shift request is of 15, then, offset at 10 will actually be selected").

Hành vi đọc từ source code 4.4.0 (không có trong docs):
- `--to-datetime` gọi `Utils.getDateTime`: nếu phần giờ không chứa `+`, `-` hoặc `Z` thì code **nối thêm `"Z"`**, tức là UTC; định dạng `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` (có thể kèm offset như `+07:00`) ([src-utils]).
- Partition không tìm được offset theo timestamp (ví dụ timestamp sau message cuối) thì in "Warn: Partition ... is empty. Falling back to latest known offset." và dùng log-end offset ([src-gor]).

### 4.2 API Java của Apache Kafka

- `seek(TopicPartition, long)`, `seekToBeginning(Collection)`, `seekToEnd(Collection)`: "a consumer can re-consume older records, or skip to the most recent records without actually consuming the intermediate records" ([consumer-javadoc], mục Controlling The Consumer's Position).
- Docs nêu đúng usecase rebuild: nếu state cục bộ bị mất, "the state may be recreated on a new machine by re-consuming all the data and recreating the state (assuming that Kafka is retaining sufficient history)" ([consumer-javadoc]).
- `offsetsForTimes(Map<TopicPartition, Long>)`: "The returned offset for each partition is the earliest offset whose timestamp is greater than or equal to the given timestamp"; "This is a blocking call. The consumer does not have to be assigned the partitions."; không tìm thấy thì entry là `null` ([consumer-javadoc]). Sau đó `seek()` tới offset trả về (suy ra, cách dùng chuẩn).
- Lưu offset ngoài Kafka (cùng giao dịch với kết quả) rồi "On restart restore the position of the consumer using seek(TopicPartition, long)" ([consumer-javadoc]).

### 4.3 Spring Kafka 4.1.2

- Listener implement `ConsumerSeekAware` (`registerSeekCallback`, `onPartitionsAssigned`, `onPartitionsRevoked`, `onIdleContainer`). Callback có `seek`, `seek(..., Function<Long,Long>)` (từ 3.2), `seekToBeginning`, `seekToEnd`, `seekRelative` (từ 2.3), `seekToTimestamp(topic, partition, ts)` và `seekToTimestamp(Collection<TopicPartition>, ts)` (từ 2.3), `getGroupId()` (từ 3.3) ([sk-seek]).
- Khi seek cùng timestamp cho nhiều partition trong `onPartitionsAssigned`/`onIdleContainer`, nên dùng bản nhận `Collection` vì "it is more efficient to find the offsets for the timestamps in a single call to the consumer's offsetsForTimes method" ([sk-seek]).
- `AbstractConsumerSeekAware` (từ 2.3) tự giữ callback theo partition; từ 2.6 có `seekToBeginning()`, `seekToEnd()`, `seekToTimestamp(long)` cho mọi partition đang giữ; ví dụ docs: `listener.seekToTimestamp(System.currentTimeMillis() - 60_000)` ([sk-seek]).
- Ví dụ docs cho topic compact: `callback.seekToBeginning(assignments.keySet())` trong `onPartitionsAssigned` để đọc lại từ đầu "every time the application is started" ([sk-seek]).
- Offset ban đầu cố định: `@TopicPartition(topic=..., partitionOffsets = @PartitionOffset(partition = "*", initialOffset = "0"))` (manual assignment) ([sk-annot], [sk-initial-offset]). Với group management: group mới đi theo `auto.offset.reset`, group cũ đi theo offset đã commit, "You can, however, seek to a specific offset during initialization" ([sk-initial-offset]).
- (suy ra) Bẫy: `onPartitionsAssigned` chạy **mỗi lần được giao partition**, kể cả sau rebalance. Nếu đặt `seekToTimestamp` vô điều kiện ở đó, mỗi rebalance sẽ replay lại. Muốn replay một lần thì dùng CLI reset offset khi group dừng, hoặc gọi seek runtime qua callback đã lưu.

---

## 5. Timestamp của message

- Mỗi event có "a key, value, timestamp, and optional metadata headers" ([intro]).
- Producer: "If the user did not provide a timestamp, the producer will stamp the record with its current time"; topic `CreateTime` thì broker dùng timestamp của producer; topic `LogAppendTime` thì timestamp "will be overwritten by the broker with the broker local time when it appends the message to its log"; timestamp thực dùng được trả về trong `RecordMetadata` ([producer-record]).
- `message.timestamp.type` (topic) / `log.message.timestamp.type` (broker): default `CreateTime` ([topic-cfg], [broker-cfg]).
- Kiểm soát lệch giờ khi `CreateTime`: `message.timestamp.before.max.ms` default `Long.MAX` (không chặn timestamp cũ); `message.timestamp.after.max.ms` default 3600000 (1 giờ, đổi từ `Long.MAX` ở 4.0, KIP-1030): "messages with a timestamp of more than 1 hour in the future will be rejected" ([topic-cfg], [upgrade]). Cả hai bị bỏ qua khi `LogAppendTime`.
- Time index chỉ thêm entry "when the timestamp is greater than the last indexed timestamp" ([topic-cfg], `index.interval.bytes`). (suy ra) Với `CreateTime` không tăng đều (nhiều producer lệch giờ), tra theo timestamp cho kết quả theo định nghĩa "earliest offset whose timestamp >= T", có thể có record trước offset đó mang timestamp lớn hơn T và record sau mang timestamp nhỏ hơn T.
- `by_duration` (reset tự động) dựa vào đồng hồ client so với timestamp phía broker: docs nói "Accurate clock synchronization between clients and brokers is therefore required" ([ops-basic]).

---

## 6. Kafka làm event log / audit trail: đảm bảo và giới hạn

| Câu hỏi | Docs nói gì | Nguồn |
|---|---|---|
| Thứ tự | "Kafka guarantees that any consumer of a given topic-partition will always read that partition's events in exactly the same order as they were written." Chỉ trong một partition. | [intro] |
| Offset có đổi không | "The offset for a message never changes" (kể cả khi compact). | [design] |
| Ghi kiểu gì | "The log allows serial appends which always go to the last file." KIP-405 mô tả "append-only log segments". | [impl-log], [kip-405] |
| Có API sửa record không | Không thấy trong Admin API; chỉ có `deleteRecords`: "Delete records whose offset is smaller than the given offset". Quyền: `DELETE_RECORDS` cần `Delete` trên `Topic`. | [admin-javadoc], [acl] |
| Đã commit có mất không | "a committed message will not be lost, as long as there is at least one in sync replica alive, at all times." | [design] |
| Event sourcing | "Kafka's support for very large stored log data makes it an excellent backend for an application built in this style." | [uses] |
| Commit log | Kafka làm "a kind of external commit-log for a distributed system"; compaction hỗ trợ usage này. | [uses] |
| Toàn vẹn khi crash | Khôi phục log kiểm tra CRC32; phát hiện hỏng thì "the log is truncated to the last valid offset". | [impl-log] |

**Giới hạn cần nói rõ khi dùng làm audit (suy ra từ các nguồn trên):**
1. Docs **không** dùng từ "immutable" cho log. Đúng hơn: append-only, không có API sửa, nhưng **xoá được** (retention, compaction, `deleteRecords`, xoá topic, `cleanup.policy` đổi được bằng `kafka-configs.sh`).
2. Thứ tự chỉ theo partition: audit cần thứ tự theo tài khoản thì key phải là số tài khoản; không có thứ tự toàn cục giữa các tài khoản.
3. `CreateTime` là giờ của producer (ứng dụng tự đặt được). Audit cần "giờ hệ thống ghi nhận" thì xem xét `LogAppendTime`, hoặc lưu cả giờ nghiệp vụ trong payload và giờ broker.
4. Retention mặc định 7 ngày; audit nhiều năm cần `retention.ms=-1` hoặc tiered storage, và quyết định này thuộc chính sách lưu trữ của ngân hàng (hỏi platform team; MSK tiered tối thiểu 3 ngày ở tầng rẻ).
5. Topic compact chỉ giữ bản cuối mỗi key, không phải lịch sử.
6. Muốn "đọc lại từ đầu" thì dữ liệu phải còn: docs viết "assuming that Kafka is retaining sufficient history" ([consumer-javadoc]).

---

## 7. Usecase gợi ý

### U1. Banking: dựng lại số dư bằng replay
- Topic `account-transactions` (`cleanup.policy=delete`, retention đủ dài theo quy định), key = số tài khoản (thứ tự theo tài khoản, [intro]).
- Topic `account-balance` (`cleanup.policy=compact`, key = số tài khoản) làm trạng thái mới nhất; service mới khởi động `seekToBeginning` trên topic này để nạp cache ([design], [sk-seek]).
- Phát hiện bug tính phí đã chạy từ 3 ngày trước: dừng group, `kafka-consumer-groups.sh --reset-offsets --group balance-svc --topic account-transactions --to-datetime 2026-11-14T00:00:00.000+07:00 --dry-run`, kiểm tra, rồi `--execute` ([ops-basic], [src-utils]). Lưu ý replay sinh **trùng hiệu ứng** nếu consumer ghi ra ngoài: cần xử lý idempotent (pattern tự dựng, xem bài 5-7).
- Câu hỏi "oái oăm": tài khoản đóng được ghi tombstone vào `account-balance`; một service dựng cache chạy chậm hơn 1 ngày (`delete.retention.ms`) sẽ không thấy tombstone, nhưng các bản ghi cũ của key đó cũng đã bị compact; tuỳ thời điểm cleaner chạy mà cache có thể còn giữ tài khoản đã đóng ([design]). (suy ra) Tăng `delete.retention.ms` cho topic này.
- Câu hỏi "oái oăm" 2: team nạp lại lịch sử giao dịch 2 năm trước vào topic `delete` 7 ngày với `CreateTime` gốc -> segment có largest timestamp quá hạn -> bị xoá ngay sau khi roll ([impl-log], [topic-cfg]). Cách xử lý có trong docs: `LogAppendTime` cho topic đó, hoặc `retention.ms` phù hợp.

### U2. Banking: audit trail
- Thứ tự theo tài khoản, offset vĩnh viễn, committed không mất khi còn một ISR ([intro], [design]).
- Quyền: chỉ cấp `Delete` trên Topic cho vai trò quản trị, vì `deleteRecords` và xoá topic đều cần quyền này ([acl]). (suy ra) Giám sát thay đổi `cleanup.policy`/`retention.ms` vì đó là cách "xoá" ít ai để ý.
- Giờ: lưu giờ nghiệp vụ trong payload; cân nhắc `LogAppendTime` để có giờ broker không do ứng dụng đặt ([producer-record]).

### U3. Chứng khoán: replay một ngày giao dịch
- Mục tiêu: chạy lại thuật toán khớp/tính toán trên dữ liệu phiên 2026-11-14 (09:00-15:00 giờ Việt Nam).
- Mở đầu: group mới, `--to-datetime 2026-11-14T09:00:00.000+07:00 --execute`. Không ghi `+07:00` thì tool hiểu là 09:00 UTC = 16:00 giờ Việt Nam, bỏ qua cả phiên ([src-utils]).
- Kết thúc: consumer tự dừng khi gặp record có timestamp > 15:00 (docs không có option "dừng ở timestamp"; suy ra là logic ứng dụng). Hoặc dùng `offsetsForTimes` cho mốc 15:00 để biết offset kết thúc từng partition ([consumer-javadoc]).
- Spring: trong listener riêng cho replay, `seekToTimestamp(partitions, ts)` một lần ([sk-seek]); tránh đặt vô điều kiện trong `onPartitionsAssigned` (mục 4.3).
- Dữ liệu cũ hơn local retention trên MSK tiered: lần đọc đầu chậm hơn, sau đó tuần tự ([msk-tiered]); trên Apache, mỗi fetch chỉ lấy một partition từ remote ([ts-ga]).

---

## Điểm nên dạy (80/20)

1. Kafka giữ dữ liệu theo retention, không theo việc đã đọc; `retention.ms` là "SLA" cho consumer, default 7 ngày.
2. Xoá theo segment, căn theo timestamp lớn nhất trong segment: dữ liệu có thể sống lâu hơn (chờ roll) hoặc ngắn hơn (timestamp cũ) so với `retention.ms`.
3. Compaction = trạng thái mới nhất mỗi key, giữ thứ tự và offset; tombstone sống `delete.retention.ms` (1 ngày); không dùng compact làm lịch sử.
4. Replay có ba đường: CLI `--reset-offsets` (group dừng, mặc định dry-run), `seek`/`offsetsForTimes`, Spring `ConsumerSeekAware`/`AbstractConsumerSeekAware.seekToTimestamp`.
5. `--to-datetime` không ghi múi giờ là UTC: luôn ghi `+07:00`.
6. `CreateTime` (default, producer đặt) khác `LogAppendTime` (broker đặt); từ 4.0 timestamp tương lai quá 1 giờ bị từ chối.
7. Audit: thứ tự chỉ trong partition, offset vĩnh viễn, append-only nhưng **xoá được**; bảo vệ bằng ACL `Delete` trên Topic và chính sách retention.
8. Tiered storage production-ready từ 3.9, không cho topic compact; MSK tiered tối thiểu 3 ngày ở tầng rẻ, đọc lần đầu chậm hơn.

## Chưa kiểm chứng / mâu thuẫn

1. Segment active có bị xoá theo retention không: docs 4.4 nói `retention.ms` "triggers the rolling of new segment if the retention.ms condition is satisfied" nhưng không nói rõ thứ tự roll rồi xoá; chưa đọc source `UnifiedLog`.
2. Hành vi `offsetsForTimes` khi `CreateTime` không tăng đều: chỉ có định nghĩa "earliest offset whose timestamp >= T" và ghi chú về time index; phần ví dụ ở mục 5 là suy ra.
3. `deleteRecords` có xoá được dữ liệu ở tầng remote (tiered) không: chưa kiểm.
4. Spring Kafka `seekToTimestamp` khi không có record nào >= timestamp (ví dụ timestamp ở tương lai): docs không nói; chưa đọc source.
5. `--to-datetime` dựa trên source tag `4.4.0` (`Utils.getDateTime`, `GroupOffsetsResetter`); docs không nói UTC. Hành vi có thể khác ở bản cũ hơn.
6. MSK `retention.ms` "Minimum 3 days" (đã ghi Unverified ở `research/0003`): trang tiered nói rõ 3 ngày là cho **tầng rẻ** và "There is no minimum retention period for primary storage" ([msk-tiered]); vẫn chưa rõ cụm non-tiered có ràng buộc gì.
7. Confluent Cloud Infinite Storage: lấy từ `research/0004`, không đọc lại.
8. Mâu thuẫn nhỏ trong docs: trang Tiered Storage 4.4 liệt kê "Admin actions ... only supported on clients from version 3.0 onwards", còn MSK yêu cầu client >= 3.0.0 để tạo topic tiered nhưng cho phép client cũ hơn chuyển topic có sẵn (với 2.8.2.tiered) ([tiered], [msk-tiered]). Không ảnh hưởng nếu dùng client 4.x.

---

## Sources

- [topic-cfg] https://kafka.apache.org/44/configuration/topic-configs/ - retention, segment, cleanup.policy, compaction, timestamp, tiered topic configs, `index.interval.bytes`
- [broker-cfg] https://kafka.apache.org/44/configuration/broker-configs/ - `log.retention.*`, `log.retention.check.interval.ms`, `log.roll.hours`, `offsets.retention.minutes`, `log.cleaner.enable`, `remote.log.storage.system.enable`, `log.message.timestamp.type`
- [consumer-cfg] https://kafka.apache.org/44/configuration/consumer-configs/ - `auto.offset.reset` (earliest, latest, by_duration, none)
- [design] https://kafka.apache.org/44/design/design/ - Consumer Position (rewind), Log Compaction (guarantees, tombstone, cleaner), Replicated Logs (committed not lost)
- [impl-log] https://kafka.apache.org/44/implementation/log/ - Writes, Reads (OutOfRangeException), Deletes theo segment, Guarantees (CRC)
- [msg-format] https://kafka.apache.org/44/implementation/message-format/ - record batch, timestampType, delete horizon khi compaction
- [ops-basic] https://kafka.apache.org/44/operations/basic-kafka-operations/ - `--reset-offsets`, by_duration và đồng bộ đồng hồ
- [tiered] https://kafka.apache.org/44/operations/tiered-storage/ - cấu hình, giới hạn tiered storage
- [tiered-cfg] https://kafka.apache.org/44/configuration/tiered-storage-configs/ - bảng config tiered storage (tra cứu)
- [kip-405] https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage - "production-ready since Kafka 3.9"
- [ts-ga] https://cwiki.apache.org/confluence/x/9xDOEg - Kafka Tiered Storage GA Release Notes (tính năng, giới hạn, một partition remote mỗi fetch)
- [upgrade] https://kafka.apache.org/44/getting-started/upgrade/ - `message.timestamp.after.max.ms` 1 giờ (4.0), `segment.bytes` tối thiểu 1 MiB (4.0), `cleanup.policy` rỗng (4.2), bug tiered 4.3.2, thay đổi tiered 4.4
- [intro] https://kafka.apache.org/44/getting-started/introduction/ - không xoá sau khi đọc, thứ tự trong partition, cấu trúc event
- [uses] https://kafka.apache.org/44/getting-started/uses/ - Event Sourcing, Commit Log
- [consumer-javadoc] https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html - seek, seekToBeginning/End, offsetsForTimes, lưu offset ngoài Kafka
- [producer-record] https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html - CreateTime vs LogAppendTime
- [admin-javadoc] https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/admin/Admin.html - `deleteRecords`
- [acl] https://kafka.apache.org/44/security/authorization-and-acls/ - `DELETE_RECORDS` cần `Delete` trên `Topic`
- [src-gor] https://github.com/apache/kafka/blob/4.4.0/tools/src/main/java/org/apache/kafka/tools/GroupOffsetsResetter.java - `resetToDateTime`, fallback về latest
- [src-utils] https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java - `getDateTime`: thiếu múi giờ thì thêm `Z`
- [sk-seek] https://docs.spring.io/spring-kafka/reference/kafka/seek.html - ConsumerSeekAware, ConsumerSeekCallback, AbstractConsumerSeekAware, seekToTimestamp
- [sk-initial-offset] https://docs.spring.io/spring-kafka/reference/kafka/topic/partition-initial-offset.html - offset ban đầu với group management và manual assignment
- [sk-annot] https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html - `@PartitionOffset(initialOffset=...)`
- [msk-tiered] https://docs.aws.amazon.com/msk/latest/developerguide/msk-tiered-storage.html - yêu cầu, giới hạn tiered storage MSK
- [msk-tiered-retention] https://docs.aws.amazon.com/msk/latest/developerguide/msk-tiered-storage-retention-rules.html - segment đóng được chép lên tầng rẻ, quy tắc retention
