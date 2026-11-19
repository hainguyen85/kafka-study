// Sinh dữ liệu khoá học cho portal (portal/index.html) từ các file Markdown của workspace.
//
// Chạy (từ thư mục gốc, SAU `python scripts/html2md.py`):   node scripts/build_portal.mjs
//
// Đầu ra:
//   portal/course.json  dữ liệu khoá học (nhóm, mục, nội dung Markdown, quiz)
//   portal/course.js    cùng dữ liệu, dạng `window.COURSE = ...` để mở portal bằng file:// (không cần web server)
//
// Nội dung lấy từ .md (lessons/, summaries/, reference/, portal/intro.md). Quiz lấy từ Quiz.mount(...) trong .html
// của bài, vì .md chỉ có bản tĩnh; chỗ đặt quiz đánh dấu bằng <!-- quiz:id --> ... <!-- /quiz --> (do html2md sinh).
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '..');
const read = (p) => fs.readFileSync(path.join(ROOT, p), 'utf8');
const exists = (p) => fs.existsSync(path.join(ROOT, p));

// ---------- cấu trúc khoá học ----------
const PARTS = [
  { title: 'Phần 1 · Nền tảng', lessons: ['0001-kafka-big-picture', '0002-key-partition-offset', '0003-build-cluster'] },
  { title: 'Phần 2 · Producer', lessons: ['0004-producer-durability', '0010-idempotent-producer'] },
  { title: 'Phần 3 · Consumer và xử lý lỗi', lessons: ['0005-consumer-offset-commit', '0011-rebalance', '0012-delivery-semantics', '0006-error-handling-dlt', '0007-idempotent-consumer-transactions'] },
  { title: 'Phần 4 · Thiết kế và kiểm tra cuối', lessons: ['0008-topic-design', '0014-throughput-latency', '0015-replay-retention', '0016-schema-evolution', '0009-final-design-test'] },
  { title: 'Phần 5 · Tổng kết', lessons: ['0013-core-80-20'] },
  { title: 'Phần 6 · Bài toán thiết kế', lessons: ['0017-notification-hub'] },
];

// Mô phỏng: tập trung một nhóm. `lessons` = bài dùng mô phỏng này (để bài hiện lối tắt).
const SIMS = [
  {
    id: 'sim-partition', title: 'Key → partition', api: 'PartitionSim', script: 'partition-sim.js',
    opts: { keys: ['DRV-001', 'DRV-002', 'DRV-004'], partitions: [3, 6] }, lessons: ['0002-key-partition-offset', '0008-topic-design'],
    md: `Message có key được băm theo key nên **cùng key luôn vào cùng partition**; không key thì producer dùng sticky partition. Hàm băm trong mô phỏng là hàm đơn giản, không phải hàm của Kafka thật.

**Thử:**
1. Chọn key \`DRV-001\`, bấm "Gửi có key" 3 lần: cùng partition, offset tăng dần.
2. Gửi thêm cho \`DRV-002\`, \`DRV-004\`: mỗi key một "ngăn".
3. Bấm "Gửi không key" khoảng 10 lần: dồn vào một partition một lúc rồi đổi.
4. Đổi sang 6 partition rồi gửi tiếp \`DRV-001\`: key có thể sang partition khác, thứ tự với message cũ không còn đảm bảo.`,
  },
  {
    id: 'sim-ack', title: 'Độ bền: acks, ISR, min.insync.replicas', api: 'AckSim', script: 'ack-sim.js',
    lessons: ['0004-producer-durability'],
    md: `Một partition RF=3 trên ba broker B1, B2, B3. Trong Kafka thật follower **tự fetch** từ leader liên tục; nút "Follower fetch từ leader" gom việc đó thành một bước để dừng thời gian ở khoảnh khắc "leader đã ghi, follower chưa kịp fetch". "Chờ 30 giây" là cho trôi qua \`replica.lag.time.max.ms\`.

**Thử:**
1. \`acks=1\`: gửi 3 message, không fetch, Kill B1 → "ACK NHƯNG MẤT".
2. \`acks=all\`, minISR=2: làm lại → không mất message nào đã ack.
3. \`acks=all\`, minISR=1: Kill B2, B3, Chờ 30 giây, gửi 2 message, Kill B1, Start B2 → partition offline; bật \`unclean.leader.election.enable\` rồi làm lại → mất.
4. \`acks=all\`, minISR=3: Kill B3, gửi, Chờ 30 giây → NotEnoughReplicasAfterAppend, ghi mới bị từ chối.`,
  },
  {
    id: 'sim-idempotence', title: 'Idempotent producer', api: 'IdempotenceSim', script: 'idempotence-sim.js',
    lessons: ['0010-idempotent-producer'],
    md: `Broker cấp producer ID; mỗi batch mang sequence number. Đoán trước rồi bấm, lần lượt khi bật và khi tắt idempotence (đổi ô tick sẽ làm lại từ đầu).

**Thử:**
1. "Gửi, ack bị mất trên đường về": bật thì không trùng, tắt thì trùng.
2. "Gửi 2 batch, batch đầu lỗi tạm thời": bật thì giữ thứ tự, tắt thì đảo.
3. "Gửi, producer crash trước khi nhận ack, restart rồi gửi lại": broker nhớ seq của PID cũ, PID mới chưa có gì để so, nên trùng trong cả hai trường hợp (giới hạn "single session").`,
  },
  {
    id: 'sim-commit', title: 'Commit offset: mất hay trùng', api: 'CommitSim', script: 'commit-sim.js',
    lessons: ['0005-consumer-offset-commit'],
    md: `Một partition 9 record, mỗi \`poll()\` trả 3. "Crash" làm consumer chạy lại từ offset đã commit; việc đang chờ trong worker bị mất.

**Thử:**
1. Chế độ A: poll, xử lý 2/3, crash, chạy lại → không mất, có trùng (at-least-once).
2. Chế độ B: poll hai lần, xử lý 1 record, crash → offset của lô đầu chưa xử lý bị MẤT.
3. Chế độ C: thử mọi kiểu crash → không bao giờ mất, trùng nhiều nhất một record.`,
  },
  {
    id: 'sim-error', title: 'Record lỗi: retry, DLT, blocking và non-blocking', api: 'ErrorSim', script: 'error-sim.js',
    lessons: ['0006-error-handling-dlt'],
    md: `Một partition 8 record cùng key; record ở offset 3 gây lỗi. Bốn cấu hình error handler, ba loại lỗi. Đơn giản hoá: commit sau mỗi record.

**Thử:**
1. A + lỗi vĩnh viễn: 10 lần giao rồi chỉ log ERROR, record mất im lặng.
2. B + lỗi vĩnh viễn: 3 lần giao, cách 300 ms, bản sao vào DLT; ô offset 3 vẫn còn trên partition.
3. C + lỗi vĩnh viễn: tổng back-off 9 phút vượt \`max.poll.interval.ms\`.
4. B rồi D với lỗi tạm thời: blocking giữ thứ tự, non-blocking làm m4 chạy trước m3.
5. Lỗi giải mã: fatal, không retry ở mọi cấu hình.`,
  },
  {
    id: 'sim-rebalance', title: 'Rebalance: eager, cooperative, static', api: 'RebalanceSim', script: 'rebalance-sim.js',
    lessons: ['0011-rebalance'],
    md: `Topic 6 partition, ban đầu C1 và C2. Mỗi partition luôn có 2 record đã xử lý nhưng chưa commit, để thấy trùng khi partition đổi chủ không êm. Đơn giản hoá: cooperative thật cần hai vòng rebalance, ở đây gộp làm một.

**Thử:**
1. Eager, "Thêm consumer": cả 6 partition dừng. Đổi sang Cooperative: chỉ partition đổi chủ dừng.
2. "Crash (kill -9)" so với "Rời êm": crash gây xử lý lại, rời êm thì không.
3. "Deploy lại" khi tắt static membership: hai lần rebalance; bật static membership: không rebalance.
4. "Xử lý quá max.poll.interval.ms": bị đá rồi vào lại, hai lần rebalance cộng trùng.`,
  },
  {
    id: 'sim-exactly', title: 'Outbox, idempotent consumer, transaction: crash ở đâu?', api: 'ExactlySim', script: 'exactly-sim.js',
    lessons: ['0007-idempotent-consumer-transactions'],
    md: `Chọn công cụ và điểm crash; sau crash, mô phỏng khởi động lại từ trạng thái bền (DB, offset đã commit, log Kafka). Thứ gì chỉ nằm trong RAM thì mất.

**Thử:**
1. Ghi kép, crash sau bước 1: DB có đơn, Kafka không có sự kiện. Outbox cùng điểm crash: đúng; crash sau bước 2: trùng trên topic.
2. Phía nhận không chống trùng, crash sau khi cộng điểm: cộng hai lần.
3. Set trong RAM, tick "bản trùng", crash sau bước 3: RAM mất khi restart nên vẫn trùng.
4. UNIQUE(eventId) cùng transaction DB: đúng ở mọi điểm crash.
5. Kafka → Kafka có transaction, crash sau bước 2: \`read_committed\` thấy một bản, \`read_uncommitted\` thấy hai.`,
  },
  {
    id: 'sim-load', title: 'Quá tải: bộ đệm producer, lag, partition là trần', api: 'LoadSim', script: 'load-sim.js',
    lessons: ['0014-throughput-latency'],
    md: `Một topic, một consumer group, tính theo từng giây. Trần thông lượng của bốn cấu hình producer lấy từ số đo lab Bài 8b (payload nén rất tốt): chỉ đáng nhìn tỷ lệ.

**Thử:**
1. Tải **mở cửa phiên**, producer **mặc định 4.x**: bộ đệm đầy sau mấy giây, và chuyện gì xảy ra với \`send()\`? Đổi sang **lz4** rồi chạy tiếp.
2. Tải **bình thường**, xử lý **1 ms**, 6 partition: 2 consumer, rồi 6 consumer. Lag còn tăng không? Đổi sang 0,2 ms.
3. Giữ 1 ms, đổi sang **3 partition** với 6 consumer: consumer nào ngồi không, lag có hết tăng không?
4. 6 partition, 6 consumer, 0,2 ms, tick **key nóng**: partition nào tụt lại, và thêm consumer có giúp không?
5. Tick **pause()**: lag tăng, nhưng vì sao đây không phải sự cố?`,
  },
  {
    id: 'sim-replay', title: 'Retention, đọc lại theo thời điểm, compaction', api: 'ReplaySim', script: 'replay-sim.js',
    lessons: ['0015-replay-retention'],
    md: `Ba tình huống: retention xoá theo segment, đọc lại theo thời điểm bằng \`--reset-offsets\`, compaction và tombstone. Một partition, đếm theo ngày.

**Thử:**
1. **Retention** 7 ngày, segment 7 ngày: cleaner chạy xong, record cũ nhất còn đọc được là ngày nào? Đổi segment 1 ngày rồi so.
2. Tick **nạp lại giao dịch 2 năm trước**: segment vừa ghi hôm nay còn sống không?
3. **Đọc lại theo thời điểm**, không ghi múi giờ, \`--dry-run\`: offset mới là bao nhiêu? Rồi ghi \`+07:00\`, tick \`--execute\`, khởi động lại consumer.
4. Tick "group vẫn đang chạy" rồi chạy lệnh: chuyện gì xảy ra?
5. **Compaction**: chạy compaction, tick "đã trôi qua 1 ngày", chạy lần nữa, rồi cho B đọc tiếp. So với consumer mới đọc từ đầu.`,
  },
  {
    id: 'sim-schema', title: 'Schema Registry: compatibility và consumer nào vỡ', api: 'SchemaSim', script: 'schema-sim.js',
    lessons: ['0016-schema-evolution'],
    md: `Subject \`payments-avro-value\` bắt đầu với v1. Registry kiểm compatibility khi đăng ký; consumer đọc message theo schema id. Mô hình theo luật đọc của Avro, rút gọn.

**Thử:**
1. Mode mặc định **BACKWARD**: thêm \`branchCode\` không default. Registry nói gì? (Đối chiếu với lab.)
2. Thêm \`channel\` có default, đăng ký, cho producer v2 gửi, hai consumer đọc (A vẫn dùng v1).
3. Bỏ \`amount\` (bắt buộc): BACKWARD nhận. Producer gửi bằng version mới, consumer A còn v1 đọc: chuyện gì xảy ra? Rút ra thứ tự nâng cấp.
4. Làm lại, đổi sang **FORWARD** rồi **FULL**, thử lại hai thay đổi trên.
5. Thêm \`channel\` có default, rồi "channel: bỏ default": BACKWARD nhận, **BACKWARD_TRANSITIVE** thì không. Vì sao?`,
  },
];

// Mô phỏng tổng hợp: một sự kiện đi từ producer, qua broker, tới consumer và DB; gộp nhiều bài.
const E2E_MODEL = `Một sự kiện chuyển tiền đi trọn đường: producer → ba broker (B1 leader, B2, B3) → consumer → DB, message lỗi thì sang DLT. Gộp Bài 4 (acks, ISR, \`min.insync.replicas\`), 4b (idempotent producer), 5 (commit offset), 5b (rebalance), 6 (DLT), 7 (idempotent consumer). Ba cấu hình mẫu:

- **Mặc định (Kafka 4.x + Spring Boot)**: \`acks=all\`, idempotence bật, \`min.insync.replicas=1\`, commit sau khi xử lý, không chống trùng, không DLT.
- **Cấu hình cho tiền**: thêm \`min.insync.replicas=2\`, \`UNIQUE(eventId)\` và DLT.
- **Cấu hình "cho nhanh"**: \`acks=1\` (idempotence tự tắt), commit trước khi xử lý xong.

\`\`\`mermaid
flowchart LR
P["Producer<br/>service chuyển tiền"] -->|"① leader chết<br/>② ISR co, đĩa hỏng<br/>③ ack mất<br/>④ producer crash"| K[("Topic payments<br/>B1 leader · B2 · B3")]
K -->|"⑤ consumer crash<br/>⑥ rebalance<br/>⑦ message lỗi"| C["Consumer"]
C --> DB[("DB")]
C -.->|"lỗi dữ liệu"| DLT[("payments-dlt")]
class K ok
class DLT warn
classDef ok fill:#e6f4ea,stroke:#1b7a3d,color:#1c1b19
classDef warn fill:#fbe9e7,stroke:#b3261e,color:#1c1b19
\`\`\`

Bảy sự cố nằm ở ba chặng. Mỗi sự cố có một ô cấu hình chặn được:

| Sự cố | Nguy cơ | Ô chặn |
|---|---|---|
| ① Leader chết trước khi follower fetch | Mất (đã ack) | \`acks=all\` |
| ② ISR co còn leader, rồi đĩa leader hỏng | Mất (đã ack) | \`min.insync.replicas=2\` |
| ③ Ack mất, producer retry | Trùng trong log | idempotence, hoặc chống trùng ở consumer |
| ④ Producer crash, restart, gửi lại | Trùng trong log | chống trùng ở consumer |
| ⑤ ⑥ Consumer crash hoặc rebalance giữa lúc xử lý | Mất (commit trước) hoặc trùng (commit sau) | commit sau + chống trùng |
| ⑦ Message sai dữ liệu | Mất im lặng | DLT có cảnh báo |

Đổi một ô bất kỳ thì cấu hình thành "Tự chọn". Mô phỏng theo mô hình tài liệu mô tả, không phải broker thật; service phát dùng outbox nên sau crash vẫn gửi lại.`;

const COMBO_SIMS = [
  {
    id: 'sim-hub', title: 'Bài toán notification hub: OTP, campaign và nhà cung cấp sập', api: 'HubSim', script: 'hub-sim.js',
    lessons: ['0017-notification-hub'],
    md: `**Đề:** một hub gửi thông báo cho ngân hàng, nhận từ API Connect (sync) và Kafka (async), gửi ra 5 kênh, tải hàng triệu người dùng, không mất, không trùng. Ba ràng buộc: OTP không được chờ campaign; nhà cung cấp có thể sập; mất hay trùng đều là sự cố. Thiết kế đầy đủ ở [Bài 11](../lessons/0017-notification-hub.md).

\`\`\`mermaid
flowchart TB
subgraph A["A: một topic chung"]
direction LR
qa["Partition: C C C … C OTP"] --> wa["Worker<br/>đọc theo offset"] --> za["Zalo 1.000/giây"]
end
subgraph B["B: mỗi mức ưu tiên một topic"]
direction LR
qb1["notif.otp: OTP OTP"] --> wb1["Nhóm OTP"] --> sb["SMS"]
qb2["notif.marketing: C C C … C"] --> wb2["Nhóm campaign"] --> zb["Zalo"]
end
A ~~~ B
class qa bad
class qb1 ok
classDef bad fill:#fbe9e7,stroke:#b3261e,color:#1c1b19
classDef ok fill:#e6f4ea,stroke:#1b7a3d,color:#1c1b19
\`\`\`

Consumer đọc mỗi partition theo thứ tự offset, nên trong topic chung OTP đứng sau campaign; Zalo sập mà retry tại chỗ thì cả hàng đứng. Mô phỏng tính theo giây; OTP 20/giây qua SMS, giao dịch 300/giây qua Push, campaign 100.000 qua Zalo. Mọi con số là giả định; hạn OTP rút còn 30 giây.

\`\`\`mermaid
sequenceDiagram
participant S as Sender
participant P as Nhà cung cấp
participant U as Điện thoại khách
S->>P: gửi bản tin
P->>U: đã gửi tới khách
P--xS: phản hồi mất, timeout
Note over S: Đã gửi hay chưa?
alt Gửi lại
S->>P: khách nhận 2 tin
else Không gửi lại
Note over S: nếu chưa gửi thì mất
else Tra trạng thái
S->>P: hỏi theo notificationId
end
\`\`\`

Dòng "timeout mơ hồ" là giới hạn mà không cấu hình Kafka nào giải được: cần nhà cung cấp hợp tác.

**Thử:**
1. Thiết kế A, retry tại chỗ, bấm "+5 giây" bốn lần. OTP chờ bao lâu? Đổi sang B và làm lại.
2. Thiết kế A, retry topic: OTP đỡ hơn không, và vì sao vẫn chậm?
3. Thiết kế B, retry topic, chạy tới giây 60. Bao nhiêu bản tin vào DLT? Bấm "Replay DLT".
4. Bỏ hai sự cố đầu, tick ba sự cố còn lại. Đếm số bản tin trùng, rồi tick "Chống trùng".
5. Thử ba cách xử lý timeout mơ hồ và nói cái giá của từng cách.`,
  },
  {
    id: 'sim-e2e', title: 'Từ producer tới DB: sự cố ở đâu thì mất, trùng?', api: 'E2ESim', script: 'e2e-sim.js',
    opts: { mode: 'one' }, lessons: ['0009-final-design-test', '0013-core-80-20'],
    md: `${E2E_MODEL}

Ba sự kiện e1, e2, e3; sự cố bạn chọn xảy ra ở e2. Đoán kết quả của e2 trước khi bấm "Chạy".

**Thử:**
1. Cấu hình **mặc định**, lần lượt từng sự cố. Sự cố nào mặc định đã chịu được, sự cố nào làm mất, sự cố nào làm trùng?
2. "Leader B1 chết ngay sau khi ghi": đổi sang \`acks=1\`. Vì sao consumer không bao giờ thấy e2?
3. "Ack bị mất": tắt idempotence (hoặc đặt \`acks=1\`). Log có mấy bản e2? Bật chống trùng: DB còn trùng không?
4. "Producer crash": idempotence bật vẫn có 2 bản trong log. Ô nào cứu được?
5. "Consumer crash" và "rebalance": so commit trước và commit sau. Một bên mất, một bên trùng; ô nào biến "trùng" thành "đúng"?
6. Cấu hình **cho tiền**: chạy lại mọi sự cố.`,
  },
  {
    id: 'sim-e2e-day', title: 'Một ngày ở production: bảy sự cố liên tiếp', api: 'E2ESim', script: 'e2e-sim.js',
    opts: { mode: 'day' }, lessons: ['0009-final-design-test', '0013-core-80-20'],
    md: `${E2E_MODEL}

Tám sự kiện: e1 bình thường, e2 tới e8 mỗi sự kiện gặp một sự cố: ack mất, leader chết, producer crash, ISR co rồi đĩa hỏng, consumer crash, message lỗi, deploy gây rebalance. Bảng kết quả ghi sự kiện nào mất, trùng hay đúng, và ô cấu hình nào sửa được.

**Thử:**
1. Cấu hình **"cho nhanh"**: đếm trước xem bao nhiêu sự kiện mất, bao nhiêu trùng, rồi chạy.
2. **Mặc định**: mặc định của Kafka và Spring đã chịu được những sự cố nào? Còn lại sự cố nào?
3. Từ mặc định, bật **từng ô một** theo cột "Sửa" (\`min.insync.replicas=2\`, chống trùng, DLT) và chạy lại sau mỗi lần. Ô nào sửa được nhiều sự cố nhất?
4. **Cho tiền**: không mất, không trùng. Nói thành lời mỗi ô chặn sự cố nào; đó là phần "độ bền" và "chống trùng" của bài thi Bài 9.`,
  },
];

// ---------- tiện ích ----------
function titleOf(html) {
  const m = html.match(/<title>([^<]*)<\/title>/);
  return m ? m[1].trim() : '';
}
function firstHeading(md) {
  const m = md.match(/^#\s+(.+)$/m);
  return m ? m[1].trim() : '';
}
function quizzesOf(html) {
  const out = {};
  const re = /Quiz\.mount\(document\.getElementById\('([^']+)'\),\s*(\[[\s\S]*?\])\s*\);/g;
  let m;
  while ((m = re.exec(html))) out[m[1]] = new Function('return ' + m[2])();
  return out;
}
// Bỏ dòng tự sinh và thanh điều hướng đầu file .md (portal có điều hướng riêng).
// Ảnh SVG do html2md tách ra (![..](img/x.svg)): chèn thẳng SVG vào trang để nó dùng biến màu của portal
// (đổi theme sáng/tối được). Bỏ <style> riêng của file và dòng trống (để marked giữ nguyên khối HTML).
function inlineSvgs(md, dir) {
  if (!dir) return md;
  return md.replace(/^!\[([^\]\n]*)\]\((img\/[\w.-]+\.svg)\)$/gm, (all, alt, rel) => {
    const p = `${dir}/${rel}`;
    if (!exists(p)) return all;
    const svg = read(p).replace(/<style>[\s\S]*?<\/style>\s*/, '')
      .replace(/^<svg width="[\d.]+" height="[\d.]+"/, '<svg') // chỉ thẻ gốc; rect bên trong giữ width/height
      .split('\n').filter((l) => l.trim()).join('\n');
    return `<figure class="svg-fig">\n${svg}\n</figure>`;
  });
}

function cleanMd(md, dir) {
  return inlineSvgs(md, dir).replace(/^<!--.*?-->\s*/s, '')
    .replace(/^\[[^\n]*\]\([^\n]*\)[^\n]*\n+/, '') // dòng breadcrumb đầu tiên
    .replace(/<!-- quiz:([\w-]+) -->[\s\S]*?<!-- \/quiz -->/g, (_, id) => `<div class="portal-quiz" data-quiz="${id}"></div>`);
}

// ---------- dựng dữ liệu ----------
const groups = [];
const items = {}; // id -> item

function add(group, item) {
  items[item.id] = item;
  group.items.push(item.id);
}

// Giới thiệu
const intro = { title: 'Giới thiệu', items: [] };
add(intro, { id: 'intro', kind: 'intro', title: 'Giới thiệu khóa học', src: 'portal/intro.md', md: read('portal/intro.md') });
groups.push(intro);

// Bài học
for (const part of PARTS) {
  const g = { title: part.title, items: [] };
  for (const stem of part.lessons) {
    const html = read(`lessons/${stem}.html`);
    const md = read(`lessons/${stem}.md`);
    add(g, {
      id: stem, kind: 'lesson', title: titleOf(html) || firstHeading(md),
      src: `lessons/${stem}.md`, html: `lessons/${stem}.html`,
      summary: exists(`summaries/${stem}.md`) ? `sum-${stem}` : null,
      sims: SIMS.concat(COMBO_SIMS).filter((s) => s.lessons.includes(stem)).map((s) => s.id),
      quizzes: quizzesOf(html), md: cleanMd(md, 'lessons'),
    });
  }
  groups.push(g);
}

// Mô phỏng (theo bài) và mô phỏng tổng hợp (nhiều bài, từ producer tới consumer)
for (const [title, list] of [['Mô phỏng', SIMS], ['Mô phỏng tổng hợp', COMBO_SIMS]]) {
  const g = { title, items: [] };
  for (const s of list) {
    add(g, { id: s.id, kind: 'sim', title: s.title, api: s.api, script: s.script, opts: s.opts || null, lessons: s.lessons, md: s.md });
  }
  groups.push(g);
}

// Tóm tắt ôn nhanh (theo thứ tự bài)
const sumGroup = { title: 'Tóm tắt ôn nhanh', items: [] };
for (const part of PARTS) for (const stem of part.lessons) {
  const p = `summaries/${stem}.md`;
  if (!exists(p)) continue;
  const md = read(p);
  add(sumGroup, { id: `sum-${stem}`, kind: 'summary', title: firstHeading(md).replace(/^Tóm tắt\s+/, ''), src: p, lesson: stem, md: cleanMd(md) });
}
groups.push(sumGroup);

// Tra cứu
const refGroup = { title: 'Tra cứu', items: [] };
for (const f of fs.readdirSync(path.join(ROOT, 'reference')).filter((f) => f.endsWith('.html')).sort()) {
  const stem = f.replace(/\.html$/, '');
  const html = read(`reference/${f}`);
  add(refGroup, { id: `ref-${stem}`, kind: 'reference', title: titleOf(html).replace(/^Bảng tra:\s*/, ''), src: `reference/${stem}.md`, html: `reference/${f}`, quizzes: quizzesOf(html), md: cleanMd(read(`reference/${stem}.md`), 'reference') });
}
groups.push(refGroup);

// Bản đồ đường dẫn -> id, để link giữa các file .md/.html mở ngay trong portal.
const paths = {};
for (const it of Object.values(items)) {
  if (it.src) paths[it.src] = it.id;
  if (it.html) paths[it.html] = it.id;
}

const course = {
  title: 'Apache Kafka cho kiến trúc microservices',
  subtitle: 'Spring Kafka · cụm 3 broker tự dựng · tiếng Việt',
  built: new Date().toISOString().slice(0, 10),
  groups, items, paths,
};

const json = JSON.stringify(course, null, 1);
fs.writeFileSync(path.join(ROOT, 'portal/course.json'), json + '\n');
fs.writeFileSync(path.join(ROOT, 'portal/course.js'), '// Tự sinh bởi scripts/build_portal.mjs. Đừng sửa tay.\nwindow.COURSE = ' + json + ';\n');
const n = (k) => Object.values(items).filter((i) => i.kind === k).length;
console.log(`portal: ${n('lesson')} bài, ${n('sim')} mô phỏng, ${n('summary')} tóm tắt, ${n('reference')} tra cứu`);
