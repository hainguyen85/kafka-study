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
3. "Producer restart, ứng dụng gửi lại đơn cuối": trùng trong cả hai trường hợp (giới hạn "single session").`,
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
      sims: SIMS.filter((s) => s.lessons.includes(stem)).map((s) => s.id),
      quizzes: quizzesOf(html), md: cleanMd(md, 'lessons'),
    });
  }
  groups.push(g);
}

// Mô phỏng
const simGroup = { title: 'Mô phỏng', items: [] };
for (const s of SIMS) {
  add(simGroup, { id: s.id, kind: 'sim', title: s.title, api: s.api, script: s.script, opts: s.opts || null, lessons: s.lessons, md: s.md });
}
groups.push(simGroup);

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
