/*
 * Mô phỏng Bài 7: crash ở đâu thì mất, trùng hay đúng, với ba công cụ: outbox (phía phát), idempotent consumer
 * (phía nhận), transaction (Kafka → Kafka). Chỉ minh hoạ MÔ HÌNH, không phải client thật:
 *  - Ghi kép: DB và Kafka không có transaction chung; crash ở giữa thì hai bên lệch nhau.
 *  - Outbox: dữ liệu + dòng outbox trong MỘT transaction DB; relay gửi rồi đánh dấu. Crash giữa gửi và đánh dấu
 *    thì relay gửi lại, nên topic có thể có bản trùng: phía nhận vẫn phải chống trùng.
 *  - Consumer: tác dụng phụ rồi mới commit offset (at-least-once). Crash ở giữa thì record được giao lại.
 *    Chống trùng trong RAM mất khi restart; UNIQUE(eventId) cùng transaction DB với tác dụng phụ thì không.
 *  - Transaction Kafka: đọc A, ghi B, commit offset trong một transaction. Crash giữa chừng thì transaction bị abort
 *    (record đã ghi vào B vẫn nằm trong log, đánh dấu aborted). read_uncommitted vẫn thấy record đó; read_committed
 *    thì không. [Kafka design: delivery semantics] [Consumer configs: isolation.level]
 * Đơn giản hoá: abort xảy ra ngay khi instance mới khởi động (thực tế: fencing theo transactional.id hoặc hết
 * transaction.timeout.ms).
 *
 * Dùng: <div id="xsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/exactly-sim.js"></script>
 *       <script>ExactlySim.mount(document.getElementById('xsim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  // ---------- các biến thể ----------
  // plan(S) trả danh sách bước tính từ trạng thái BỀN hiện tại; sau crash, plan(S) được gọi lại (đó là "khởi động lại").
  var V = {
    dual: {
      label: 'Phía phát: ghi kép (lưu DB rồi gửi Kafka)', fam: 'pub',
      plan: function (S) {
        if (S.requestDone || S.requestLost) return [];
        return [
          { say: 'Lưu đơn #1 vào DB, commit.', tok: ['n1', 'n2', 'đơn #1', 'msg'], apply: function (S) { S.orders++; } },
          { say: 'Gửi OrderPlaced e1 lên Kafka.', tok: ['n1', 'n4', 'e1', 'msg'], apply: function (S) { S.topic.push({ id: 'e1' }); S.requestDone = true; } }
        ];
      },
      crash: function (S) { S.requestLost = true; },
      restartNote: 'Request đang xử lý mất theo process: không ai biết còn thiếu bước gửi Kafka.',
      verdict: function (S) {
        if (S.orders && !S.topic.length) return { bad: true, text: 'MẤT sự kiện: DB có đơn #1, Kafka không có e1. Service đọc Kafka (kho, email) không bao giờ biết.' };
        return { text: 'ĐÚNG (lần này): DB có đơn, Kafka có e1. Nhưng thử crash sau bước 1.' };
      }
    },
    outbox: {
      label: 'Phía phát: outbox + relay', fam: 'pub',
      plan: function (S) {
        var steps = [];
        if (!S.orders) steps.push({ say: 'MỘT transaction DB: lưu đơn #1 và dòng outbox e1. Hoặc cả hai, hoặc không.', tok: ['n1', 'n2', 'đơn + e1', 'msg'],
          apply: function (S) { S.orders++; S.outbox.push({ id: 'e1', sent: false }); } });
        var unsent = S.outbox.filter(function (r) { return !r.sent; });
        if (!S.orders || unsent.length) {
          steps.push({ say: 'Relay đọc dòng outbox chưa gửi, gửi e1 lên Kafka.', tok: ['n3', 'n4', 'e1', 'msg'], apply: function (S) { S.topic.push({ id: 'e1' }); } });
          steps.push({ say: 'Relay đánh dấu dòng outbox e1 là đã gửi.', tok: ['n3', 'n2', 'đã gửi', 'ack'], apply: function (S) { S.outbox.forEach(function (r) { r.sent = true; }); } });
        }
        return steps;
      },
      crash: function () {},
      restartNote: 'Relay khởi động lại, quét bảng outbox tìm dòng chưa đánh dấu.',
      verdict: function (S) {
        if (S.topic.length > 1) return { warn: true, text: 'KHÔNG MẤT, nhưng TRÙNG trên topic: ' + S.topic.length + ' bản e1 (relay gửi lại vì chưa kịp đánh dấu). Phía nhận phải chống trùng theo eventId.' };
        return { text: 'ĐÚNG: DB có đơn, topic có đúng một e1. Crash ở bước nào thì relay cũng gửi bù.' };
      }
    },
    none: { label: 'Phía nhận: không chống trùng', fam: 'con', dedup: 'none' },
    ram: { label: 'Phía nhận: chống trùng bằng Set trong RAM', fam: 'con', dedup: 'ram' },
    db: { label: 'Phía nhận: UNIQUE(eventId) cùng transaction DB', fam: 'con', dedup: 'db' },
    notx: { label: 'Kafka → Kafka: không transaction', fam: 'kk', tx: false },
    tx: { label: 'Kafka → Kafka: transaction (transaction-id-prefix)', fam: 'kk', tx: true }
  };

  function conPlan(v) {
    return function (S) {
      var steps = [];
      for (var o = S.committed; o < S.topic.length; o++) (function (o) {
        var id = S.topic[o].id;
        steps.push({ say: 'poll(): nhận ' + id + ' ở offset ' + o + '.', tok: ['n1', 'n2', id, 'msg'], apply: function () {} });
        steps.push({
          say: null, tok: null, apply: function (S) {
            if (v.dedup === 'ram' && S.ram.indexOf(id) >= 0) { S.lastSay = 'Set trong RAM đã có ' + id + ': bỏ qua.'; S.lastTok = ['n2', 'n2', '', 'ack']; return; }
            if (v.dedup === 'db' && S.eventIds.indexOf(id) >= 0) { S.lastSay = 'INSERT eventId ' + id + ' vi phạm UNIQUE: transaction DB rollback, bỏ qua, không cộng điểm.'; S.lastTok = ['n2', 'n3', 'trùng', 'err']; return; }
            S.points += 10;
            if (v.dedup === 'ram') S.ram.push(id);
            if (v.dedup === 'db') S.eventIds.push(id);
            S.lastSay = v.dedup === 'db' ? 'Một transaction DB: INSERT eventId ' + id + ' và cộng 10 điểm.' : 'Cộng 10 điểm vào DB' + (v.dedup === 'ram' ? ', thêm ' + id + ' vào Set trong RAM.' : '.');
            S.lastTok = ['n2', 'n3', '+10', 'msg'];
          }
        });
        steps.push({ say: 'Commit offset ' + (o + 1) + '.', tok: ['n2', 'n4', 'commit ' + (o + 1), 'ack'], apply: function (S) { S.committed = o + 1; } });
      })(o);
      return steps;
    };
  }
  function kkPlan(v) {
    return function (S) {
      var steps = [];
      if (S.committed >= 1) return steps;
      steps.push({ say: v.tx ? 'beginTransaction(), poll(): nhận e1 từ topic A.' : 'poll(): nhận e1 từ topic A.', tok: ['n1', 'n2', 'e1', 'msg'], apply: function () {} });
      steps.push({ say: v.tx ? 'Gửi kết quả r1 sang topic B (trong transaction, chưa commit).' : 'Gửi kết quả r1 sang topic B (ghi ngay, không có transaction).', tok: ['n2', 'n3', 'r1', 'msg'],
        apply: function (S) { S.outB.push({ id: 'r1', state: v.tx ? 'open' : 'done' }); } });
      steps.push({ say: v.tx ? 'sendOffsetsToTransaction + commitTransaction(): offset A và r1 cùng commit.' : 'Commit offset 1 của topic A.', tok: ['n2', 'n1', v.tx ? 'commit tx' : 'commit 1', 'ack'],
        apply: function (S) { S.committed = 1; S.outB.forEach(function (r) { if (r.state === 'open') r.state = 'done'; }); } });
      return steps;
    };
  }
  Object.keys(V).forEach(function (k) {
    var v = V[k];
    if (v.fam === 'con') {
      v.plan = conPlan(v);
      v.crash = function (S) { S.ram = []; };
      v.restartNote = 'Consumer khởi động lại, đọc tiếp từ offset đã commit. Bộ nhớ RAM (nếu có) trống.';
      v.verdict = function (S) {
        var want = 10 * uniq(S.topic).length;
        if (S.points > want) return { bad: true, text: 'TRÙNG: cộng ' + S.points + ' điểm, đúng ra là ' + want + '.' };
        return { text: 'ĐÚNG: cộng ' + S.points + ' điểm' + (S.topic.length > uniq(S.topic).length ? ' dù topic có bản trùng' : '') + '.' };
      };
    }
    if (v.fam === 'kk') {
      v.plan = kkPlan(v);
      v.crash = function (S) { S.outB.forEach(function (r) { if (r.state === 'open') r.state = 'aborted'; }); };
      v.restartNote = v.tx ? 'Instance mới với cùng transactional.id: transaction dang dở bị abort. r1 đã ghi vẫn nằm trong log, đánh dấu aborted.' : 'Ứng dụng khởi động lại, đọc lại từ offset đã commit của topic A.';
      v.verdict = function (S, iso) {
        var seen = S.outB.filter(function (r) { return iso === 'read_uncommitted' || r.state === 'done'; }).length;
        if (seen > 1) return { bad: true, text: 'TRÙNG ở phía đọc topic B: reader ' + iso + ' thấy ' + seen + ' bản r1' + (S.outB.some(function (r) { return r.state === 'aborted'; }) ? ' (kể cả bản aborted)' : '') + '.' };
        return { text: 'ĐÚNG: reader ' + iso + ' thấy đúng một r1' + (S.outB.length > 1 ? ' (bản aborted vẫn nằm trong log nhưng bị bỏ qua)' : '') + '.' };
      };
    }
  });
  function uniq(t) { var s = {}; t.forEach(function (r) { s[r.id] = 1; }); return Object.keys(s); }

  function mount(root) {
    var S;
    var box = el('div', 'sim');
    var row = el('div', 'row'), row1 = el('div', 'row'), row2 = el('div', 'row');
    var sel = el('select'), crashSel = el('select'), iso = el('select');
    var dupLab = el('label'), dup = el('input'); dup.type = 'checkbox';
    dupLab.appendChild(dup); dupLab.appendChild(document.createTextNode(' Topic có sẵn bản trùng e1 (ứng dụng gửi lại)'));
    Object.keys(V).forEach(function (k) { var o = el('option', null, V[k].label); o.value = k; sel.appendChild(o); });
    ['read_committed', 'read_uncommitted'].forEach(function (k) { var o = el('option', null, 'Reader topic B: isolation.level=' + k); o.value = k; iso.appendChild(o); });
    sel.setAttribute('aria-label', 'công cụ'); crashSel.setAttribute('aria-label', 'điểm crash'); iso.setAttribute('aria-label', 'isolation level của reader');
    row.appendChild(sel); row1.appendChild(crashSel); row1.appendChild(iso); row1.appendChild(dupLab);
    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row1, row2, stageHost, cap, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var st = global.FlowAnim ? FlowAnim.stage(stageHost, { width: 640, height: 150, label: 'Các thành phần của luồng: service hoặc consumer, DB, relay, topic.' }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    if (st) {
      st.node('n1', { x: 10, y: 40, w: 145, h: 62 }); st.node('n2', { x: 170, y: 40, w: 145, h: 62 });
      st.node('n3', { x: 330, y: 40, w: 145, h: 62 }); st.node('n4', { x: 490, y: 40, w: 140, h: 62 });
      st.text('head', { x: 10, y: 22, cls: 'strong' });
    }

    function v() { return V[sel.value]; }
    function fresh() {
      S = { orders: 0, outbox: [], topic: [], requestDone: false, requestLost: false, ram: [], eventIds: [], points: 0, committed: 0, outB: [], events: [], ran: false, crashed: false };
      if (v().fam === 'con') { S.topic.push({ id: 'e1' }); if (dup.checked) S.topic.push({ id: 'e1' }); }
      if (v().fam === 'kk') S.topic.push({ id: 'e1' });
    }
    function fillCrash() {
      crashSel.innerHTML = '';
      fresh();
      var steps = v().plan(S), o = el('option', null, 'Không crash'); o.value = '0'; crashSel.appendChild(o);
      steps.forEach(function (s, i) {
        if (i === steps.length - 1) return;
        var probe = s.say || stepName(i);
        var x = el('option', null, 'Crash sau bước ' + (i + 1) + ': ' + short(probe)); x.value = String(i + 1); crashSel.appendChild(x);
      });
      iso.style.display = v().fam === 'kk' ? '' : 'none';
      dupLab.style.display = v().fam === 'con' ? '' : 'none';
    }
    function stepName(i) { return v().fam === 'con' && i % 3 === 1 ? 'tác dụng phụ (cộng điểm), chưa commit offset' : 'bước ' + (i + 1); }
    function short(t) { t = t.replace(/\.$/, ''); return t.length > 60 ? t.slice(0, 58) + '…' : t; }

    function snapshot() { return JSON.parse(JSON.stringify({ S: S, k: sel.value })); }
    function paint(x) {
      if (!st) return;
      var s = x.S, fam = V[x.k].fam;
      if (fam === 'pub') {
        st.setText('head', 'Phía phát: lưu đơn và phát sự kiện OrderPlaced');
        st.setNode('n1', { title: 'Order service', sub: s.requestLost ? 'crash: request mất' : 'xử lý request', cls: s.requestLost ? 'dead' : 'client' });
        st.setNode('n2', { title: 'DB', sub: 'đơn: ' + s.orders + (x.k === 'outbox' ? ' · outbox: ' + s.outbox.map(function (r) { return r.sent ? '✓' : '…'; }).join('') : ''), cls: s.orders ? 'ok' : '' });
        st.setNode('n3', { title: 'Relay', sub: x.k === 'outbox' ? 'quét outbox' : 'không dùng', cls: x.k === 'outbox' ? 'client' : 'out' });
        st.setNode('n4', { title: 'Topic orders', sub: s.topic.length ? s.topic.map(function (r) { return r.id; }).join(' ') : 'trống', cls: s.topic.length > 1 ? 'bad' : s.topic.length ? 'ok' : '' });
      } else if (fam === 'con') {
        st.setText('head', 'Phía nhận: cộng điểm từ sự kiện CreditPoints');
        st.setNode('n1', { title: 'Topic points', sub: s.topic.map(function (r, i) { return i + ':' + r.id; }).join(' '), cls: '' });
        st.setNode('n2', { title: 'Consumer', sub: V[x.k].dedup === 'ram' ? 'RAM: {' + s.ram.join(',') + '}' : 'listener', cls: 'client' });
        st.setNode('n3', { title: 'DB', sub: 'điểm: ' + s.points + (V[x.k].dedup === 'db' ? ' · ids: ' + s.eventIds.join(',') : ''), cls: s.points > 10 * uniq(s.topic).length ? 'bad' : s.points ? 'ok' : '' });
        st.setNode('n4', { title: 'Offset đã commit', sub: '= ' + s.committed, cls: 'hot' });
      } else {
        st.setText('head', 'Kafka → Kafka: đọc topic A, ghi topic B, commit offset');
        st.setNode('n1', { title: 'Topic A', sub: 'e1 · committed ' + s.committed, cls: '' });
        st.setNode('n2', { title: 'Ứng dụng', sub: V[x.k].tx ? 'transactional' : 'không transaction', cls: 'client' });
        st.setNode('n3', { title: 'Topic B', sub: s.outB.length ? s.outB.map(function (r) { return r.id + (r.state === 'aborted' ? '✗' : r.state === 'open' ? '…' : ''); }).join(' ') : 'trống', cls: s.outB.length > 1 ? 'hot' : s.outB.length ? 'ok' : '' });
        st.setNode('n4', { title: 'Reader B', sub: iso.value.replace('read_', ''), cls: '' });
      }
    }
    function snap() { var x = snapshot(); if (pl) pl.call(function () { paint(x); }); }
    function ev(t) { S.events.push(t); if (pl) pl.say(t, 650); }

    function exec(s) {
      S.lastSay = null; S.lastTok = null;
      s.apply(S);
      var tok = s.tok || S.lastTok, say = s.say || S.lastSay;
      if (pl && tok && tok[0] !== tok[1]) pl.tok(tok[0], tok[1], tok[2], tok[3]);
      if (pl && tok && tok[0] === tok[1]) pl.pulse(tok[0]);
      snap();
      ev(say);
    }
    function run() {
      fresh(); S.ran = true;
      var k = +crashSel.value, steps = v().plan(S);
      for (var i = 0; i < steps.length; i++) {
        exec(steps[i]);
        if (k && i + 1 === k) {
          S.crashed = true; v().crash(S);
          if (pl) pl.pulse('n2');
          snap();
          ev('CRASH sau bước ' + k + '. ' + v().restartNote);
          var again = v().plan(S);
          if (!again.length) ev('Không còn bước nào được chạy lại.');
          again.forEach(exec);
          break;
        }
      }
      var r = v().verdict(S, iso.value);
      S.verdict = r;
      ev('Kết quả: ' + r.text);
    }

    [['Chạy', run], ['Làm lại', function () { fresh(); }]].forEach(function (p) {
      var b = el('button', null, p[0]); b.type = 'button';
      b.addEventListener('click', function () { if (pl && p[0] === 'Chạy') pl.run(p[1], draw); else { p[1](); draw(); } });
      row2.appendChild(b);
    });
    if (pl) row2.appendChild(pl.controls());
    sel.addEventListener('change', function () { fillCrash(); draw(); });
    [crashSel, iso].forEach(function (x) { x.addEventListener('change', function () { fresh(); draw(); }); });
    dup.addEventListener('change', function () { fillCrash(); draw(); });

    function draw() {
      paint(snapshot());
      var r = S.verdict;
      cap.textContent = r ? '› Đã chạy ' + S.events.length + ' bước' + (S.crashed ? ', có crash và khởi động lại' : '') + '. Kết quả:' : 'Chọn công cụ và điểm crash, đoán kết quả (đúng, mất hay trùng), rồi bấm "Chạy".';
      ledger.className = 'note' + (r && (r.bad || r.warn) ? ' bad' : '');
      ledger.textContent = r ? (r.bad ? '✗ ' : r.warn ? '⚠ ' : '✓ ') + r.text : 'Chưa chạy.';
      evBox.innerHTML = '';
      S.events.forEach(function (t, i) { evBox.appendChild(el('div', null, (i + 1) + '. ' + t)); });
    }
    fillCrash(); draw();
  }
  global.ExactlySim = { mount: mount };
})(window);
