/*
 * Mô phỏng tổng hợp: một sự kiện chuyển tiền đi từ producer, qua ba broker, tới consumer và DB.
 * Gộp Bài 4 (acks, ISR, min.insync.replicas), 4b (idempotent producer), 5 (commit offset), 5b (rebalance),
 * 6 (message lỗi, DLT), 7 (idempotent consumer). Chỉ minh hoạ MÔ HÌNH, không phải client thật.
 *
 * Mô hình:
 *  - Một partition, RF=3 trên B1 (leader), B2, B3. Follower fetch được gom thành một bước.
 *  - acks=1: leader ack ngay sau khi ghi; acks=all: chờ mọi replica trong ISR. min.insync.replicas chỉ có tác dụng
 *    với acks=all. Idempotence cần acks=all; đặt acks=1 mà không bật tường minh thì idempotence tự tắt.
 *    [Producer configs: acks, enable.idempotence] [Broker configs: min.insync.replicas, unclean.leader.election.enable]
 *  - Consumer chỉ thấy record đã có ở mọi replica trong ISR (high watermark).
 *  - Idempotence chỉ phủ retry trong một session (cùng PID); producer restart là PID mới.
 *  - Service phát dùng outbox nên sau crash vẫn gửi lại sự kiện chưa đánh dấu (Bài 7).
 *  - Consumer: "commit trước" = auto-commit trong khi xử lý ở thread khác; "commit sau" = Spring mặc định.
 *    Crash hay rebalance giữa lúc xử lý: lấy trường hợp xấu nhất của từng cách commit.
 *  - Message lỗi: DefaultErrorHandler giao tối đa 10 lần, rồi log và commit (mặc định) hoặc gửi DLT.
 * Đơn giản hoá: sau sự cố ở broker, B1 quay lại và lại làm leader (auto.leader.rebalance.enable) trước sự kiện kế tiếp.
 *
 * Dùng: <div id="esim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/e2e-sim.js"></script>
 *       <script>E2ESim.mount(document.getElementById('esim'), { mode: 'one' });</script>   // mode: 'one' | 'day'
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  var PRESETS = {
    def: { label: 'Mặc định (Kafka 4.x + Spring Boot)', acks: 'all', minIsr: 1, idem: true, commit: 'after', dedup: false, dlt: false },
    bank: { label: 'Cấu hình cho tiền (Bài 4, 4b, 5, 6, 7)', acks: 'all', minIsr: 2, idem: true, commit: 'after', dedup: true, dlt: true },
    fast: { label: 'Cấu hình "cho nhanh"', acks: '1', minIsr: 1, idem: false, commit: 'before', dedup: false, dlt: false },
    custom: { label: 'Tự chọn' }
  };
  var FAULTS = {
    none: { label: 'Không có sự cố', short: 'không' },
    leader: { label: 'Leader B1 chết ngay sau khi ghi, follower chưa kịp fetch', short: 'leader chết', fix: 'acks=all (Bài 4)' },
    disk: { label: 'B2, B3 tụt khỏi ISR, rồi đĩa B1 hỏng hẳn', short: 'ISR co, đĩa hỏng', fix: 'acks=all + min.insync.replicas=2 (Bài 4)' },
    ackLost: { label: 'Ack bị mất trên đường về, producer retry', short: 'ack mất', fix: 'enable.idempotence=true (Bài 4b) hoặc chống trùng ở consumer (Bài 7)' },
    pCrash: { label: 'Producer crash trước khi nhận ack, restart rồi gửi lại', short: 'producer crash', fix: 'chống trùng theo eventId ở consumer (Bài 4b, 7)' },
    cCrash: { label: 'Consumer crash giữa lúc xử lý', short: 'consumer crash', fixBefore: 'commit sau khi xử lý (Bài 5)', fixAfter: 'chống trùng theo eventId (Bài 7)' },
    rebal: { label: 'Deploy consumer: rebalance giữa lúc xử lý', short: 'rebalance', fixBefore: 'commit sau khi xử lý (Bài 5, 5b)', fixAfter: 'chống trùng theo eventId (Bài 5b, 7)' },
    poison: { label: 'Message sai dữ liệu (amount = null)', short: 'message lỗi', fix: 'DLT + cảnh báo (Bài 6)' }
  };
  // Một ngày ở production: mỗi sự kiện gặp một sự cố
  var DAY = ['none', 'ackLost', 'leader', 'pCrash', 'disk', 'cCrash', 'poison', 'rebal'];

  function mount(root, opts) {
    opts = opts || {};
    var mode = opts.mode === 'day' ? 'day' : 'one';
    var S;
    var box = el('div', 'sim');
    var rowP = el('div', 'row'), rowK = el('div', 'row'), rowK2 = el('div', 'row'), rowF = el('div', 'row'), rowB = el('div', 'row');

    function select(aria, list) {
      var s = el('select'); s.setAttribute('aria-label', aria);
      list.forEach(function (o) { var x = el('option', null, o[1]); x.value = o[0]; s.appendChild(x); });
      return s;
    }
    function check(text) { var l = el('label'), b = el('input'); b.type = 'checkbox'; l.appendChild(b); l.appendChild(document.createTextNode(' ' + text)); return { lab: l, box: b }; }

    var preset = select('cấu hình mẫu', Object.keys(PRESETS).map(function (k) { return [k, PRESETS[k].label]; }));
    var acks = select('acks', [['all', 'acks=all'], ['1', 'acks=1']]);
    var minIsr = select('min.insync.replicas', [['1', 'min.insync.replicas=1'], ['2', 'min.insync.replicas=2']]);
    var idem = check('enable.idempotence');
    var commit = select('lúc commit offset', [['after', 'Commit sau khi xử lý (Spring mặc định)'], ['before', 'Commit trước khi xử lý xong (auto-commit, xử lý ở thread khác)']]);
    var dedup = check('Chống trùng: UNIQUE(eventId)');
    var dlt = check('DLT cho message lỗi');
    var fault = select('sự cố', Object.keys(FAULTS).filter(function (k) { return k !== 'none'; }).map(function (k) { return [k, FAULTS[k].label]; }));
    rowP.appendChild(preset);
    [acks, minIsr, idem.lab].forEach(function (x) { rowK.appendChild(x); });
    [commit, dedup.lab, dlt.lab].forEach(function (x) { rowK2.appendChild(x); });
    if (mode === 'one') { rowF.appendChild(document.createTextNode('Sự cố ở e2: ')); rowF.appendChild(fault); }
    else rowF.appendChild(el('span', null, 'Tám sự kiện: e1 bình thường, e2 tới e8 mỗi sự kiện gặp một sự cố khác nhau (xem cột "Sự cố").'));

    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note'), table = el('div');
    table.style.cssText = 'overflow-x:auto';
    var det = el('details'), sum = el('summary', null, 'Nhật ký từng bước'), evBox = el('div');
    det.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    det.appendChild(sum); det.appendChild(evBox);
    [rowP, rowK, rowK2, rowF, rowB, stageHost, cap, table, ledger, det].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var st = FlowAnim.stage(stageHost, { width: 640, height: 200, rail: true, label: 'Producer, ba broker B1 B2 B3, consumer, DB và topic DLT.' });
    var pl = FlowAnim.player(box, st, cap);
    st.node('P', { x: 10, y: 72, w: 125, h: 58 });
    st.node('B1', { x: 185, y: 8, w: 135, h: 54 });
    st.node('B2', { x: 185, y: 73, w: 135, h: 54 });
    st.node('B3', { x: 185, y: 138, w: 135, h: 54 });
    st.node('C', { x: 370, y: 72, w: 125, h: 58 });
    st.node('DB', { x: 520, y: 30, w: 115, h: 54 });
    st.node('DLT', { x: 520, y: 116, w: 115, h: 54 });

    function cfg() {
      return { acks: acks.value, minIsr: +minIsr.value, idem: idem.box.checked && acks.value === 'all', commit: commit.value, dedup: dedup.box.checked, dlt: dlt.box.checked };
    }
    function applyPreset() {
      var p = PRESETS[preset.value]; if (!p.acks) return;
      acks.value = p.acks; minIsr.value = String(p.minIsr); idem.box.checked = p.idem; commit.value = p.commit; dedup.box.checked = p.dedup; dlt.box.checked = p.dlt;
    }
    function syncIdem() {
      var off = acks.value !== 'all';
      if (off) idem.box.checked = false;
      idem.box.disabled = off;
      idem.lab.title = off ? 'Idempotence cần acks=all: đặt acks=1 thì idempotence tự tắt (bật tường minh thì ConfigException).' : '';
      minIsr.title = off ? 'min.insync.replicas chỉ có tác dụng khi acks=all.' : '';
    }
    function plan() {
      if (mode === 'day') return DAY.map(function (f, i) { return { id: 'e' + (i + 1), fault: f }; });
      return [{ id: 'e1', fault: 'none' }, { id: 'e2', fault: fault.value }, { id: 'e3', fault: 'none' }];
    }

    function fresh() {
      S = { log: [], br: { B1: 'leader', B2: 'f', B3: 'f' }, pDead: false, cDead: false, pid: 1, committed: 0, db: {}, dlt: [], skipped: [], lost: {}, events: [], rows: null, cNote: '' };
    }

    // ---------- vẽ ----------
    function ids(list) {
      var a = list.map(function (r) { return r.id; });
      return a.length > 5 ? '… ' + a.slice(-5).join(' ') : a.length ? a.join(' ') : 'trống';
    }
    function snapshot() { return JSON.parse(JSON.stringify({ S: S, c: cfg() })); }
    function paint(x) {
      var s = x.S, c = x.c;
      st.setNode('P', { title: 'Producer', sub: s.pDead ? 'crash' : 'PID ' + s.pid + ' · acks=' + c.acks, cls: s.pDead ? 'dead' : 'client' });
      ['B1', 'B2', 'B3'].forEach(function (b) {
        var state = s.br[b];
        var has = s.log.filter(function (r) { return r.reps.indexOf(b) >= 0; });
        st.setNode(b, {
          title: b + (state === 'leader' ? ' (leader)' : state === 'out' ? ' (ngoài ISR)' : state === 'dead' ? ' (chết)' : ''),
          sub: state === 'dead' ? '—' : ids(has),
          cls: state === 'leader' ? 'leader' : state === 'out' ? 'out' : state === 'dead' ? 'dead' : ''
        });
      });
      st.setNode('C', { title: 'Consumer', sub: s.cDead ? (s.cNote || 'crash') : 'đã commit: ' + s.committed, cls: s.cDead ? 'dead' : 'client' });
      var keys = Object.keys(s.db), dup = keys.some(function (k) { return s.db[k] > 1; });
      var dbIds = keys.map(function (k) { return k + (s.db[k] > 1 ? '×' + s.db[k] : ''); });
      st.setNode('DB', { title: 'DB', sub: !keys.length ? 'trống' : dbIds.length > 4 ? '… ' + dbIds.slice(-3).join(' ') : dbIds.join(' '), cls: dup ? 'bad' : keys.length ? 'ok' : '' });
      st.setNode('DLT', { title: 'payments-dlt', sub: s.dlt.length ? s.dlt.join(' ') : 'trống', cls: s.dlt.length ? 'hot' : 'out' });
    }
    function snap() { var x = snapshot(); pl.call(function () { paint(x); }); }
    function say(t, ms) { S.events.push(t); pl.say(t, ms || 900); }
    function tok(a, b, l, k, o) { pl.tok(a, b, l, k, o); }

    // ---------- phía ghi ----------
    function write(id, poison, c, quiet) {   // ghi bình thường, ISR đầy đủ
      var rec = { id: id, poison: poison, reps: ['B1'] };
      tok('P', 'B1', id, 'msg'); S.log.push(rec); snap();
      if (c.acks === '1') {
        tok('B1', 'P', 'ack', 'ack');
        pl.par([['B1', 'B2', id, 'fetch'], ['B1', 'B3', id, 'fetch']]); rec.reps = ['B1', 'B2', 'B3']; snap();
        if (!quiet) say(id + ': B1 ghi rồi ack ngay (acks=1); B2, B3 fetch sau.', 600);
      } else {
        pl.par([['B1', 'B2', id, 'fetch'], ['B1', 'B3', id, 'fetch']]); rec.reps = ['B1', 'B2', 'B3']; snap();
        tok('B1', 'P', 'ack', 'ack');
        if (!quiet) say(id + ': B2, B3 fetch xong, B1 ack (acks=all).', 600);
      }
      return rec;
    }
    function restoreB1(text) {
      S.br = { B1: 'leader', B2: 'f', B3: 'f' };
      S.log.forEach(function (r) { r.reps = ['B1', 'B2', 'B3']; });
      pl.pulse('B1'); snap();
      say(text || 'B1 quay lại, chép dữ liệu từ leader mới, theo kịp và lại làm leader (auto.leader.rebalance.enable).', 800);
    }
    function lose(rec, why) {
      S.log.splice(S.log.indexOf(rec), 1);
      S.lost[rec.id] = why;
    }
    function produce(ev, c) {
      var id = ev.id, f = ev.fault, poison = f === 'poison';
      if (f === 'leader') {
        var rec = { id: id, poison: false, reps: ['B1'] };
        tok('P', 'B1', id, 'msg'); S.log.push(rec); snap();
        if (c.acks === '1') {
          tok('B1', 'P', 'ack', 'ack');
          say('acks=1: B1 ghi ' + id + ' vào log của mình rồi ack ngay. Producer coi như đã gửi xong.');
          S.br.B1 = 'dead'; S.br.B2 = 'leader'; pl.pulse('B1'); snap();
          lose(rec, 'producer đã nhận ack, nhưng chỉ leader cũ có bản ghi');
          snap();
          say('B1 chết trước khi B2, B3 kịp fetch. B2 (trong ISR) lên làm leader và không có ' + id + '. Consumer chưa từng thấy ' + id + ' vì nó chưa có ở mọi replica trong ISR.', 1400);
          restoreB1('B1 khởi động lại, cắt log theo leader mới (' + id + ' biến mất), theo kịp rồi lại làm leader.');
        } else {
          pl.par([['B1', 'B2', id, 'fetch'], ['B1', 'B3', id, 'fetch']]); rec.reps = ['B1', 'B2', 'B3']; snap();
          tok('B1', 'P', 'ack', 'ack');
          say('acks=all: B1 chờ B2, B3 fetch xong mới ack.');
          S.br.B1 = 'dead'; S.br.B2 = 'leader'; pl.pulse('B1'); snap();
          say('B1 chết. B2 lên làm leader và đã có ' + id + ': không mất.', 1000);
          restoreB1();
        }
        return;
      }
      if (f === 'disk') {
        S.br.B2 = 'out'; S.br.B3 = 'out'; snap();
        say('B2, B3 chậm quá replica.lag.time.max.ms nên bị loại khỏi ISR. ISR chỉ còn B1.', 1100);
        if (c.acks === 'all' && c.minIsr >= 2) {
          tok('P', 'B1', id, 'err', { bounce: true });
          say('ISR có 1 replica, nhỏ hơn min.insync.replicas=2: broker từ chối (NotEnoughReplicas). Producer chờ rồi retry.', 1300);
          pl.par([['B1', 'B2', 'fetch', 'fetch'], ['B1', 'B3', 'fetch', 'fetch']]); S.br.B2 = 'f'; S.br.B3 = 'f'; snap();
          say('B2, B3 theo kịp và vào lại ISR.', 700);
          write(id, false, c, true);
          say('Retry thành công: ' + id + ' có ở cả ba broker rồi mới ack.', 700);
          S.br.B1 = 'dead'; S.br.B2 = 'leader'; pl.pulse('B1'); snap();
          say('Đĩa B1 hỏng hẳn. B2 lên làm leader và đã có ' + id + ': không mất.', 1000);
          restoreB1('Thay đĩa, B1 chép lại toàn bộ dữ liệu từ leader mới rồi lại làm leader.');
        } else {
          var r2 = { id: id, poison: false, reps: ['B1'] };
          tok('P', 'B1', id, 'msg'); S.log.push(r2); snap();
          tok('B1', 'P', 'ack', 'ack');
          say(c.acks === '1' ? 'acks=1 không xét ISR: B1 ack khi ' + id + ' mới có 1 bản sao.' : 'acks=all nhưng min.insync.replicas=1: ISR chỉ có B1 nên B1 ack khi ' + id + ' mới có 1 bản sao.', 1300);
          S.br.B1 = 'dead'; pl.pulse('B1'); snap();
          lose(r2, 'producer đã nhận ack, nhưng bản sao duy nhất nằm trên đĩa B1 đã hỏng');
          snap();
          say('Đĩa B1 hỏng hẳn. B2, B3 không có ' + id + ' và không thuộc ISR: partition offline tới khi bầu leader "unclean". Bầu xong thì ' + id + ' không còn ở đâu.', 1600);
          restoreB1('Bầu B2 làm leader (unclean), thay đĩa B1, B1 chép lại dữ liệu rồi lại làm leader.');
        }
        return;
      }
      if (f === 'ackLost') {
        write(id, false, c, true);
        tok('B1', 'P', 'ack', 'ack', { half: true });
        say('Broker đã ghi ' + id + ', nhưng ack bị mất trên đường về. Hết request.timeout.ms, producer gửi lại.', 1100);
        tok('P', 'B1', id + ' retry', 'msg');
        if (c.idem) {
          tok('B1', 'P', 'ack', 'ack');
          say('Cùng PID ' + S.pid + ', cùng sequence: broker nhận ra bản lặp, không ghi thêm, chỉ ack lại.', 1100);
        } else {
          var dupRec = { id: id, poison: false, reps: ['B1'] }; S.log.push(dupRec); snap();
          pl.par([['B1', 'B2', id, 'fetch'], ['B1', 'B3', id, 'fetch']]); dupRec.reps = ['B1', 'B2', 'B3']; snap();
          tok('B1', 'P', 'ack', 'ack');
          say('Idempotence ' + (c.acks === '1' ? 'đã tự tắt vì acks=1' : 'tắt') + ': broker ghi thêm một bản ' + id + '. Log có 2 bản.', 1100);
        }
        return;
      }
      if (f === 'pCrash') {
        tok('P', 'B1', id, 'msg');
        var r3 = { id: id, poison: false, reps: ['B1'] }; S.log.push(r3); snap();
        pl.par([['B1', 'B2', id, 'fetch'], ['B1', 'B3', id, 'fetch']]); r3.reps = ['B1', 'B2', 'B3'];
        S.pDead = true; pl.pulse('P'); snap();
        tok('B1', 'P', 'ack', 'ack', { half: true });
        say('Producer crash trước khi nhận ack. Broker đã ghi ' + id + '.', 900);
        S.pDead = false; S.pid++; snap();
        say('Service khởi động lại với PID mới ' + S.pid + '. Dòng outbox của ' + id + ' chưa được đánh dấu đã gửi, nên gửi lại.', 1200);
        write(id, false, c, true);
        say('Broker không có sequence nào của PID ' + S.pid + ' để so, nên ghi thêm bản thứ hai' + (c.idem ? ', dù bật idempotence.' : '.'), 1200);
        return;
      }
      write(id, poison, c, false);
    }

    // ---------- phía đọc ----------
    function processRec(rec, c) {
      if (rec.poison) {
        pl.pulse('C');
        say('Listener ném lỗi vì amount = null. DefaultErrorHandler giao lại, tổng cộng 10 lần, vẫn lỗi.', 1100);
        if (c.dlt) {
          tok('C', 'DLT', rec.id, 'err'); S.dlt.push(rec.id); snap();
          say('Recoverer gửi ' + rec.id + ' sang payments-dlt; cảnh báo báo cho người trực.', 900);
        } else {
          S.skipped.push(rec.id);
          say('Hết lượt: Spring log ERROR rồi commit offset. ' + rec.id + ' bị bỏ qua, không ai được báo.', 1100);
        }
        return;
      }
      if (c.dedup && S.db[rec.id]) {
        tok('C', 'DB', 'trùng', 'err');
        say('INSERT eventId ' + rec.id + ' vi phạm UNIQUE: transaction DB rollback, bỏ qua.', 900);
        return;
      }
      tok('C', 'DB', '+' + rec.id, 'msg');
      S.db[rec.id] = (S.db[rec.id] || 0) + 1; snap();
      if (S.db[rec.id] > 1) say('Ghi ' + rec.id + ' vào DB lần thứ ' + S.db[rec.id] + ': trùng.', 900);
    }
    function commitTo(o) { tok('C', 'B1', 'commit ' + o, 'ack'); S.committed = o; snap(); }
    function crashC(f, text) {
      S.cDead = true; S.cNote = f === 'rebal' ? 'rebalance' : 'crash'; pl.pulse('C'); snap();
      say(text, 1400);
      S.cDead = false; S.cNote = ''; snap();
    }
    function consume(ev, c) {
      var hit = ev.fault === 'cCrash' || ev.fault === 'rebal', done = false;
      var who = ev.fault === 'rebal' ? 'Deploy: partition chuyển sang pod mới' : 'Consumer crash';
      for (var o = S.committed; o < S.log.length; o++) {
        var rec = S.log[o], here = hit && !done && rec.id === ev.id;
        tok('B1', 'C', rec.id, 'msg');
        if (c.commit === 'before') {
          commitTo(o + 1);
          if (here) {
            done = true;
            S.lost[rec.id] = 'offset đã commit trước khi ghi DB';
            crashC(ev.fault, who + ' sau khi commit, trước khi ghi DB. Đọc tiếp từ offset đã commit ' + (o + 1) + ': ' + rec.id + ' bị bỏ qua.');
            continue;
          }
          processRec(rec, c);
        } else {
          processRec(rec, c);
          if (here) {
            done = true;
            crashC(ev.fault, who + ' sau khi ghi DB, trước khi commit. Đọc lại từ offset đã commit ' + o + ': ' + rec.id + ' được giao lại.');
            o--; continue;
          }
          commitTo(o + 1);
        }
      }
    }

    // ---------- kết quả ----------
    function finish(events, c) {
      S.rows = events.map(function (ev) {
        var id = ev.id, F = FAULTS[ev.fault];
        var copies = S.log.filter(function (r) { return r.id === id; }).length;
        var n = S.db[id] || 0, out, bad = false;
        if (ev.fault === 'poison') {
          if (S.dlt.indexOf(id) >= 0) out = '✓ vào DLT, có cảnh báo';
          else { out = '✗ mất im lặng (chỉ có log ERROR)'; bad = true; }
        } else if (n === 1) out = '✓ đúng một lần';
        else if (n > 1) { out = '✗ trùng: ghi DB ' + n + ' lần'; bad = true; }
        else { out = '✗ mất: ' + (S.lost[id] || 'không tới DB'); bad = true; }
        var fix = bad ? (F.fix || (c.commit === 'before' ? F.fixBefore : F.fixAfter)) : '';
        return { id: id, fault: F.short, copies: copies, db: n, out: out, bad: bad, fix: fix };
      });
    }
    function run() {
      fresh();
      var c = cfg(), events = plan();
      snap();
      events.forEach(function (ev) {
        if (ev.fault !== 'none') say('— ' + ev.id + ': ' + FAULTS[ev.fault].label + '.', 900);
        produce(ev, c);
        consume(ev, c);
      });
      finish(events, c);
    }

    function draw() {
      paint(snapshot());
      table.innerHTML = '';
      if (!S.rows) {
        cap.textContent = mode === 'day'
          ? 'Chọn cấu hình, đoán xem sự kiện nào mất, trùng hay đúng, rồi bấm "Chạy cả ngày".'
          : 'Chọn cấu hình và sự cố, đoán kết quả của e2 (đúng, mất hay trùng), rồi bấm "Chạy".';
        ledger.className = 'note'; ledger.textContent = 'Chưa chạy.';
        evBox.innerHTML = '';
        return;
      }
      var t = el('table'), h = el('tr');
      ['Sự kiện', 'Sự cố', 'Bản trong log', 'Ghi DB', 'Kết quả'].forEach(function (x) { h.appendChild(el('th', null, x)); });
      t.appendChild(h);
      S.rows.forEach(function (r) {
        var tr = el('tr');
        [r.id, r.fault, String(r.copies), String(r.db), r.out].forEach(function (x, i) {
          var td = el('td', null, x);
          if (i === 4 && r.bad) td.style.color = 'var(--bad)';
          tr.appendChild(td);
        });
        t.appendChild(tr);
      });
      table.appendChild(t);
      var bad = S.rows.filter(function (r) { return r.bad; });
      var lost = bad.filter(function (r) { return r.out.indexOf('✗ m') === 0; }).length, dupN = bad.length - lost;
      cap.textContent = '› Xong ' + S.rows.length + ' sự kiện.';
      ledger.className = 'note' + (bad.length ? ' bad' : '');
      ledger.textContent = bad.length
        ? '✗ ' + lost + ' mất, ' + dupN + ' trùng. Sửa: ' + bad.map(function (r) { return r.id + ' → ' + r.fix; }).join('; ') + '.'
        : '✓ Không mất, không trùng: mọi sự kiện vào DB đúng một lần hoặc nằm trong DLT có cảnh báo.';
      evBox.innerHTML = '';
      S.events.forEach(function (x, i) { evBox.appendChild(el('div', null, (i + 1) + '. ' + x)); });
    }

    var go = el('button', null, mode === 'day' ? 'Chạy cả ngày' : 'Chạy'); go.type = 'button';
    go.addEventListener('click', function () { pl.run(run, function () { draw(); syncIdem(); }); });
    var reset = el('button', null, 'Làm lại'); reset.type = 'button';
    reset.addEventListener('click', function () { fresh(); draw(); });
    rowB.appendChild(go); rowB.appendChild(reset); rowB.appendChild(pl.controls());

    preset.addEventListener('change', function () { applyPreset(); syncIdem(); fresh(); draw(); });
    [acks, minIsr, commit, idem.box, dedup.box, dlt.box].forEach(function (x) {
      x.addEventListener('change', function () { preset.value = 'custom'; syncIdem(); fresh(); draw(); });
    });
    fault.addEventListener('change', function () { fresh(); draw(); });

    preset.value = opts.preset || 'def'; applyPreset(); syncIdem();
    fresh(); draw();
  }
  global.E2ESim = { mount: mount };
})(window);
