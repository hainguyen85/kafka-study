/*
 * Mô phỏng đường đi của một record làm listener lỗi trong Spring Kafka (Bài 6).
 * Chỉ minh hoạ MÔ HÌNH ĐƯỢC TÀI LIỆU MÔ TẢ, không phải client thật:
 *  - DefaultErrorHandler mặc định FixedBackOff(0L, 9): 1 lần giao + 9 lần thử lại, không chờ; hết lượt thì
 *    recoverer mặc định chỉ log ERROR và commit offset (record bị bỏ qua). [Spring Kafka: Handling exceptions]
 *  - Lỗi "fatal" (ví dụ DeserializationException) không retry, đi thẳng tới recoverer.
 *  - DeadLetterPublishingRecoverer gửi BẢN SAO sang <topic>-dlt, cùng số partition; record gốc vẫn ở topic gốc.
 *  - Blocking retry: thread consumer ngủ theo back-off, cả partition đứng, thứ tự theo key được giữ.
 *  - Non-blocking (@RetryableTopic, Boot mặc định attempts=3, delay 1 giây): record lỗi sang topic -retry, partition
 *    gốc chạy tiếp, thứ tự theo key KHÔNG còn được giữ. [Retry topic]
 * Đơn giản hoá: commit sau mỗi record; consumer của topic -retry xử lý record sau khi partition gốc đã đi thêm một record
 * (thay cho "chờ 1 giây"); một partition 8 record cùng key.
 *
 * Dùng: <div id="esim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/error-sim.js"></script>
 *       <script>ErrorSim.mount(document.getElementById('esim'));</script>
 */
(function (global) {
  var N = 8, BAD = 3, MAX_POLL = 300000;
  var MODES = {
    A: { label: 'A: mặc định, FixedBackOff(0L, 9), không DLT', interval: 0, retries: 9, dlt: false },
    B: { label: 'B: DLT + FixedBackOff(300L, 2L)', interval: 300, retries: 2, dlt: true },
    C: { label: 'C: back-off dài, FixedBackOff(60000L, 9L)', interval: 60000, retries: 9, dlt: true },
    D: { label: 'D: non-blocking, @RetryableTopic (attempts=3, 1 giây)', interval: 1000, retries: 2, dlt: true, nonBlocking: true }
  };
  var ERRS = {
    perm: { label: 'Lỗi vĩnh viễn: dữ liệu sai (IllegalArgumentException)', name: 'POISON' },
    temp: { label: 'Lỗi tạm thời: DB timeout, lần giao thứ 2 thành công', name: 'm3' },
    fatal: { label: 'Lỗi giải mã: DeserializationException (fatal)', name: 'HỎNG' }
  };
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function fmt(ms) { return ms >= 60000 ? (ms / 60000) + ' phút' : ms >= 1000 ? (ms / 1000) + ' giây' : ms + ' ms'; }

  function mount(root) {
    var S;
    var box = el('div', 'sim');
    var row = el('div', 'row'), row1 = el('div', 'row'), row2 = el('div', 'row');
    var mode = el('select'), err = el('select');
    Object.keys(MODES).forEach(function (k) { var o = el('option', null, MODES[k].label); o.value = k; mode.appendChild(o); });
    Object.keys(ERRS).forEach(function (k) { var o = el('option', null, ERRS[k].label); o.value = k; err.appendChild(o); });
    mode.setAttribute('aria-label', 'cấu hình error handler'); err.setAttribute('aria-label', 'loại lỗi của record offset 3');
    row.appendChild(mode); row1.appendChild(err);
    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row1, row2, stageHost, cap, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    function fresh() {
      S = { pos: 0, committed: 0, tries: 0, waited: 0, order: [], status: {}, dlt: [], retryQ: [], log: 0, events: [], sinceRetry: 0 };
    }
    fresh();
    mode.addEventListener('change', function () { fresh(); draw(); });
    err.addEventListener('change', function () { fresh(); draw(); });

    function M() { return MODES[mode.value]; }
    function E() { return ERRS[err.value]; }
    function name(o) { return o === BAD ? E().name : 'm' + o; }

    // ---------- sân khấu ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 300, label: 'Partition 8 record cùng key, listener, error handler, topic DLT, topic retry và offset đã commit.'
    }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    function cellX(o) { return 20 + o * 76; }
    if (st) {
      st.text('head', { x: 20, y: 18, cls: 'strong' }); st.setText('head', 'Topic orders, partition 2 (cùng key acc-1): offset 0..7');
      for (var o = 0; o < N; o++) st.node('o' + o, { x: cellX(o), y: 28, w: 70, h: 48 });
      st.text('com', { x: 20, y: 96, cls: 'strong' });
      st.node('L', { x: 20, y: 122, w: 180, h: 58 });
      st.node('H', { x: 230, y: 122, w: 190, h: 58 });
      st.node('D', { x: 450, y: 122, w: 170, h: 58 });
      st.node('K', { x: 20, y: 222, w: 180, h: 58 });
      st.node('G', { x: 230, y: 222, w: 190, h: 58 });
      st.node('R', { x: 450, y: 222, w: 170, h: 58 });
    }
    function snapshot() {
      return { pos: S.pos, committed: S.committed, tries: S.tries, waited: S.waited, status: JSON.parse(JSON.stringify(S.status)),
        dlt: S.dlt.slice(), retryQ: S.retryQ.map(function (r) { return r.o; }), log: S.log, mode: mode.value };
    }
    function paint(s) {
      if (!st) return;
      var m = MODES[s.mode];
      for (var o = 0; o < N; o++) {
        var x = s.status[o] || '', cls = 'small', sub = '—';
        if (x === 'ok') { cls += ' ok'; sub = '✓ xong'; }
        else if (x === 'late') { cls += ' ok bad'; sub = '✓ muộn'; }
        else if (x === 'skip') { cls += ' bad'; sub = 'bỏ qua'; }
        else if (x === 'dlt') { cls += ' hot'; sub = '→ DLT'; }
        else if (x === 'retry') { cls += ' pending'; sub = '→ -retry'; }
        else if (x === 'fail') { cls += ' pending bad'; sub = 'lỗi ×' + s.tries; }
        else if (o >= s.pos && s.tries && o > s.pos) { sub = 'đang chờ'; }
        st.setNode('o' + o, { title: name(o), sub: sub, cls: cls });
      }
      st.setText('com', '▲ committed ' + s.committed, s.committed < N ? cellX(s.committed) + 4 : cellX(N - 1) + 4);
      st.setNode('L', { title: '@KafkaListener', sub: s.pos < N ? 'kế tiếp: offset ' + s.pos : 'đã hết partition', cls: 'client' });
      var hsub = m.nonBlocking ? 'không chặn partition' : s.tries ? 'đã giao ' + s.tries + '/' + (1 + m.retries) + ' lần' : 'chờ ' + fmt(m.interval) + ' × ' + m.retries;
      st.setNode('H', { title: m.nonBlocking ? '@RetryableTopic' : 'DefaultErrorHandler', sub: hsub, cls: s.tries && !m.nonBlocking ? 'hot' : '' });
      st.setNode('D', { title: 'orders-dlt (P2)', sub: m.dlt ? (s.dlt.length ? 'có: ' + s.dlt.join(', ') : 'trống') : 'không cấu hình', cls: m.dlt ? (s.dlt.length ? 'hot' : '') : 'out' });
      st.setNode('K', { title: 'Offset đã commit', sub: '= ' + s.committed, cls: 'hot' });
      st.setNode('G', { title: 'Log ERROR', sub: s.log ? s.log + ' dòng, không còn gì khác' : 'trống', cls: s.log ? 'bad' : (m.dlt ? 'out' : '') });
      st.setNode('R', { title: 'orders-retry', sub: m.nonBlocking ? (s.retryQ.length ? 'chờ: ' + s.retryQ.map(name).join(', ') : 'trống') : 'không dùng', cls: m.nonBlocking ? (s.retryQ.length ? 'pending' : '') : 'out' });
    }
    function snap() { var s = snapshot(); if (pl) pl.call(function () { paint(s); }); }
    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 7); if (pl) pl.say(t); }

    function fails(o, attempt) {
      if (o !== BAD) return false;
      if (err.value === 'temp') return attempt < 2;
      return true;
    }
    function maxDeliveries() { return err.value === 'fatal' ? 1 : 1 + M().retries; }

    function commitTo(o) { S.committed = o + 1; }

    function recover(o, from) {
      var m = M();
      if (m.dlt) {
        if (pl) pl.tok(from, 'D', 'bản sao ' + name(o), 'err');
        S.dlt.push(name(o)); S.status[o] = 'dlt';
        ev('Recoverer gửi BẢN SAO của ' + name(o) + ' sang orders-dlt, cùng partition 2, kèm header lỗi. Bản gốc vẫn ở offset ' + o + '.');
      } else {
        if (pl) pl.tok(from, 'G', 'ERROR', 'err');
        S.log++; S.status[o] = 'skip';
        ev('Hết lượt: recoverer mặc định chỉ log ERROR. ' + name(o) + ' bị bỏ qua im lặng, không có ở DLT nào.');
      }
    }

    function stepMain() {
      var m = M(), o = S.pos;
      if (pl) pl.tok('o' + o, 'L', name(o), 'msg');
      var attempt = S.tries + 1;
      if (!fails(o, attempt)) {
        S.status[o] = 'ok'; S.order.push(name(o));
        if (S.tries) ev('Lần giao thứ ' + attempt + ': ' + name(o) + ' thành công. Partition chạy tiếp, thứ tự được giữ.');
        else ev('Xử lý xong ' + name(o) + '.');
        S.tries = 0; commitTo(o); S.pos++;
        if (S.retryQ.length) S.sinceRetry++;
        snap(); return;
      }
      // listener ném lỗi
      S.tries = attempt; S.status[o] = 'fail';
      if (pl) pl.tok('L', 'H', 'lỗi', 'err');
      snap();
      if (m.nonBlocking) {
        if (err.value === 'fatal') { S.tries = 0; recover(o, 'L'); commitTo(o); S.pos++; snap(); return; }
        if (pl) pl.tok('H', 'R', name(o), 'fetch');
        S.retryQ.push({ o: o, attempt: 1 }); S.status[o] = 'retry'; S.tries = 0; S.sinceRetry = 0;
        commitTo(o); S.pos++;
        ev(name(o) + ' lỗi: chuyển sang orders-retry, commit, partition gốc chạy tiếp ngay (không ai chờ ' + name(o) + ').');
        snap(); return;
      }
      if (err.value === 'fatal') {
        ev('DeserializationException là lỗi fatal: không retry, đi thẳng tới recoverer.');
        S.tries = 0; recover(o, 'H'); commitTo(o); S.pos++; snap(); return;
      }
      if (attempt < maxDeliveries()) {
        S.waited += m.interval;
        if (pl) pl.pulse('H');
        var warn = S.waited > MAX_POLL ? ' Đã chờ ' + fmt(S.waited) + ' > max.poll.interval.ms (5 phút): nguy cơ bị đá khỏi group, rebalance (Bài 5b).' : '';
        ev('Lần giao ' + attempt + '/' + maxDeliveries() + ' lỗi. Chờ ' + fmt(m.interval) + ', seek lại offset ' + o + '. Các record sau đứng chờ.' + warn);
        if (pl) pl.tok('H', 'L', 'seek ' + o, 'fetch');
        snap(); return;
      }
      ev('Lần giao ' + attempt + '/' + maxDeliveries() + ' cũng lỗi: hết lượt.');
      S.tries = 0; recover(o, 'H'); commitTo(o); S.pos++; snap();
    }

    function stepRetry() {
      var r = S.retryQ[0], o = r.o;
      r.attempt++;
      if (pl) pl.tok('R', 'L', name(o), 'fetch');
      if (!fails(o, r.attempt)) {
        S.retryQ.shift(); S.order.push(name(o)); S.status[o] = 'late';
        ev('Consumer của orders-retry xử lý xong ' + name(o) + ', nhưng SAU ' + S.order.slice(-2, -1).join('') + ': cùng key mà thứ tự đã đảo.');
        snap(); return;
      }
      if (r.attempt < 3) {
        ev('Lần giao ' + r.attempt + '/3 của ' + name(o) + ' (từ orders-retry) lỗi, chờ lượt sau.');
        S.sinceRetry = 0; snap(); return;
      }
      S.retryQ.shift();
      ev('Lần giao 3/3 của ' + name(o) + ' lỗi: hết lượt.');
      recover(o, 'R'); snap();
    }

    function step() {
      if (S.retryQ.length && (S.sinceRetry >= 1 || S.pos >= N)) { stepRetry(); return; }
      if (S.pos >= N) { ev('Hết partition. Xem dòng tổng kết bên dưới.'); return; }
      stepMain();
    }
    function runAll() {
      var guard = 0;
      while ((S.pos < N || S.retryQ.length) && guard++ < 60) step();
    }

    [['Giao record kế tiếp', step], ['Chạy tới hết', runAll], ['Làm lại', fresh]].forEach(function (p) {
      var b = el('button', null, p[0]); b.type = 'button';
      b.addEventListener('click', function () {
        if (pl && p[1] !== fresh) pl.run(p[1], draw); else { p[1](); draw(); }
      });
      row2.appendChild(b);
    });
    if (pl) row2.appendChild(pl.controls());

    function draw() {
      paint(snapshot());
      cap.textContent = S.events[0] ? '› ' + S.events[0] : 'Chọn cấu hình và loại lỗi, đoán trước kết quả, rồi bấm "Giao record kế tiếp".';
      var m = M(), o = BAD, st3 = S.status[o];
      var lost = st3 === 'skip', late = st3 === 'late';
      ledger.className = 'note' + (lost || late || S.waited > MAX_POLL ? ' bad' : '');
      ledger.textContent = 'Thứ tự đã xử lý: ' + (S.order.length ? S.order.join(' ') : '(chưa có)') +
        ' · Partition đứng chờ: ' + (m.nonBlocking ? 'không' : fmt(S.waited)) +
        ' · DLT: ' + (S.dlt.length ? S.dlt.join(', ') : 'trống') +
        (lost ? ' · ' + name(o) + ' MẤT IM LẶNG (chỉ còn log)' : '') +
        (late ? ' · VỠ THỨ TỰ theo key: m4 xử lý trước ' + name(o) : '') +
        (st3 === 'skip' || st3 === 'dlt' ? ' · Record gốc ' + name(o) + ' vẫn nằm ở offset ' + o + ' của topic orders; chỉ offset commit đi qua nó.' : '');
      evBox.innerHTML = '';
      S.events.slice(1).forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    draw();
  }
  global.ErrorSim = { mount: mount };
})(window);
