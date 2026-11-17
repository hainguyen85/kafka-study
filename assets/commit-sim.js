/*
 * Mô phỏng commit offset của một consumer đọc một partition có 9 record (offset 0..8).
 * Chỉ để minh hoạ MÔ HÌNH ĐƯỢC TÀI LIỆU MÔ TẢ (consumer javadoc: auto-commit xảy ra trong poll(); commit là
 * offset của record KẾ TIẾP cần đọc), không phải client thật. Batch cố định 3 record (như max.poll.records=3).
 *
 * Ba chế độ:
 *  A. auto-commit, xử lý ngay trong thread consumer: poll() kế tiếp mới commit lô trước (và chỉ khi đã xử lý xong lô).
 *  B. auto-commit, giao việc cho worker rồi return: poll() kế tiếp commit lô trước dù worker chưa làm xong.
 *  C. ack thủ công sau khi xử lý từng record: commit đúng record đã xử lý xong.
 * "Crash": process chết, chạy lại từ offset đã commit. Worker queue mất.
 *
 * Hình động (assets/flow-anim.js, nạp trước file này nếu có): record bay từ partition về consumer khi poll(),
 * sang worker ở chế độ B, commit bay xuống ô "offset đã commit".
 *
 * Dùng: <div id="sim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/commit-sim.js"></script>
 *       <script>CommitSim.mount(document.getElementById('sim'));</script>
 */
(function (global) {
  var N = 9, BATCH = 3;
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root) {
    var S;
    function fresh() { S = { committed: 0, pos: 0, batch: [], todo: [], done: {}, events: [], crashes: 0 }; }
    fresh();
    var box = el('div', 'sim');
    var row = el('div', 'row');
    var mode = el('select');
    [['A', 'A: auto-commit, xử lý trong thread consumer'], ['B', 'B: auto-commit, giao việc cho worker'], ['C', 'C: ack thủ công sau khi xử lý']].forEach(function (o) {
      var x = el('option', null, o[1]); x.value = o[0]; mode.appendChild(x);
    });
    mode.addEventListener('change', function () { fresh(); draw(); });
    row.appendChild(mode);
    var row2 = el('div', 'row');
    var stageHost = el('div'), cap = el('div', 'fa-cap');
    var grid = el('div', 'parts');
    var ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row2, stageHost, cap, grid, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    // ---------- sân khấu ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 300, label: 'Partition 9 record, consumer, worker và offset đã commit.'
    }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    if (pl) row.appendChild(pl.controls());
    function cellX(o) { return 20 + o * 68; }
    if (st) {
      st.text('head', { x: 20, y: 18, cls: 'strong' }); st.setText('head', 'Partition: offset 0..8');
      for (var o = 0; o < N; o++) st.node('o' + o, { x: cellX(o), y: 28, w: 60, h: 48 });
      st.text('pos', { x: 20, y: 96, cls: 'strong' });
      st.text('com', { x: 20, y: 114, cls: 'strong' });
      st.node('C', { x: 20, y: 150, w: 200, h: 58 });
      st.node('W', { x: 250, y: 150, w: 200, h: 58 });
      st.node('K', { x: 20, y: 236, w: 200, h: 58 });
    }
    function snapshot() {
      return { committed: S.committed, pos: S.pos, todo: S.todo.slice(), batch: S.batch.slice(), done: JSON.parse(JSON.stringify(S.done)), mode: mode.value };
    }
    function paint(s) {
      if (!st) return;
      for (var o = 0; o < N; o++) {
        var n = s.done[o] || 0, cls = 'small', sub = '—';
        if (n > 1) { cls += ' bad'; sub = 'xử lý ' + n + ' lần'; }
        // đang chờ mà offset đã commit vượt qua: viền đỏ nét đứt, crash lúc này là mất
        else if (s.todo.indexOf(o) >= 0) { cls += o < s.committed ? ' pending bad' : ' pending'; sub = o < s.committed ? 'chờ ⚠' : 'chờ'; }
        else if (o < s.committed && !n) { cls += ' bad'; sub = 'MẤT'; }
        else if (n === 1) { cls += ' ok'; sub = '✓ xong'; }
        st.setNode('o' + o, { title: 'off ' + o, sub: sub, cls: cls });
      }
      var px = s.pos < N ? cellX(s.pos) + 4 : cellX(N - 1) + 40;
      st.setText('pos', '▲ đọc tới ' + s.pos, px);
      st.setText('com', '▲ committed ' + s.committed, s.committed < N ? cellX(s.committed) + 4 : cellX(N - 1) + 30);
      var inThread = s.mode !== 'B' ? s.todo : [];
      st.setNode('C', { title: 'Consumer (thread poll)', sub: inThread.length ? 'đang giữ: ' + inThread.join(', ') : 'rảnh', cls: 'client' });
      st.setNode('W', { title: 'Worker', sub: s.mode === 'B' ? (s.todo.length ? 'hàng chờ: ' + s.todo.join(', ') : 'hàng chờ trống') : 'không dùng ở chế độ ' + s.mode,
        cls: s.mode === 'B' ? '' : 'out' });
      st.setNode('K', { title: 'Offset đã commit', sub: '= ' + s.committed + (s.committed ? ' (đọc lại từ ' + s.committed + ')' : ''), cls: 'hot' });
    }
    function snap() { var s = snapshot(); if (pl) pl.call(function () { paint(s); }); }

    [['poll()', poll], ['Xử lý 1 record', work], ['Crash rồi chạy lại', crash], ['Làm lại', function () { fresh(); }]].forEach(function (p) {
      var b = el('button', null, p[0]); b.type = 'button';
      b.addEventListener('click', function () {
        if (pl && p[0] !== 'Làm lại') pl.run(p[1], draw);
        else { p[1](); draw(); }
      });
      row2.appendChild(b);
    });

    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 6); if (pl) pl.say(t); }

    function poll() {
      var m = mode.value;
      if (m !== 'B' && S.todo.length) { ev('poll() bị chặn: lô hiện tại chưa xử lý xong (xử lý nằm trong thread consumer).'); return; }
      if (m !== 'C' && S.batch.length) { // auto-commit xảy ra trong poll(): commit tới hết lô trước
        S.committed = S.batch[S.batch.length - 1] + 1;
        if (pl) pl.tok('C', 'K', 'commit ' + S.committed, S.todo.length ? 'err' : 'ack');
        snap();
        ev('poll(): auto-commit offset ' + S.committed + ' (hết lô trước' + (S.todo.length ? ', DÙ worker còn ' + S.todo.length + ' record chưa làm' : '') + ').');
      }
      if (S.pos >= N) { S.batch = []; ev('Hết dữ liệu.'); return; }
      S.batch = [];
      for (var i = 0; i < BATCH && S.pos < N; i++) S.batch.push(S.pos++);
      S.todo = S.todo.concat(S.batch);
      if (pl) pl.par(S.batch.map(function (x) { return ['o' + x, 'C', '#' + x, 'msg']; }));
      if (pl && m === 'B') { snap(); pl.par(S.batch.map(function (x) { return ['C', 'W', '#' + x, 'fetch']; })); }
      snap();
      ev('poll() trả offset ' + S.batch.join(',') + (m === 'B' ? ' → giao hết cho worker, return ngay.' : '.'));
    }

    function work() {
      if (!S.todo.length) { ev('Không có record nào chờ xử lý.'); return; }
      var o = S.todo.shift();
      S.done[o] = (S.done[o] || 0) + 1;
      if (pl) pl.pulse(mode.value === 'B' ? 'W' : 'C');
      snap();
      if (pl) pl.pulse('o' + o);
      ev('Xử lý xong offset ' + o + '.');
      if (mode.value === 'C') {
        S.committed = o + 1;
        if (pl) pl.tok('C', 'K', 'commit ' + S.committed, 'ack');
        snap();
        ev('ack(): commit offset ' + S.committed + '.');
      }
    }

    function crash() {
      S.crashes++;
      if (pl) { pl.pulse('C'); if (mode.value === 'B') pl.pulse('W'); }
      ev('CRASH. Chạy lại từ offset đã commit = ' + S.committed + '. Việc đang chờ trong worker bị mất.');
      S.pos = S.committed; S.batch = []; S.todo = [];
      if (pl) pl.tok('K', 'C', 'từ ' + S.committed, 'fetch');
      snap();
    }

    function draw() {
      paint(snapshot());
      cap.textContent = S.events[0] ? '› ' + S.events[0] : (st ? 'Bấm poll() để bắt đầu.' : '');
      grid.innerHTML = '';
      for (var o = 0; o < N; o++) {
        var c = el('div', 'part');
        c.appendChild(el('h4', null, 'offset ' + o));
        var n = S.done[o] || 0;
        var tags = [];
        if (o === S.committed) tags.push('◄ committed');
        if (S.todo.indexOf(o) >= 0) tags.push('chờ xử lý');
        c.appendChild(el('span', 'msg', n ? 'đã xử lý ' + n + ' lần' : 'chưa xử lý'));
        if (tags.length) c.appendChild(el('span', 'msg', tags.join(' · ')));
        if (n > 1) c.style.borderColor = 'var(--bad)';
        if (o < S.committed && !n) c.style.borderColor = 'var(--bad)';
        grid.appendChild(c);
      }
      var lost = [], dup = [];
      for (var i = 0; i < N; i++) {
        if (i < S.committed && !(S.done[i])) lost.push(i);
        if ((S.done[i] || 0) > 1) dup.push(i);
      }
      ledger.className = 'note' + (lost.length ? ' bad' : '');
      ledger.textContent = 'Offset đã commit: ' + S.committed + ' · MẤT (đã commit mà chưa từng xử lý): ' +
        (lost.length ? lost.join(', ') : 'không') + ' · TRÙNG (xử lý ≥ 2 lần): ' + (dup.length ? dup.join(', ') : 'không');
      evBox.innerHTML = '';
      S.events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    draw();
  }
  global.CommitSim = { mount: mount };
})(window);
