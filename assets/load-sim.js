/*
 * Mô phỏng quá tải (Bài 8b): hai chỗ dữ liệu chờ khi tải cao.
 *  1. Bộ đệm producer (buffer.memory 32 MB): ứng dụng ghi nhanh hơn producer gửi được thì hàng đợi dài ra,
 *     độ trễ gửi chủ yếu là thời gian xếp hàng; bộ đệm đầy thì send() chặn thread gọi (tối đa max.block.ms).
 *  2. Lag của consumer group: đọc chậm hơn ghi thì lag tăng đều; số partition là trần song song
 *     (consumer thừa nhàn rỗi); key nóng dồn tải vào một partition, thêm consumer không giúp.
 * Trần thông lượng của bốn cấu hình producer lấy từ số đo lab Bài 8b trên máy người học (200.000 message 200 byte,
 * payload nén rất tốt): đây là số minh hoạ, máy và dữ liệu khác sẽ khác. Mô hình tính theo từng giây, không phải client thật.
 *
 * Dùng: <div id="lsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/load-sim.js"></script>
 *       <script>LoadSim.mount(document.getElementById('lsim'));</script>
 */
(function (global) {
  var BUF = 160000;          // ≈ 32 MB / 200 byte
  var CFG = {
    l0: { label: 'Producer: linger.ms=0 (mặc định cũ), ~15.000 msg/s', cap: 15000 },
    def: { label: 'Producer: mặc định 4.x, linger.ms=5, ~20.000 msg/s', cap: 20000 },
    lz4: { label: 'Producer: thêm compression.type=lz4, ~58.000 msg/s', cap: 58000 },
    tuned: { label: 'Producer: linger.ms=25, batch 128 KB, lz4, ~72.000 msg/s', cap: 72000 }
  };
  var LOAD = {
    low: { label: 'Tải: bình thường, 10.000 msg/s', rate: 10000 },
    open: { label: 'Tải: mở cửa phiên, 40.000 msg/s', rate: 40000 }
  };
  var PROC = {
    fast: { label: 'Xử lý mỗi message 0,05 ms (20.000/s mỗi consumer)', rate: 20000 },
    mid: { label: 'Xử lý mỗi message 0,2 ms (5.000/s mỗi consumer)', rate: 5000 },
    slow: { label: 'Xử lý mỗi message 1 ms (1.000/s, như lab)', rate: 1000 }
  };
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function n(x) { return Math.round(x).toLocaleString('vi-VN'); }
  function sel(opts, val, aria) {
    var s = el('select'); s.setAttribute('aria-label', aria);
    Object.keys(opts).forEach(function (k) { var o = el('option', null, opts[k].label || opts[k]); o.value = k; s.appendChild(o); });
    s.value = val; return s;
  }
  function numSel(from, to, val, fmt, aria) {
    var s = el('select'); s.setAttribute('aria-label', aria);
    for (var i = from; i <= to; i++) { var o = el('option', null, fmt(i)); o.value = i; s.appendChild(o); }
    s.value = val; return s;
  }

  function mount(root) {
    var box = el('div', 'sim');
    var r1 = el('div', 'row'), r2 = el('div', 'row'), r3 = el('div', 'row'), r4 = el('div', 'row');
    var load = sel(LOAD, 'open', 'tốc độ ứng dụng ghi'), cfg = sel(CFG, 'def', 'cấu hình producer');
    var parts = numSel(3, 6, 6, function (i) { return i + ' partition'; }, 'số partition');
    var cons = numSel(1, 6, 2, function (i) { return i + ' consumer'; }, 'số consumer trong group');
    var proc = sel(PROC, 'mid', 'thời gian xử lý mỗi message');
    var hot = el('input'); hot.type = 'checkbox';
    var hotL = el('label'); hotL.appendChild(hot); hotL.appendChild(document.createTextNode(' Key nóng: một mã chiếm 60% tải'));
    var pause = el('input'); pause.type = 'checkbox';
    var pauseL = el('label'); pauseL.appendChild(pause); pauseL.appendChild(document.createTextNode(' Hệ thống phía sau quá tải: pause() container'));
    r1.appendChild(load); r1.appendChild(cfg);
    r2.appendChild(parts); r2.appendChild(cons); r2.appendChild(proc);
    r3.appendChild(hotL); r3.appendChild(pauseL);
    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [r1, r2, r3, r4, stageHost, cap, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var S;
    function fresh() { S = { t: 0, buf: 0, lag: [0, 0, 0, 0, 0, 0], blocked: 0, events: [], last: null, lastSay: '' }; }
    fresh();
    // Đổi cấu hình giữa chừng được (như đổi cấu hình rồi deploy lại); "Làm lại" để về 0.
    [load, cfg, parts, cons, proc, hot, pause].forEach(function (x) { x.addEventListener('change', function () { draw(); }); });

    // ---------- sân khấu ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 322, label: 'Ứng dụng ghi vào bộ đệm producer, producer gửi vào các partition, consumer trong group đọc từng partition.'
    }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    function rowY(i) { return 8 + i * 52; }
    if (st) {
      st.node('A', { x: 10, y: 20, w: 170, h: 60 });
      st.node('F', { x: 10, y: 160, w: 170, h: 70 });
      for (var i = 0; i < 6; i++) {
        st.node('P' + i, { x: 240, y: rowY(i), w: 160, h: 48 });
        st.node('C' + i, { x: 460, y: rowY(i), w: 170, h: 48 });
      }
    }
    function owner(p, C) { return p % C; }
    function owned(c, P, C) { var r = []; for (var p = 0; p < P; p++) if (owner(p, C) === c) r.push(p); return r; }

    function snapshot() {
      return { t: S.t, buf: S.buf, lag: S.lag.slice(), last: S.last && JSON.parse(JSON.stringify(S.last)),
        P: +parts.value, C: +cons.value, rate: LOAD[load.value].rate, cap: CFG[cfg.value].cap, pr: PROC[proc.value].rate, paused: pause.checked, hot: hot.checked };
    }
    function paint(s) {
      if (!st) return;
      var L = s.last;
      st.setNode('A', { title: 'Ứng dụng gọi send()', sub: n(s.rate) + ' msg/s' + (L && L.blocked ? ' · ĐANG BỊ CHẶN' : ''), cls: L && L.blocked ? 'bad' : 'client' });
      var pct = Math.round(s.buf * 100 / BUF);
      var wait = s.buf / s.cap;
      st.setNode('F', { title: 'Bộ đệm producer ' + pct + '%', sub: 'chờ ' + n(s.buf) + ' · ~' + (wait < 1 ? Math.round(wait * 1000) + ' ms' : wait.toFixed(1).replace('.', ',') + ' giây'),
        cls: pct >= 100 ? 'bad' : pct > 0 ? 'hot' : '' });
      var maxLag = Math.max.apply(null, s.lag.slice(0, s.P).concat([1]));
      for (var p = 0; p < 6; p++) {
        if (p >= s.P) { st.setNode('P' + p, { title: '', sub: '', cls: 'gone' }); continue; }
        var d = L ? L.dlag[p] : 0;
        st.setNode('P' + p, { title: 'P' + p + (s.hot && p === 0 ? ' (key nóng)' : ''), sub: 'lag ' + n(s.lag[p]) + (d > 0 ? ' ▲' + n(d) + '/s' : d < 0 ? ' ▼' : ''),
          cls: 'small' + (s.lag[p] > 0 && s.lag[p] === maxLag && d > 0 ? ' bad' : s.lag[p] > 0 ? ' pending' : ' ok') });
      }
      for (var c = 0; c < 6; c++) {
        if (c >= s.C) { st.setNode('C' + c, { title: '', sub: '', cls: 'gone' }); continue; }
        var mine = owned(c, s.P, s.C);
        var sub = !mine.length ? 'nhàn rỗi: hết partition' : s.paused ? 'pause(): vẫn poll()' : 'đọc ' + mine.map(function (p) { return 'P' + p; }).join(', ') + ' · ' + n(s.pr) + '/s';
        st.setNode('C' + c, { title: 'C' + (c + 1), sub: sub, cls: 'small ' + (!mine.length ? 'out' : s.paused ? 'pending' : 'client') });
      }
    }
    function snap() { var s = snapshot(); if (pl) pl.call(function () { paint(s); }); }
    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 8); }

    // ---------- một giây ----------
    function tick() {
      var P = +parts.value, C = +cons.value, rate = LOAD[load.value].rate, capP = CFG[cfg.value].cap, pr = PROC[proc.value].rate;
      // producer: nhận vào bộ đệm tới khi đầy, gửi tối đa capP mỗi giây
      var prevBuf = S.buf;
      var accepted = Math.min(rate, BUF - S.buf + capP);
      var blocked = rate - accepted > 0.5;
      S.buf += accepted;
      var sent = Math.min(capP, S.buf);
      S.buf -= sent;
      // chia vào partition
      var share = [];
      for (var p = 0; p < P; p++) share[p] = hot.checked ? sent * 0.4 / P + (p === 0 ? sent * 0.6 : 0) : sent / P;
      var before = S.lag.slice();
      for (p = 0; p < P; p++) S.lag[p] += share[p];
      // consumer: mỗi consumer xử lý tối đa pr/giây trên các partition của nó, ưu tiên theo backlog
      var readTotal = 0;
      for (var c = 0; c < C; c++) {
        var mine = owned(c, P, C), budget = pause.checked ? 0 : pr;
        while (budget > 0.5 && mine.length) {
          var open = mine.filter(function (q) { return S.lag[q] > 0.5; });
          if (!open.length) break;
          var each = budget / open.length;
          open.forEach(function (q) { var take = Math.min(each, S.lag[q]); S.lag[q] -= take; budget -= take; readTotal += take; });
        }
      }
      for (p = P; p < 6; p++) S.lag[p] = 0;
      S.t++;
      var dlag = S.lag.map(function (x, i) { return Math.round(x - before[i]); });
      var idle = []; for (c = P; c < C; c++) idle.push('C' + (c + 1));
      S.last = { sent: sent, blocked: blocked, read: readTotal, dlag: dlag, idle: idle };
      if (blocked) S.blocked++;

      // hình: một lượt token cho cả giây
      if (pl) {
        pl.tok('A', 'F', '+' + n(accepted), blocked ? 'err' : 'msg');
        var legs = []; for (p = 0; p < P; p++) if (share[p] > 0) legs.push(['F', 'P' + p, n(share[p]), 'msg']);
        if (legs.length) pl.par(legs);
        snap();
        if (!pause.checked) {
          var reads = []; for (p = 0; p < P; p++) if (before[p] + share[p] > 0.5) reads.push(['P' + p, 'C' + owner(p, C), 'fetch', 'fetch']);
          if (reads.length) pl.par(reads);
        }
        snap();
      }
      // lời giải thích: chọn điều quan trọng nhất của giây này
      var totalD = Math.round(S.lag.reduce(function (a, b) { return a + b; }, 0) - before.reduce(function (a, b) { return a + b; }, 0));
      // hai nửa: phía producer (chỉ khi có hàng đợi) và phía consumer (luôn có)
      var prodMsg = blocked ? 'Producer: bộ đệm ĐẦY, send() chặn thread của ứng dụng (tối đa max.block.ms = 60 giây rồi ném lỗi): backpressure phía producer.'
        : S.buf > prevBuf ? 'Producer: ứng dụng ghi ' + n(rate) + '/s nhưng chỉ gửi được ' + n(sent) + '/s, hàng đợi trong bộ đệm dài thêm; độ trễ gửi chủ yếu là thời gian xếp hàng.'
        : prevBuf > 0 ? 'Producer: gửi được ' + n(sent) + '/s, nhanh hơn tốc độ ghi ' + n(rate) + '/s: hàng đợi trong bộ đệm ngắn lại' + (S.buf > 0 ? '.' : ', đã hết.')
        : '';
      var consMsg;
      if (pause.checked) consMsg = 'Consumer: container đang pause(), lag tăng là chủ đích, không phải sự cố; vẫn poll() nên không rebalance.';
      else if (hot.checked && dlag[0] > 0 && dlag.slice(1, P).every(function (d) { return d <= 0; })) consMsg = 'Consumer: chỉ P0 (key nóng) tụt lại, consumer giữ P0 quá tải trong khi các consumer khác rảnh. Thêm consumer không giúp: một partition chỉ một consumer đọc.';
      else if (idle.length && totalD > 0) consMsg = 'Consumer: ' + idle.join(', ') + ' nhàn rỗi vì chỉ có ' + P + ' partition, trong khi lag vẫn tăng: số partition là trần song song.';
      else if (totalD > 0) consMsg = 'Consumer: lag tăng ' + n(totalD) + '/giây, group đọc ' + n(readTotal) + '/s chậm hơn tốc độ ghi ' + n(sent) + '/s. Lag tăng đều là tín hiệu quá tải.';
      else if (totalD < 0) consMsg = 'Consumer: lag giảm ' + n(-totalD) + '/giây, group đọc nhanh hơn ghi, đang đuổi kịp.';
      else consMsg = 'Consumer: đọc kịp, lag bằng 0.';
      var msg = prodMsg ? prodMsg + ' ' + consMsg : consMsg;
      ev('Giây ' + S.t + ': ' + msg);
      if (pl && msg !== S.lastSay) pl.say(msg, 700);
      S.lastSay = msg;
    }
    function run(k) { return function () { for (var i = 0; i < k; i++) tick(); }; }

    [['+1 giây', run(1)], ['Chạy 5 giây', run(5)], ['Làm lại', null]].forEach(function (b) {
      var x = el('button', null, b[0]); x.type = 'button';
      x.addEventListener('click', function () {
        if (!b[1]) { fresh(); draw(); return; }
        if (pl) pl.run(b[1], draw); else { b[1](); draw(); }
      });
      r4.appendChild(x);
    });
    if (pl) r4.appendChild(pl.controls());

    function draw() {
      paint(snapshot());
      var L = S.last, P = +parts.value;
      var total = S.lag.slice(0, P).reduce(function (a, b) { return a + b; }, 0);
      cap.textContent = S.events[0] ? '› ' + S.events[0] : 'Chọn cấu hình, đoán trước: lag tăng hay giảm, send() có bị chặn không? Rồi bấm "Chạy 5 giây".';
      var bad = L && (L.blocked || L.dlag.some(function (d) { return d > 0; }));
      ledger.className = 'note' + (bad ? ' bad' : '');
      ledger.textContent = 'Giây ' + S.t + ' · producer gửi ' + (L ? n(L.sent) : 0) + '/s (trần ' + n(CFG[cfg.value].cap) + ') · bộ đệm ' +
        Math.round(S.buf * 100 / BUF) + '% · send() bị chặn: ' + (L && L.blocked ? 'CÓ' : 'không') + (S.blocked ? ' (đã chặn tổng ' + S.blocked + ' giây)' : '') +
        ' · tổng lag ' + n(total) + ' · consumer nhàn rỗi: ' + (L && L.idle.length ? L.idle.join(', ') : Math.max(0, +cons.value - P) ? 'C' + (P + 1) + '…' : 'không');
      evBox.innerHTML = '';
      S.events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    draw();
  }
  global.LoadSim = { mount: mount };
})(window);
