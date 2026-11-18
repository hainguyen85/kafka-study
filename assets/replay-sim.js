/*
 * Mô phỏng Bài 8c: giữ dữ liệu và đọc lại. Ba tình huống, chọn bằng ô đầu tiên:
 *  1. Retention theo segment: broker xoá CẢ segment khi timestamp lớn nhất của segment quá retention.ms;
 *     segment đang ghi không bị xoá. Nên dữ liệu sống lâu hơn retention, hoặc ngắn hơn khi nạp lại
 *     dữ liệu cũ giữ CreateTime gốc. [Implementation: Log]
 *  2. Đọc lại theo thời điểm: kafka-consumer-groups.sh --reset-offsets --to-datetime; không ghi múi giờ thì
 *     bị hiểu là UTC (đọc trong source 4.4.0). Mốc sau record cuối thì về cuối log kèm cảnh báo "is empty".
 *     Mặc định chỉ xem trước; group phải dừng. [Basic operations]
 *  3. Compaction và tombstone: compaction giữ bản cuối mỗi key ở các segment đã đóng, offset không đổi;
 *     tombstone (value null) bị dọn sau delete.retention.ms, consumer chậm hơn thế có thể bỏ lỡ lệnh xoá. [Design: Log Compaction]
 * Mô hình theo tài liệu, rút gọn (ví dụ: một partition, ngày thay cho mili giây), không phải broker thật.
 *
 * Dùng: <div id="rsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/replay-sim.js"></script>
 *       <script>ReplaySim.mount(document.getElementById('rsim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function sel(pairs, val, aria) {
    var s = el('select'); s.setAttribute('aria-label', aria);
    pairs.forEach(function (p) { var o = el('option', null, p[1]); o.value = p[0]; s.appendChild(o); });
    s.value = val; return s;
  }
  function chk(text) { var i = el('input'); i.type = 'checkbox'; var l = el('label'); l.appendChild(i); l.appendChild(document.createTextNode(' ' + text)); return { box: i, label: l }; }

  // Phiên 14/11: 09:00 → 15:00 giờ VN, mỗi 30 phút một record (offset 0..12). Lưu theo giờ VN, đổi sang UTC khi so.
  var SESSION = []; for (var i = 0; i <= 12; i++) { var m = 9 * 60 + i * 30; SESSION.push(m); }
  function hhmm(min) { var h = Math.floor(min / 60), mm = min % 60; return (h < 10 ? '0' : '') + h + ':' + (mm < 10 ? '0' : '') + mm; }

  function mount(root) {
    var box = el('div', 'sim');
    var r0 = el('div', 'row'), r1 = el('div', 'row'), r2 = el('div', 'row');
    var mode = sel([['ret', '1. Retention: broker xoá theo segment'], ['time', '2. Đọc lại theo thời điểm: --reset-offsets --to-datetime'],
      ['compact', '3. Compaction và tombstone']], 'ret', 'tình huống');
    r0.appendChild(mode);
    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [r0, r1, r2, stageHost, cap, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var st = global.FlowAnim ? FlowAnim.stage(stageHost, { width: 640, height: 250, label: 'Log của một partition và những gì đọc được từ nó.' }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    var events = [];
    function ev(t) { events.unshift(t); events = events.slice(0, 7); if (pl) pl.say(t, 650); }
    var cur = null;   // tình huống đang chạy: { draw, ids }
    var used = [];    // id các nút trên sân khấu của tình huống hiện tại
    function node(id, o) { if (st) { if (used.indexOf(id) < 0) used.push(id); if (!st.hasNode(id)) st.node(id, o); } }  // tạo một lần; id mang cả hình dạng
    function clearStage() { if (!st) return; used.forEach(function (id) { st.setNode(id, { title: '', sub: '', cls: 'gone' }); }); }
    function buttons(list) {
      r2.innerHTML = '';
      list.forEach(function (b) {
        var x = el('button', null, b[0]); x.type = 'button';
        x.addEventListener('click', function () { if (pl && b[2] !== false) pl.run(b[1], draw); else { b[1](); draw(); } });
        r2.appendChild(x);
      });
      if (pl) r2.appendChild(pl.controls());
    }
    function draw() { if (cur) cur.draw(); evBox.innerHTML = ''; events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); }); }

    // ================= 1. Retention =================
    function retention() {
      var ret = sel([['3', 'retention.ms = 3 ngày'], ['7', 'retention.ms = 7 ngày (mặc định)']], '7', 'retention');
      var seg = sel([['1', 'segment.ms = 1 ngày'], ['3', 'segment.ms = 3 ngày'], ['7', 'segment.ms = 7 ngày (mặc định)']], '7', 'segment');
      var back = chk('Ngày 10 có team nạp lại giao dịch 2 năm trước, giữ CreateTime gốc');
      r1.innerHTML = ''; [ret, seg, back.label].forEach(function (x) { r1.appendChild(x); });
      var S;
      function segments() {
        var s = +seg.value, list = [];
        for (var d = 1; d <= 10; d += s) list.push({ from: d, to: Math.min(10, d + s - 1), maxTs: Math.min(10, d + s - 1) });
        list[list.length - 1].active = true;
        if (back.box.checked) list.splice(list.length - 1, 0, { from: 10, to: 10, maxTs: -720, backfill: true });
        return list;
      }
      function fresh() { S = { segs: segments(), ran: false }; }
      [ret, seg, back.box].forEach(function (x) { x.addEventListener('change', function () { fresh(); events = []; draw(); }); });
      fresh();
      function W() { return Math.max(1, S.segs.length); }
      function sid(k) { return 'rs' + k + 'of' + W(); }
      function layout() {
        clearStage();
        var w = Math.floor(600 / W()) - 6;
        S.segs.forEach(function (g, k) { node(sid(k), { x: 20 + k * (w + 6), y: 30, w: w, h: 70 }); });
        node('rcl', { x: 20, y: 140, w: 290, h: 60 }); node('rrd', { x: 330, y: 140, w: 290, h: 60 });
      }
      function label(g) { return g.backfill ? 'Nạp lại (ghi ngày 10)' : g.from === g.to ? 'Ngày ' + g.from : 'Ngày ' + g.from + '–' + g.to; }
      function status(g) {
        var limit = 10 - +ret.value;     // "bây giờ" = cuối ngày 10; timestamp lớn nhất < limit thì hết hạn
        if (g.active) return { t: 'đang ghi: không xoá', cls: 'hot' };
        if (g.maxTs < limit) return S.ran ? { t: 'ĐÃ XOÁ', cls: 'dead' } : { t: 'chờ xoá', cls: 'pending bad' };
        if (g.from < limit) return { t: 'còn, có ngày quá hạn', cls: 'pending' };
        return { t: 'còn', cls: 'ok' };
      }
      function paint() {
        if (!st) return;
        layout();
        S.segs.forEach(function (g, k) {
          var s = status(g);
          var wide = W() <= 4;
          st.setNode(sid(k), { title: g.backfill ? (wide ? 'Nạp lại, ts 2 năm trước' : 'Nạp lại') : wide ? label(g) + ' · ts max ' + g.maxTs : label(g).replace('Ngày ', 'N'), sub: s.t, cls: 'small ' + s.cls });
        });
        st.setNode('rcl', { title: 'Log cleaner (mỗi 5 phút)', sub: 'bây giờ: cuối ngày 10 · mốc: ngày ' + (10 - +ret.value), cls: S.ran ? 'hot' : '' });
        var alive = S.segs.filter(function (g) { return !(S.ran && status(g).t === 'ĐÃ XOÁ') && !g.backfill; });
        st.setNode('rrd', { title: 'Consumer đọc từ đầu', sub: alive.length ? 'record cũ nhất: ngày ' + alive[0].from : 'trống', cls: 'client' });
      }
      function run() {
        S.ran = true;
        var limit = 10 - +ret.value, del = [], longer = [];
        S.segs.forEach(function (g, k) {
          if (g.active) return;
          if (g.maxTs < limit) { del.push(label(g)); if (pl) pl.pulse(sid(k)); }
          else if (g.from < limit) longer.push(label(g));
        });
        if (pl) pl.call(paint);
        ev(del.length ? 'Cleaner xoá cả segment: ' + del.join(', ') + '. Xoá theo segment, không theo từng record.' : 'Cleaner chạy nhưng không segment nào có timestamp lớn nhất quá ' + ret.value + ' ngày.');
        if (longer.length) ev('Segment ' + longer.join(', ') + ' chứa ngày đã quá ' + ret.value + ' ngày nhưng vẫn còn, vì timestamp LỚN NHẤT của nó chưa quá hạn: dữ liệu sống LÂU hơn retention.');
        if (back.box.checked) ev('Segment nạp lại vừa ghi hôm nay mà đã bị xoá: timestamp lớn nhất là 2 năm trước. Dữ liệu sống NGẮN hơn retention, không có lỗi nào. Sửa: message.timestamp.type=LogAppendTime.');
      }
      buttons([['Log cleaner chạy (cuối ngày 10)', run], ['Làm lại', function () { fresh(); events = []; }, false]]);
      return {
        draw: function () {
          paint();
          var limit = 10 - +ret.value;
          var oldest = S.segs.filter(function (g) { return !g.backfill && !(S.ran && !g.active && g.maxTs < limit); });
          cap.textContent = events[0] ? '› ' + events[0] : 'Đoán trước: với cấu hình này, consumer đọc từ đầu thấy dữ liệu từ ngày nào? Rồi cho cleaner chạy.';
          ledger.className = 'note' + (S.ran && (back.box.checked) ? ' bad' : '');
          ledger.textContent = 'retention ' + ret.value + ' ngày, segment ' + seg.value + ' ngày · ' + S.segs.length + ' segment · ' +
            (S.ran ? 'sau khi cleaner chạy, record cũ nhất: ngày ' + (oldest.length ? oldest[0].from : '-') + ' (mốc retention: ngày ' + limit + ')' : 'cleaner chưa chạy');
        }
      };
    }

    // ================= 2. Đọc lại theo thời điểm =================
    function replay() {
      var tz = sel([['', 'không ghi múi giờ'], ['+07:00', 'ghi +07:00'], ['Z', 'ghi Z (UTC)']], '', 'múi giờ trong lệnh');
      var exe = chk('--execute (bỏ trống là --dry-run)'), live = chk('Group vẫn đang chạy');
      r1.innerHTML = ''; [tz, exe.label, live.label].forEach(function (x) { r1.appendChild(x); });
      var S;
      function fresh() { S = { committed: 13, preview: null, replayed: null }; }
      [tz, exe.box, live.box].forEach(function (x) { x.addEventListener('change', function () { S.preview = null; S.replayed = null; draw(); }); });
      fresh();
      var cellW = 44;
      function layout() {
        clearStage();
        SESSION.forEach(function (m, o) { node('tc' + o, { x: 10 + o * (cellW + 4), y: 40, w: cellW, h: 52 }); });
        node('tcli', { x: 10, y: 120, w: 400, h: 56 }); node('tgrp', { x: 430, y: 120, w: 200, h: 56 });
        node('tout', { x: 10, y: 190, w: 620, h: 50 });
      }
      function cmd() { return '--to-datetime 2026-11-14T09:00:00.000' + tz.value + (exe.box.checked ? ' --execute' : ' --dry-run'); }
      function targetVN() { return tz.value === '+07:00' ? 9 * 60 : 9 * 60 + 7 * 60; }   // phút trong ngày, giờ VN
      function paint() {
        if (!st) return;
        layout();
        SESSION.forEach(function (m, o) {
          var cls = 'small';
          if (S.replayed) cls += S.replayed.indexOf(o) >= 0 ? ' ok' : ' out';
          else if (S.preview != null && o >= S.preview) cls += ' pending';
          st.setNode('tc' + o, { title: hhmm(m), sub: 'off ' + o, cls: cls });
        });
        st.setNode('tcli', { title: 'kafka-consumer-groups.sh --reset-offsets', sub: cmd(), cls: 'client' });
        st.setNode('tgrp', { title: 'Group quant-replay', sub: (live.box.checked ? 'đang chạy · ' : 'đã dừng · ') + 'committed ' + S.committed, cls: live.box.checked ? 'hot' : '' });
        var t = targetVN(), txt = tz.value === '+07:00' ? '09:00 giờ VN' : '09:00 UTC = 16:00 giờ VN';
        st.setNode('tout', { title: 'Mốc thời gian công cụ hiểu: ' + txt, sub: S.preview == null ? 'chưa chạy' : 'offset mới: ' + S.preview + (S.preview >= SESSION.length ? ' (cuối log)' : ' (record ' + hhmm(SESSION[S.preview]) + ')'),
          cls: t > SESSION[SESSION.length - 1] ? 'bad' : 'ok' });
      }
      function run() {
        if (pl) pl.pulse('tcli');
        if (live.box.checked) { ev('CLI từ chối: chỉ reset được khi group không còn consumer nào chạy. Dừng mọi instance trước.'); return; }
        var t = targetVN(), off = SESSION.findIndex(function (m) { return m >= t; });
        var warn = '';
        if (off < 0) { off = SESSION.length; warn = ' Cảnh báo "Partition 0 from topic orders-ticks is empty. Falling back to latest known offset." dù partition có dữ liệu.'; }
        S.preview = off;
        if (pl) pl.call(paint);
        if (!exe.box.checked) { ev('Dry-run: in offset mới ' + off + ' nhưng KHÔNG đổi gì; committed vẫn là ' + S.committed + '.' + warn); return; }
        S.committed = off;
        if (pl) pl.call(paint);
        ev('Execute: committed của group đổi thành ' + off + '.' + warn);
      }
      function start() {
        var from = S.committed, list = [];
        for (var o = from; o < SESSION.length; o++) list.push(o);
        S.replayed = list;
        if (pl) { list.slice(0, 4).forEach(function (o) { pl.tok('tc' + o, 'tgrp', hhmm(SESSION[o]), 'fetch'); }); pl.call(paint); }
        ev(list.length ? 'Consumer chạy lại từ offset ' + from + ': đọc ' + list.length + ' record, từ ' + hhmm(SESSION[from]) + ' tới 15:00.' + (from === 0 ? ' Đúng cả phiên.' : ' Thiếu phần đầu phiên.')
          : 'Consumer chạy lại từ offset ' + from + ' = cuối log: không đọc được record nào của phiên. Không có lỗi, chỉ có kết quả rỗng.');
      }
      buttons([['Chạy lệnh', run], ['Khởi động lại consumer', start], ['Làm lại', function () { fresh(); events = []; }, false]]);
      return {
        draw: function () {
          paint();
          cap.textContent = events[0] ? '› ' + events[0] : 'Muốn chạy lại phiên 14/11 từ 09:00 giờ VN. Đoán trước offset mới với lựa chọn hiện tại, rồi bấm "Chạy lệnh".';
          var bad = S.replayed && S.replayed.length < SESSION.length;
          ledger.className = 'note' + (bad ? ' bad' : '');
          ledger.textContent = 'Log: 13 record phiên 14/11, 09:00 → 15:00 giờ VN · committed của group: ' + S.committed +
            (S.replayed ? ' · đã đọc lại ' + S.replayed.length + '/13 record của phiên' : '');
        }
      };
    }

    // ================= 3. Compaction =================
    function compaction() {
      var r = chk('Đã trôi qua delete.retention.ms (1 ngày) kể từ tombstone');
      r1.innerHTML = ''; r1.appendChild(r.box.parentNode);
      var BASE = [['A1', 100], ['A2', 50], ['A3', 70], ['A1', 120], ['A2', 80], ['A3', 75], ['A2', null], ['A1', 130], ['A3', 90]];
      var ACTIVE_FROM = 8;   // offset 8 nằm trong segment đang ghi
      var S;
      function fresh() {
        S = { log: BASE.map(function (kv, o) { return { o: o, k: kv[0], v: kv[1] }; }), slowPos: 6, slow: { A1: 120, A2: 80, A3: 75 }, fresh: null, runs: 0 };
      }
      r.box.addEventListener('change', function () { draw(); });
      fresh();
      function layout() {
        clearStage();
        BASE.forEach(function (kv, o) { node('kc' + o, { x: 10 + o * 69, y: 40, w: 64, h: 52 }); });
        node('kcl', { x: 10, y: 120, w: 300, h: 50 });
        node('kB', { x: 10, y: 186, w: 300, h: 56 }); node('kN', { x: 330, y: 186, w: 300, h: 56 });
      }
      function show(c) { var keys = Object.keys(c).sort(); return keys.length ? keys.map(function (k) { return k + '=' + c[k]; }).join(', ') : '(trống)'; }
      function paint() {
        if (!st) return;
        layout();
        BASE.forEach(function (kv, o) {
          var rec = S.log.filter(function (x) { return x.o === o; })[0];
          if (!rec) { st.setNode('kc' + o, { title: 'off ' + o, sub: 'đã dọn', cls: 'small out' }); return; }
          st.setNode('kc' + o, { title: rec.k + '=' + (rec.v == null ? 'null' : rec.v), sub: 'off ' + o,
            cls: 'small' + (rec.v == null ? ' bad' : o >= ACTIVE_FROM ? ' hot' : '') });
        });
        st.setNode('kcl', { title: 'Compaction', sub: (S.runs ? 'đã chạy ' + S.runs + ' lần' : 'chưa chạy') + ' · off 8 ở segment đang ghi', cls: '' });
        st.setNode('kB', { title: 'Cache của B (đọc chậm)', sub: 'đọc tới offset ' + (S.slowPos - 1) + ' · ' + show(S.slow), cls: S.slow.A2 != null && S.slowPos > 6 ? 'bad' : 'client' });
        st.setNode('kN', { title: 'Cache của consumer mới (đọc từ đầu)', sub: S.fresh ? show(S.fresh) : 'chưa đọc', cls: S.fresh ? 'ok' : '' });
      }
      function compact() {
        S.runs++;
        var lastOf = {};
        S.log.forEach(function (x) { if (x.o < ACTIVE_FROM) lastOf[x.k] = x.o; });
        var removed = [];
        S.log = S.log.filter(function (x) {
          if (x.o >= ACTIVE_FROM) return true;
          if (lastOf[x.k] !== x.o) { removed.push(x.k + '@' + x.o); return false; }
          if (x.v == null && r.box.checked) { removed.push('tombstone ' + x.k + '@' + x.o); return false; }
          return true;
        });
        if (pl) { pl.pulse('kcl'); pl.call(paint); }
        ev(removed.length ? 'Compaction dọn ' + removed.join(', ') + '. Bản còn lại giữ nguyên offset; segment đang ghi (offset 8) không bị đụng tới.' +
          (S.log.some(function (x) { return x.v == null; }) ? ' Tombstone A2 còn ở lại tới khi qua delete.retention.ms.' : '')
          : 'Compaction chạy nhưng không còn gì để dọn.');
      }
      function slowRead() {
        var rest = S.log.filter(function (x) { return x.o >= S.slowPos; });
        rest.forEach(function (x) { if (x.v == null) delete S.slow[x.k]; else S.slow[x.k] = x.v; });
        if (pl) { rest.slice(0, 4).forEach(function (x) { pl.tok('kc' + x.o, 'kB', x.k, 'fetch'); }); }
        var sawTomb = rest.some(function (x) { return x.v == null; });
        S.slowPos = BASE.length;
        if (pl) pl.call(paint);
        ev(sawTomb ? 'B đọc tiếp từ offset 6, thấy tombstone A2 nên xoá A2 khỏi cache: đúng.'
          : 'B đọc tiếp từ offset 6 nhưng tombstone A2 đã bị dọn: B không bao giờ thấy lệnh xoá, cache vẫn giữ A2 = tài khoản đã đóng.');
      }
      function freshRead() {
        var c = {};
        S.log.forEach(function (x) { if (x.v == null) delete c[x.k]; else c[x.k] = x.v; });
        S.fresh = c;
        if (pl) { S.log.slice(0, 4).forEach(function (x) { pl.tok('kc' + x.o, 'kN', x.k, 'fetch'); }); pl.call(paint); }
        ev('Consumer mới đọc từ đầu (seekToBeginning): ' + show(c) + '. Đọc từ đầu luôn ra trạng thái cuối, có hay không có tombstone.');
      }
      buttons([['Compaction chạy', compact], ['B đọc tiếp', slowRead], ['Consumer mới đọc từ đầu', freshRead], ['Làm lại', function () { fresh(); events = []; }, false]]);
      return {
        draw: function () {
          paint();
          cap.textContent = events[0] ? '› ' + events[0] : 'Topic account-balance (compact). A2 bị đóng ở offset 6 (tombstone). B đã đọc tới offset 5 rồi dừng. Thử: compaction chạy, tick "đã trôi qua 1 ngày", compaction chạy lần nữa, rồi B đọc tiếp.';
          var bad = S.slow.A2 != null && S.slowPos > 6;
          ledger.className = 'note' + (bad ? ' bad' : '');
          ledger.textContent = 'Log còn ' + S.log.length + '/' + BASE.length + ' record · offset còn lại: ' + S.log.map(function (x) { return x.o; }).join(', ') +
            ' · cache B: ' + show(S.slow) + (bad ? ' (SAI: A2 đã đóng)' : '');
        }
      };
    }

    var MAKERS = { ret: retention, time: replay, compact: compaction };
    function pick() { events = []; cur = MAKERS[mode.value](); draw(); }
    mode.addEventListener('change', pick);
    pick();
  }
  global.ReplaySim = { mount: mount };
})(window);
