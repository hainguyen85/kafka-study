/*
 * Mô phỏng "message đi vào partition nào". Chỉ để minh hoạ, KHÔNG phải thuật toán thật của Kafka.
 * - Có key: băm key (djb2 đơn giản, không nhất thiết trùng hàm băm của Kafka) rồi chia dư số partition.
 * - Không key: theo tài liệu producer, Kafka dùng "sticky partition", đổi partition khi đã ghi đủ batch.size byte.
 *   Ở đây đổi sau mỗi STICKY_EVERY message để dễ thấy.
 *
 * Hình động (assets/flow-anim.js, nạp trước file này nếu có): message bay từ producer vào partition được chọn.
 *
 * Dùng:
 *   <div id="sim"></div>
 *   <script src="../assets/flow-anim.js"></script>
 *   <script src="../assets/partition-sim.js"></script>
 *   <script>PartitionSim.mount(document.getElementById('sim'), { keys: ['DRV-001','DRV-002'], partitions: [3, 6] });</script>
 */
(function (global) {
  function hash(s) { var h = 5381; for (var i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0; return h; }
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root, opt) {
    opt = opt || {};
    var counts = opt.partitions || [3, 6];
    var MAXP = Math.max.apply(null, counts);
    var STICKY_EVERY = 4;
    var n = counts[0], seq = 0, sticky = 0, stuck = 0, log = [];
    var box = el('div', 'sim');
    var row = el('div', 'row');
    var key = el('input'); key.type = 'text'; key.value = (opt.keys && opt.keys[0]) || 'KEY-1'; key.setAttribute('aria-label', 'key');
    var sel = el('select'); sel.setAttribute('aria-label', 'số partition');
    counts.forEach(function (c) { var o = el('option', null, c + ' partition'); o.value = c; sel.appendChild(o); });
    var send = el('button', null, 'Gửi có key');
    var sendNo = el('button', null, 'Gửi không key');
    var reset = el('button', null, 'Xoá hết');
    [el('span', null, 'key:'), key, sel, send, sendNo, reset].forEach(function (x) { row.appendChild(x); });
    var presets = el('div', 'row');
    (opt.keys || []).forEach(function (k) {
      var b = el('button', null, 'key = ' + k);
      b.addEventListener('click', function () { key.value = k; });
      presets.appendChild(b);
    });
    var stageHost = el('div'), cap = el('div', 'fa-cap');
    var parts = el('div', 'parts');
    var note = el('div', 'note');
    note.hidden = true;
    [row, presets, stageHost, cap, parts, note].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    // ---------- sân khấu ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 12 + MAXP * 52, label: 'Producer gửi message vào các partition của topic.'
    }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    if (pl) row.appendChild(pl.controls());
    if (st) {
      st.node('P', { x: 20, y: 40, w: 200, h: 62 });
      for (var i = 0; i < MAXP; i++) st.node('p' + i, { x: 400, y: 8 + i * 52, w: 220, h: 46 });
    }
    function layout() {
      if (!st) return;
      var H = Math.max(120, 12 + n * 52);
      st.svg.setAttribute('viewBox', '0 0 640 ' + H);
      st.moveNode('P', { y: H / 2 - 31 });
      st.unlinkAll('L');
      for (var i = 0; i < MAXP; i++) if (i < n) st.linkTo('L' + i, 'P', 'p' + i);
    }
    function paint(upto) {
      if (!st) return;
      var shown = log.slice(0, upto == null ? log.length : upto);
      st.setNode('P', { title: 'Producer', sub: 'key: ' + (key.value.trim() || '(trống)'), cls: 'client' });
      for (var i = 0; i < MAXP; i++) {
        if (i >= n) { st.setNode('p' + i, { cls: 'gone' }); continue; }
        var mine = shown.filter(function (m) { return m.p === i; });
        var last = mine[mine.length - 1];
        st.setNode('p' + i, { title: 'Partition ' + i + ' · ' + mine.length + ' message',
          sub: last ? 'cuối: off ' + (mine.length - 1) + ' · ' + (last.k || 'không key') + ' #' + last.id : '(trống)', cls: 'small' });
      }
    }

    function draw() {
      paint();
      parts.innerHTML = '';
      var cols = [];
      for (var p = 0; p < n; p++) { var c = el('div', 'part'); c.appendChild(el('h4', null, 'Partition ' + p)); parts.appendChild(c); cols.push({ el: c, off: 0 }); }
      var byKey = {};
      log.forEach(function (m) {
        if (m.p >= n) return;
        var col = cols[m.p];
        col.el.appendChild(el('span', 'msg', 'off ' + col.off + ' · ' + (m.k || '(không key)') + ' #' + m.id));
        col.off++;
        if (m.k) { byKey[m.k] = byKey[m.k] || {}; byKey[m.k][m.p] = true; }
      });
      var split = Object.keys(byKey).filter(function (k) { return Object.keys(byKey[k]).length > 1; });
      note.hidden = log.length === 0;
      if (split.length) {
        note.className = 'note bad';
        note.textContent = 'Key ' + split.join(', ') + ' đã nằm ở nhiều partition. Thứ tự giữa các partition không được đảm bảo.';
      } else {
        note.className = 'note';
        note.textContent = 'Mỗi key chỉ nằm trong một partition: thứ tự theo key được giữ (offset tăng dần trong partition).';
      }
    }
    function add(k) {
      var p, why;
      if (k) { p = hash(k) % n; why = 'Key ' + k + ': băm key rồi chia dư cho ' + n + ' → Partition ' + p + '. Cùng key luôn vào cùng partition.'; }
      else {
        p = sticky % n; why = 'Không key: sticky partition, dồn vào Partition ' + p + ' một lúc rồi mới đổi.';
        if (++stuck >= STICKY_EVERY) { stuck = 0; sticky = (sticky + 1) % n; }
      }
      log.push({ k: k, p: p, id: ++seq });
      if (!pl) { cap.textContent = '› ' + why; draw(); return; }
      var before = log.length - 1;
      pl.run(function () {
        pl.call(function () { paint(before); cap.textContent = '› ' + why; });
        pl.tok('P', 'p' + p, (k || '∅') + ' #' + seq, 'msg');
        pl.call(function () { paint(); });
        pl.pulse('p' + p);
      }, draw);
    }
    send.addEventListener('click', function () { add(key.value.trim() || (opt.keys && opt.keys[0]) || 'KEY-1'); });
    sendNo.addEventListener('click', function () { add(''); });
    reset.addEventListener('click', function () { log = []; seq = 0; sticky = 0; stuck = 0; cap.textContent = ''; draw(); });
    sel.addEventListener('change', function () {
      n = parseInt(sel.value, 10); layout(); draw();
      if (log.length) cap.textContent = '› Đổi sang ' + n + ' partition: message mới có thể vào partition khác; message cũ nằm yên chỗ cũ.';
    });
    key.addEventListener('input', function () { paint(); });
    layout();
    draw();
  }
  global.PartitionSim = { mount: mount };
})(window);
