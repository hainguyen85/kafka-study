/*
 * Mô phỏng "message đi vào partition nào". Chỉ để minh hoạ, KHÔNG phải thuật toán thật của Kafka.
 * - Có key: băm key (djb2 đơn giản, không nhất thiết trùng hàm băm của Kafka) rồi chia dư số partition.
 * - Không key: theo tài liệu producer, Kafka dùng "sticky partition", đổi partition khi đã ghi đủ batch.size byte.
 *   Ở đây đổi sau mỗi STICKY_EVERY message để dễ thấy.
 *
 * Dùng:
 *   <div id="sim"></div>
 *   <script src="../assets/partition-sim.js"></script>
 *   <script>PartitionSim.mount(document.getElementById('sim'), { keys: ['DRV-001','DRV-002'], partitions: [3, 6] });</script>
 */
(function (global) {
  function hash(s) { var h = 5381; for (var i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0; return h; }
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root, opt) {
    opt = opt || {};
    var counts = opt.partitions || [3, 6];
    var STICKY_EVERY = 4;
    var n = counts[0], seq = 0, sticky = 0, stuck = 0, log = [];
    var box = el('div', 'sim');
    var row = el('div', 'row');
    var key = el('input'); key.type = 'text'; key.value = (opt.keys && opt.keys[0]) || (opt.keys && opt.keys[0]) || 'KEY-1'; key.setAttribute('aria-label', 'key');
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
    var parts = el('div', 'parts');
    var note = el('div', 'note');
    note.hidden = true;
    [row, presets, parts, note].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    function draw() {
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
      var p;
      if (k) { p = hash(k) % n; }
      else { p = sticky % n; if (++stuck >= STICKY_EVERY) { stuck = 0; sticky = (sticky + 1) % n; } }
      log.push({ k: k, p: p, id: ++seq });
      draw();
    }
    send.addEventListener('click', function () { add(key.value.trim() || (opt.keys && opt.keys[0]) || 'KEY-1'); });
    sendNo.addEventListener('click', function () { add(''); });
    reset.addEventListener('click', function () { log = []; seq = 0; sticky = 0; stuck = 0; draw(); });
    sel.addEventListener('change', function () { n = parseInt(sel.value, 10); draw(); });
    draw();
  }
  global.PartitionSim = { mount: mount };
})(window);
