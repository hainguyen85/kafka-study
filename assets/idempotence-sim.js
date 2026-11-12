/*
 * Mô phỏng idempotent producer trên MỘT partition.
 * Chỉ minh hoạ mô hình mà tài liệu mô tả (docs Design: "the broker assigns each producer an ID and deduplicates
 * messages using a sequence number that is sent by the producer along with every message"), không phải client thật.
 *  - Bật idempotence: mỗi batch mang (producer ID, sequence). Broker nhớ sequence cuối của từng producer ID:
 *      sequence đã thấy  -> không ghi lại, vẫn báo thành công (chặn trùng do retry);
 *      sequence nhảy cóc -> từ chối (out of order), producer gửi lại theo đúng thứ tự (giữ thứ tự).
 *  - Producer khởi động lại: nhận producer ID MỚI, sequence đếm lại từ 0 ("single session").
 *
 * Dùng: <div id="isim"></div> <script src="../assets/idempotence-sim.js"></script>
 *       <script>IdempotenceSim.mount(document.getElementById('isim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root) {
    var S;
    function fresh() { S = { pid: 1000, seq: 0, log: [], last: {}, n: 1, lastVal: null, events: [] }; }
    fresh();
    var box = el('div', 'sim');
    var row = el('div', 'row');
    var idem = el('input'); idem.type = 'checkbox'; idem.checked = true;
    var lab = el('label'); lab.appendChild(idem); lab.appendChild(document.createTextNode(' enable.idempotence=true'));
    idem.addEventListener('change', function () { fresh(); ev(idem.checked ? 'Bật idempotence (làm lại từ đầu).' : 'Tắt idempotence (làm lại từ đầu).'); draw(); });
    row.appendChild(lab);
    var row2 = el('div', 'row');
    [['Gửi bình thường', normal],
     ['Gửi, ack bị mất trên đường về', lostAck],
     ['Gửi 2 batch, batch đầu lỗi tạm thời', reorder],
     ['Producer restart, ứng dụng gửi lại đơn cuối', restart],
     ['Làm lại', function () { fresh(); }]].forEach(function (p) {
      var b = el('button', null, p[0]); b.type = 'button';
      b.addEventListener('click', function () { p[1](); draw(); });
      row2.appendChild(b);
    });
    var table = el('div', 'parts');
    var ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row2, table, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 7); }
    function newBatch(val) { return { pid: S.pid, seq: S.seq++, val: val || ('đơn #' + S.n++) }; }

    // Broker nhận một batch. Trả 'ok' | 'dup' | 'outoforder'.
    function deliver(b) {
      if (!idem.checked) { S.log.push({ val: b.val, pid: '-', seq: '-' }); return 'ok'; }
      var last = S.last[b.pid];
      var expected = last == null ? 0 : last + 1;
      if (last != null && b.seq <= last) return 'dup';
      if (b.seq !== expected) return 'outoforder';
      S.log.push({ val: b.val, pid: b.pid, seq: b.seq });
      S.last[b.pid] = b.seq;
      return 'ok';
    }

    function normal() {
      var b = newBatch(); deliver(b); S.lastVal = b.val;
      ev('Gửi ' + b.val + tag(b) + ' → broker ghi, producer nhận ack.');
    }

    function lostAck() {
      var b = newBatch(); deliver(b); S.lastVal = b.val;
      ev('Gửi ' + b.val + tag(b) + ' → broker ĐÃ ghi, nhưng ack bị mất trên đường về.');
      var r = deliver(b); // retry của producer: cùng batch, cùng sequence
      if (r === 'dup') ev('Producer tự retry cùng batch' + tag(b) + ' → broker thấy sequence đã có, KHÔNG ghi lại, trả ack.');
      else ev('Producer tự retry → broker không nhận ra, ghi thêm một bản: TRÙNG.');
    }

    function reorder() {
      var a = newBatch(), b = newBatch(); S.lastVal = b.val;
      ev('Gửi song song ' + a.val + tag(a) + ' và ' + b.val + tag(b) + '. ' + a.val + ' gặp lỗi tạm thời, chưa được ghi.');
      var rb = deliver(b);
      if (rb === 'outoforder') {
        ev(b.val + ' tới trước nhưng sequence nhảy cóc → broker từ chối (out of order).');
        deliver(a); deliver(b);
        ev('Producer gửi lại ' + a.val + ' rồi ' + b.val + ' theo đúng thứ tự sequence → thứ tự giữ nguyên.');
      } else {
        ev(b.val + ' được ghi trước. Producer retry ' + a.val + ' → ghi sau: ĐẢO THỨ TỰ.');
        deliver(a);
      }
    }

    function restart() {
      if (!S.lastVal) { ev('Chưa có đơn nào để gửi lại.'); return; }
      S.pid += 1; S.seq = 0;
      ev('Producer khởi động lại → producer ID mới ' + S.pid + ', sequence đếm lại từ 0.');
      var b = newBatch(S.lastVal); deliver(b);
      ev('Ứng dụng không chắc lần trước đã vào chưa, gửi lại ' + b.val + tag(b) + ' → với broker đây là bản ghi MỚI: TRÙNG' + (idem.checked ? ' dù đã bật idempotence.' : '.'));
    }

    function tag(b) { return idem.checked ? ' (PID ' + b.pid + ', seq ' + b.seq + ')' : ''; }

    function draw() {
      table.innerHTML = '';
      var col = el('div', 'part');
      col.appendChild(el('h4', null, 'Log của partition'));
      var seen = {}, dups = [], prevNum = 0, reordered = false;
      S.log.forEach(function (r, off) {
        var m = el('span', 'msg', 'off ' + off + ' · ' + r.val + (r.pid !== '-' ? ' · PID ' + r.pid + ' seq ' + r.seq : ''));
        if (seen[r.val]) { dups.push(r.val); m.style.outline = '1px solid var(--bad)'; }
        var num = +r.val.replace(/\D/g, '');
        if (!seen[r.val] && num < prevNum) { reordered = true; m.style.outline = '1px dashed var(--bad)'; }
        if (!seen[r.val]) prevNum = Math.max(prevNum, num);
        seen[r.val] = true;
        col.appendChild(m);
      });
      table.appendChild(col);
      var bad = dups.length || reordered;
      ledger.className = 'note' + (bad ? ' bad' : '');
      ledger.textContent = 'Bản ghi trong log: ' + S.log.length + ' · TRÙNG: ' + (dups.length ? dups.join(', ') : 'không') +
        ' · ĐẢO THỨ TỰ: ' + (reordered ? 'có' : 'không') + (idem.checked ? ' · producer ID hiện tại: ' + S.pid : '');
      evBox.innerHTML = '';
      S.events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    draw();
  }
  global.IdempotenceSim = { mount: mount };
})(window);
