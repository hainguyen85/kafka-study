/*
 * Mô phỏng hợp đồng sự kiện (Bài 8d): Schema Registry kiểm tra compatibility khi đăng ký version mới,
 * và consumer với schema của mình đọc message do producer ở version khác ghi.
 * Mô hình theo luật đọc của Avro (reader schema đọc dữ liệu ghi bằng writer schema), rút gọn:
 *  - field có trong reader mà writer không có: phải có default, nếu không thì lỗi READER_FIELD_MISSING_DEFAULT_VALUE;
 *  - field writer có mà reader không có: bị bỏ qua;
 *  - cùng tên mà khác kiểu: TYPE_MISMATCH (không mô phỏng các kiểu được nâng như int → long).
 * Compatibility: BACKWARD = schema mới đọc được dữ liệu của version liền trước; FORWARD = schema liền trước đọc được dữ liệu mới;
 * FULL = cả hai; *_TRANSITIVE = so với MỌI version trước; NONE = không kiểm. [Confluent: Schema evolution]
 * Message mang byte magic 0 + 4 byte schema id; consumer lấy writer schema theo id. [Confluent: Serdes wire format]
 * Không phải registry thật; id được cấp tăng dần.
 *
 * Dùng: <div id="ssim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/schema-sim.js"></script>
 *       <script>SchemaSim.mount(document.getElementById('ssim'));</script>
 */
(function (global) {
  var MODES = ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FULL', 'NONE'];
  var CHANGES = {
    addOpt: { label: 'Thêm channel: string, default "APP"', ok: function (f) { return !has(f, 'channel'); },
      apply: function (f) { return f.concat([{ n: 'channel', t: 'string', d: 'APP' }]); } },
    addReq: { label: 'Thêm branchCode: string, KHÔNG default', ok: function (f) { return !has(f, 'branchCode'); },
      apply: function (f) { return f.concat([{ n: 'branchCode', t: 'string' }]); } },
    dropOpt: { label: 'Bỏ currency (có default "VND")', ok: function (f) { return has(f, 'currency'); },
      apply: function (f) { return f.filter(function (x) { return x.n !== 'currency'; }); } },
    dropReq: { label: 'Bỏ amount (bắt buộc, không default)', ok: function (f) { return has(f, 'amount'); },
      apply: function (f) { return f.filter(function (x) { return x.n !== 'amount'; }); } },
    reqify: { label: 'channel: bỏ default, thành bắt buộc', ok: function (f) { var c = get(f, 'channel'); return c && c.d != null; },
      apply: function (f) { return f.map(function (x) { return x.n === 'channel' ? { n: x.n, t: x.t } : x; }); } },
    retype: { label: 'amount: đổi kiểu long → string', ok: function (f) { var a = get(f, 'amount'); return a && a.t === 'long'; },
      apply: function (f) { return f.map(function (x) { return x.n === 'amount' ? { n: 'amount', t: 'string' } : x; }); } }
  };
  var V1 = [{ n: 'eventId', t: 'string' }, { n: 'accountId', t: 'string' }, { n: 'amount', t: 'long' }, { n: 'currency', t: 'string', d: 'VND' }];
  function has(f, n) { return f.some(function (x) { return x.n === n; }); }
  function get(f, n) { return f.filter(function (x) { return x.n === n; })[0]; }
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function fieldsText(f) { return f.map(function (x) { return x.n + (x.t !== 'string' ? ':' + x.t : '') + (x.d != null ? '=' + x.d : ''); }).join(', '); }
  function hex(id) { var h = id.toString(16); while (h.length < 8) h = '0' + h; return '00 | ' + h.match(/../g).join(' '); }

  // reader đọc dữ liệu do writer ghi: trả { ok, err, notes }
  function canRead(reader, writer) {
    var notes = [];
    for (var i = 0; i < reader.length; i++) {
      var r = reader[i], w = get(writer, r.n);
      if (!w) {
        if (r.d == null) return { ok: false, err: "READER_FIELD_MISSING_DEFAULT_VALUE: '" + r.n + "' không có trong dữ liệu và không có default" };
        notes.push(r.n + ' = ' + r.d + ' (default)');
      } else if (w.t !== r.t) return { ok: false, err: "TYPE_MISMATCH: '" + r.n + "' ghi là " + w.t + ', đọc là ' + r.t };
    }
    writer.forEach(function (w) { if (!get(reader, w.n)) notes.push('bỏ qua ' + w.n); });
    return { ok: true, notes: notes };
  }

  function mount(root) {
    var box = el('div', 'sim');
    var r1 = el('div', 'row'), r2 = el('div', 'row'), r3 = el('div', 'row');
    var mode = el('select'); mode.setAttribute('aria-label', 'compatibility của subject');
    MODES.forEach(function (m) { var o = el('option', null, 'compatibility = ' + m + (m === 'BACKWARD' ? ' (mặc định)' : '')); o.value = m; mode.appendChild(o); });
    var change = el('select'); change.setAttribute('aria-label', 'thay đổi so với version mới nhất');
    var prodV = el('select'); prodV.setAttribute('aria-label', 'version của producer');
    var aV = el('select'); aV.setAttribute('aria-label', 'schema của consumer A');
    var bV = el('select'); bV.setAttribute('aria-label', 'schema của consumer B');
    var stageHost = el('div'), cap = el('div', 'fa-cap'), ledger = el('div', 'note'), tableHost = el('div');
    tableHost.style.cssText = 'overflow-x:auto;font-size:.8rem';
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    r1.appendChild(mode); r1.appendChild(change);
    r3.appendChild(el('span', null, 'Producer dùng')); r3.appendChild(prodV);
    r3.appendChild(el('span', null, 'Consumer A dùng')); r3.appendChild(aV);
    r3.appendChild(el('span', null, 'Consumer B dùng')); r3.appendChild(bV);
    [r1, r2, r3, stageHost, cap, ledger, tableHost, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var S;
    function fresh() { S = { versions: [{ v: 1, id: 1, f: V1 }], nextId: 2, msgs: [], events: [], last: null }; }
    fresh();

    var st = global.FlowAnim ? FlowAnim.stage(stageHost, { width: 640, height: 210, label: 'Producer, Schema Registry, topic, hai consumer.' }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    if (st) {
      st.node('P', { x: 10, y: 10, w: 225, h: 56 });
      st.node('R', { x: 245, y: 10, w: 385, h: 56 });
      st.node('T', { x: 10, y: 82, w: 620, h: 50 });
      st.node('A', { x: 10, y: 148, w: 305, h: 56 });
      st.node('B', { x: 325, y: 148, w: 305, h: 56 });
    }
    function latest() { return S.versions[S.versions.length - 1]; }
    function ver(v) { return S.versions.filter(function (x) { return x.v === +v; })[0]; }
    function byId(id) { return S.versions.filter(function (x) { return x.id === id; })[0]; }

    function fillSelects() {
      var keepC = change.value;
      change.innerHTML = '';
      Object.keys(CHANGES).forEach(function (k) {
        var o = el('option', null, CHANGES[k].label); o.value = k; o.disabled = !CHANGES[k].ok(latest().f); change.appendChild(o);
      });
      var firstOk = Object.keys(CHANGES).filter(function (k) { return CHANGES[k].ok(latest().f); })[0];
      change.value = keepC && CHANGES[keepC].ok(latest().f) ? keepC : firstOk || '';
      [[prodV, latest().v], [aV, 1], [bV, latest().v]].forEach(function (p) {
        var s = p[0], keep = s.value;
        s.innerHTML = '';
        S.versions.forEach(function (x) { var o = el('option', null, 'v' + x.v + ' (id ' + x.id + ')'); o.value = x.v; s.appendChild(o); });
        s.value = keep && ver(keep) && s !== bV && s !== prodV ? keep : p[1];
      });
    }

    function check(cand) {
      var m = mode.value, list = /TRANSITIVE/.test(m) ? S.versions : [latest()], errs = [];
      if (m === 'NONE') return errs;
      list.forEach(function (old) {
        if (m !== 'FORWARD') { var r = canRead(cand, old.f); if (!r.ok) errs.push('đọc dữ liệu v' + old.v + ': ' + r.err); }
        if (m === 'FORWARD' || m === 'FULL') { var r2 = canRead(old.f, cand); if (!r2.ok) errs.push('v' + old.v + ' đọc dữ liệu mới: ' + r2.err); }
      });
      return errs;
    }
    function snapshot() {
      return { versions: S.versions.map(function (x) { return 'v' + x.v + '=id ' + x.id; }), msgs: S.msgs.slice(-6).map(function (m) { return 'off ' + m.off + ' id ' + m.id; }),
        p: prodV.value, a: aV.value, b: bV.value, last: S.last };
    }
    function paint(s) {
      if (!st) return;
      st.setNode('P', { title: 'Producer (KafkaAvroSerializer)', sub: 'đang dùng v' + s.p, cls: 'client' });
      st.setNode('R', { title: 'Registry · subject payments-avro-value · ' + mode.value, sub: s.versions.join(', '),
        cls: s.last === 'reject' ? 'bad' : s.last === 'accept' ? 'ok' : '' });
      st.setNode('T', { title: 'Topic payments-avro (Kafka chỉ lưu byte)', sub: s.msgs.length ? s.msgs.join(' · ') : '(trống)' });
      var res = function (v) {
        if (!S.msgs.length) return { sub: 'schema v' + v + ' · chưa đọc', cls: 'client' };
        var bad = S.msgs.filter(function (m) { return !canRead(ver(v).f, byId(m.id).f).ok; }).length;
        return { sub: 'schema v' + v + ' · ' + (bad ? bad + ' message LỖI giải mã' : 'đọc được hết ' + S.msgs.length), cls: bad ? 'bad' : 'ok' };
      };
      var a = res(s.a), b = res(s.b);
      st.setNode('A', { title: 'Consumer A', sub: a.sub, cls: a.cls });
      st.setNode('B', { title: 'Consumer B', sub: b.sub, cls: b.cls });
    }
    function snap() { var s = snapshot(); if (pl) pl.call(function () { paint(s); }); }
    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 7); if (pl) pl.say(t, 650); }

    function register(onlyCheck) {
      return function () {
        var c = CHANGES[change.value];
        if (!c) { ev('Không còn thay đổi nào áp dụng được cho version mới nhất.'); return; }
        var cand = c.apply(latest().f), errs = check(cand);
        if (pl) pl.tok('P', 'R', onlyCheck ? 'kiểm tra' : 'đăng ký', 'msg');
        if (errs.length) {
          S.last = 'reject';
          if (pl) pl.tok('R', 'P', '409', 'err');
          snap();
          ev((onlyCheck ? 'POST /compatibility: is_compatible=false. ' : 'Registry TỪ CHỐI (409): "Schema being registered is incompatible with an earlier schema". ') + errs.join('; ') + '. Chưa có byte nào vào Kafka.');
          return;
        }
        if (onlyCheck) { S.last = null; if (pl) pl.tok('R', 'P', 'true', 'ack'); snap(); ev('POST /compatibility: is_compatible=true với ' + mode.value + '. Chưa đăng ký gì.'); return; }
        var v = { v: latest().v + 1, id: S.nextId++, f: cand };
        S.versions.push(v); S.last = 'accept';
        if (pl) pl.tok('R', 'P', 'id ' + v.id, 'ack');
        fillSelects(); snap();
        ev('Registry nhận: version ' + v.v + ', schema id ' + v.id + '. Field: ' + fieldsText(cand) + '.' +
          (mode.value === 'NONE' ? ' Mode NONE không kiểm gì: hợp đồng chỉ còn là thoả thuận miệng.' : ''));
        var oldBreak = S.versions.slice(0, -1).filter(function (o) { return !canRead(o.f, cand).ok; }).map(function (o) { return 'v' + o.v; });
        if (oldBreak.length && mode.value !== 'NONE') ev('Lưu ý: consumer còn dùng ' + oldBreak.join(', ') + ' sẽ KHÔNG đọc được message v' + v.v + '. Với ' + mode.value + ', nâng consumer trước, producer sau.');
      };
    }
    function send() {
      var v = ver(prodV.value), m = { off: S.msgs.length, id: v.id, v: v.v };
      S.msgs.push(m);
      if (pl) pl.tok('P', 'T', hex(v.id), 'msg');
      snap();
      ev('Producer v' + v.v + ' gửi offset ' + m.off + ': byte đầu ' + hex(v.id) + ' (magic 0, schema id ' + v.id + '), sau đó là dữ liệu Avro.');
    }
    function read() {
      if (!S.msgs.length) { ev('Topic chưa có message nào.'); return; }
      var m = S.msgs[S.msgs.length - 1], w = byId(m.id);
      [['A', aV], ['B', bV]].forEach(function (c) {
        var r = canRead(ver(c[1].value).f, w.f);
        if (pl) pl.tok('T', c[0], 'id ' + m.id, r.ok ? 'fetch' : 'err');
        ev('Consumer ' + c[0] + ' (v' + c[1].value + ') lấy schema id ' + m.id + ' từ registry rồi đọc offset ' + m.off + ': ' +
          (r.ok ? 'được' + (r.notes.length ? ' (' + r.notes.join(', ') + ')' : '') : 'LỖI ' + r.err + '. Spring: DeserializationException, lỗi fatal, không retry (Bài 6).'));
      });
      snap();
    }
    [['Đăng ký version mới', register(false)], ['Chỉ kiểm tra (POST /compatibility)', register(true)], ['Producer gửi 1 message', send], ['Hai consumer đọc message cuối', read],
     ['Làm lại', null]].forEach(function (b) {
      var x = el('button', null, b[0]); x.type = 'button';
      x.addEventListener('click', function () {
        if (!b[1]) { fresh(); fillSelects(); draw(); return; }
        if (pl) pl.run(b[1], draw); else { b[1](); draw(); }
      });
      r2.appendChild(x);
    });
    if (pl) r2.appendChild(pl.controls());
    mode.addEventListener('change', function () { S.last = null; draw(); });
    [change, prodV, aV, bV].forEach(function (s) { s.addEventListener('change', function () { draw(); }); });

    function draw() {
      paint(snapshot());
      cap.textContent = S.events[0] ? '› ' + S.events[0] : 'v1 đã đăng ký (id 1). Chọn một thay đổi, đoán registry nhận hay từ chối với mode hiện tại, rồi bấm "Đăng ký version mới".';
      var anyBad = S.msgs.some(function (m) { return [aV, bV].some(function (s) { return !canRead(ver(s.value).f, byId(m.id).f).ok; }); });
      ledger.className = 'note' + (anyBad || S.last === 'reject' ? ' bad' : '');
      ledger.textContent = 'Version mới nhất v' + latest().v + ': ' + fieldsText(latest().f);
      tableHost.innerHTML = '';
      if (S.msgs.length) {
        var t = el('table'), h = el('tr');
        ['Offset', 'Byte đầu (magic | id)', 'Ghi bằng', 'Consumer A (v' + aV.value + ')', 'Consumer B (v' + bV.value + ')'].forEach(function (x) { h.appendChild(el('th', null, x)); });
        t.appendChild(h);
        S.msgs.slice(-8).forEach(function (m) {
          var tr = el('tr'), w = byId(m.id);
          [m.off, hex(m.id), 'v' + m.v].forEach(function (x) { tr.appendChild(el('td', null, String(x))); });
          [aV, bV].forEach(function (s) {
            var r = canRead(ver(s.value).f, w.f), td = el('td', null, r.ok ? '✔' + (r.notes.length ? ' ' + r.notes.join(', ') : '') : '✘ ' + r.err.split(':')[0]);
            if (!r.ok) td.style.color = 'var(--bad)';
            tr.appendChild(td);
          });
          t.appendChild(tr);
        });
        tableHost.appendChild(t);
      }
      evBox.innerHTML = '';
      S.events.forEach(function (x) { evBox.appendChild(el('div', null, '› ' + x)); });
    }
    fillSelects();
    draw();
  }
  global.SchemaSim = { mount: mount };
})(window);
