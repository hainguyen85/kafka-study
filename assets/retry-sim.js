/*
 * Mô phỏng retry và DLQ của notification hub, kênh SMS. Tính theo từng giây, 10 phút.
 * Mọi con số (tải, hạn mức, tỉ lệ lỗi, thời gian sập) là GIẢ ĐỊNH để thấy hình dạng vấn đề, không phải số đo thật.
 *
 * Mô hình (theo Bài 11, mục 8 và 9):
 *  - Hai làn, mỗi làn một topic và một consumer group, dùng chung hạn mức nhà cung cấp SMS:
 *    notif.sms.otp (OTP) và notif.sms.tx (biến động số dư, khách thường và khách ưu tiên).
 *  - Router gắn policy vào bản tin: số lần tối đa và hạn chót theo loại bản tin và phân khúc.
 *  - Thang chờ là của làn: OTP 2 giây rồi 4 giây; làn tx cố định 10 giây, hoặc tăng dần 10, 20, 40, 80, tối đa 120 giây.
 *  - Retry topic (non-blocking): bản tin lỗi sang retry topic, partition chính đi tiếp; mất thứ tự. [Spring: retry topic]
 *    Retry tại chỗ (blocking): bản tin lỗi giữ đầu hàng tới hạn chờ, cả làn đứng. (Bài 6)
 *  - Hết lượt: vào DLT, hoặc "log rồi bỏ qua" như recoverer mặc định của Spring (mất im lặng, Bài 6).
 *    OTP hết lượt khi có policy: FAILED, báo nguồn (không vào DLT, Bài 11 mục 9).
 *  - Lỗi vĩnh viễn (số sai): có phân loại thì vào DLT ngay; không phân loại thì bị retry như lỗi tạm thời.
 *  - Ngắt mạch: tỉ lệ lỗi trong một giây từ 50% trở lên thì mở, tạm dừng cả hai làn (pause), 10 giây sau thử lại vài lời gọi.
 *    Bản tin nằm yên trong log, không mất lượt.
 *
 * Dùng: <div id="rsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/retry-sim.js"></script>
 *       <script>RetrySim.mount(document.getElementById('rsim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function fmt(n) { return Math.round(n).toLocaleString('vi-VN'); }
  function clock(t) { var m = Math.floor(t / 60), s = t % 60; return m + ':' + (s < 10 ? '0' : '') + s; }

  var END = 600, STEP = 30;
  var RATE = { otp: 2, reg: 6, vip: 2 };   // bản tin/giây
  var CAP = 40;                            // lời gọi/giây: hạn mức nhà cung cấp SMS
  var FLAKY = 0.1, PERM = 0.02;            // lỗi lẻ tẻ; tỉ lệ số sai của tin số dư
  var OUT = { from: 60, to: 240 };         // SMS sập 3 phút
  var OTP_TTL = 60;                        // hạn OTP rút gọn
  var CB_WAIT = 10, CB_PROBE = 3;
  var NAME = { otp: 'OTP', reg: 'Số dư, khách thường', vip: 'Số dư, khách ưu tiên' };
  var LANE = { otp: 'otp', reg: 'tx', vip: 'tx' };
  var TYPES = ['otp', 'reg', 'vip'];

  function mount(root) {
    var S;
    var box = el('div', 'sim');
    var r1 = el('div', 'row'), r2 = el('div', 'row'), r3 = el('div', 'row'), rb = el('div', 'row');
    function select(aria, list) {
      var s = el('select'); s.setAttribute('aria-label', aria);
      list.forEach(function (o) { var x = el('option', null, o[1]); x.value = o[0]; s.appendChild(x); });
      return s;
    }
    function check(text, on) { var l = el('label'), b = el('input'); b.type = 'checkbox'; b.checked = on; l.appendChild(b); l.appendChild(document.createTextNode(' ' + text)); return { lab: l, box: b }; }
    var mode = select('kiểu thử lại', [['topic', 'Sang hàng thử lại (hàng chính đi tiếp)'], ['block', 'Thử lại tại chỗ (cả hàng đứng chờ)'], ['none', 'Không thử lại']]);
    var ladder = select('thang chờ của hàng giao dịch', [['exp', 'Thang chờ tăng dần: 10, 20, 40, 80, tối đa 120 giây'], ['fixed', 'Thang chờ cố định: 10 giây']]);
    var exhaust = select('hết lượt thì', [['dlt', 'Hết lượt: vào DLQ'], ['skip', 'Hết lượt: chỉ ghi log rồi bỏ']]);
    var policy = check('Policy theo bản tin (số lần theo phân khúc, hạn OTP)', true);
    var classify = check('Lỗi vĩnh viễn vào DLQ ngay', true);
    var cb = check('Ngắt mạch', false);
    var ev = {
      flaky: check('Lỗi lẻ tẻ 10%', true),
      out: check('SMS sập phút 1:00 tới 4:00', true),
      perm: check('2% tin số dư sai số', true)
    };
    r1.appendChild(mode); r1.appendChild(ladder); r1.appendChild(exhaust);
    r2.appendChild(policy.lab); r2.appendChild(classify.lab); r2.appendChild(cb.lab);
    r3.appendChild(el('span', null, 'Sự cố:'));
    Object.keys(ev).forEach(function (k) { r3.appendChild(ev[k].lab); });

    var stageHost = el('div'), cap = el('div', 'fa-cap'), table = el('div'), ledger = el('div', 'note');
    table.style.cssText = 'overflow-x:auto';
    [r1, r2, r3, rb, stageHost, cap, table, ledger].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var st = FlowAnim.stage(stageHost, { width: 640, height: 222, label: 'Router, hàng OTP và hàng giao dịch, ngắt mạch, Sender SMS, nhà mạng SMS, thử lại, DLQ và bộ đếm quá hạn.' });
    var pl = FlowAnim.player(box, st, cap);
    st.node('R', { x: 6, y: 80, w: 96, h: 56 });
    st.node('TO', { x: 118, y: 18, w: 132, h: 52 });
    st.node('TT', { x: 118, y: 100, w: 132, h: 52 });
    st.node('CB', { x: 268, y: 4, w: 118, h: 50 });
    st.node('S', { x: 268, y: 74, w: 118, h: 56 });
    st.node('P', { x: 404, y: 74, w: 118, h: 56 });
    st.node('RT', { x: 268, y: 162, w: 118, h: 56 });
    st.node('DLT', { x: 404, y: 162, w: 118, h: 56 });
    st.node('X', { x: 540, y: 162, w: 94, h: 56 });
    [['l1', 'R', 'TO'], ['l2', 'R', 'TT'], ['l3', 'TO', 'S'], ['l4', 'TT', 'S'], ['l5', 'S', 'P'], ['l6', 'S', 'RT'], ['l7', 'S', 'DLT'], ['l8', 'CB', 'S']]
      .forEach(function (x) { st.linkTo(x[0], x[1], x[2], 'on'); });

    function on(k) { return ev[k].box.checked; }
    function maxAtt(type) { return policy.box.checked ? { otp: 3, reg: 5, vip: 8 }[type] : 5; }
    function deadline(type) { return policy.box.checked && type === 'otp' ? OTP_TTL : Infinity; }
    function delay(lane, n) {   // n: lần thử vừa lỗi (1 = lần giao đầu)
      if (lane === 'otp') return n === 1 ? 2 : 4;
      return ladder.value === 'fixed' ? 10 : Math.min(10 * Math.pow(2, n - 1), 120);
    }
    function per() { var o = {}; TYPES.forEach(function (k) { o[k] = 0; }); return o; }

    function fresh() {
      S = {
        t: 0, lane: { otp: { q: [], hold: null }, tx: { q: [], hold: null } }, rt: [], dlt: [],
        sent: per(), expired: per(), failed: per(), dltN: per(), lost: per(), wait: per(), late: 0,
        calls: 0, retryCalls: 0, wasted: 0, stall: { otp: 0, tx: 0 }, rtMax: 0,
        cb: { state: 'closed', until: 0 }, acc: { otp: 0, tx: 0, perm: 0 },
        ev: [], w: null, tickStall: {}
      };
    }
    function note(t, s) { S.ev.push([t, s]); }

    // ---------- một lời gọi tới nhà cung cấp cho k bản tin cùng nhóm ----------
    function attempt(co, k, ctx) {
      S.calls += k; ctx.calls += k; S.w.calls += k;
      if (co.attempt > 1) S.retryCalls += k;
      if (co.perm && co.attempt > 1 && !classify.box.checked) S.wasted += k;   // phân loại thì số sai chỉ bị gọi lại khi lần trước lỗi vì nhà mạng sập
      var fail;
      if (ctx.down) fail = k;
      else if (co.perm) fail = k;
      else if (on('flaky')) { S.acc[co.lane] += k * FLAKY; fail = Math.min(k, Math.floor(S.acc[co.lane] + 1e-9)); S.acc[co.lane] -= fail; }
      else fail = 0;
      var ok = k - fail;
      if (ok > 0) {
        S.sent[co.type] += ok; S.w.sent += ok;
        S.wait[co.type] = Math.max(S.wait[co.type], ctx.t - co.t0);
        if (co.type === 'otp' && ctx.t - co.t0 > OTP_TTL) S.late += ok;
      }
      ctx.fails += fail; S.w.fail += fail;
      if (fail > 0) failed({ type: co.type, lane: co.lane, t0: co.t0, n: fail, attempt: co.attempt, perm: co.perm }, co.perm && !ctx.down, ctx);
      return fail;
    }
    function failed(f, permanent, ctx) {
      if (permanent && classify.box.checked) return dead(f);
      if (mode.value === 'none' || f.attempt >= maxAtt(f.type)) return dead(f);
      var nf = { type: f.type, lane: f.lane, t0: f.t0, n: f.n, attempt: f.attempt + 1, perm: f.perm, due: ctx.t + delay(f.lane, f.attempt) };
      if (mode.value === 'topic') { S.rt.push(nf); S.w.toRetry += f.n; }
      else S.lane[f.lane].hold = nf;
    }
    function dead(f) {
      S.w.dead += f.n;
      if (f.type === 'otp' && policy.box.checked) { S.failed.otp += f.n; return; }
      if (exhaust.value === 'dlt') { S.dlt.push(f); S.dltN[f.type] += f.n; }
      else S.lost[f.type] += f.n;
    }
    function stale(co, t) { return t - co.t0 > deadline(co.type); }

    // ---------- một làn trong một giây ----------
    function runLane(L, ctx) {
      var lane = S.lane[L];
      if (mode.value === 'topic') {
        S.rt.filter(function (r) { return r.lane === L && r.due <= ctx.t && r.n > 0; })
          .sort(function (a, b) { return a.due - b.due; })
          .forEach(function (r) {
            if (stale(r, ctx.t)) { S.expired[r.type] += r.n; S.w.exp += r.n; r.n = 0; return; }
            var k = Math.min(r.n, ctx.budget); if (k <= 0) return;
            ctx.budget -= k; r.n -= k;
            attempt({ type: r.type, lane: L, t0: r.t0, attempt: r.attempt, perm: r.perm }, k, ctx);
          });
        S.rt = S.rt.filter(function (r) { return r.n > 0; });
      }
      if (mode.value === 'block') {
        // Retry tại chỗ: bản tin lỗi giữ đầu hàng; xử lý từng bản tin, lỗi thì cả làn đứng tới hạn chờ
        while (ctx.budget > 0) {
          if (lane.hold) {
            var h = lane.hold;
            if (stale(h, ctx.t)) { S.expired[h.type] += h.n; S.w.exp += h.n; lane.hold = null; continue; }
            if (h.due > ctx.t) { S.stall[L]++; S.tickStall[L] = true; return; }
            lane.hold = null; ctx.budget -= 1;
            attempt(h, 1, ctx);
            continue;
          }
          var co = lane.q[0]; if (!co) return;
          if (stale(co, ctx.t)) { S.expired[co.type] += co.n; S.w.exp += co.n; lane.q.shift(); continue; }
          ctx.budget -= 1; co.n -= 1; if (co.n <= 0) lane.q.shift();
          attempt({ type: co.type, lane: L, t0: co.t0, attempt: 1, perm: co.perm }, 1, ctx);
        }
        return;
      }
      while (ctx.budget > 0 && lane.q.length) {
        var c = lane.q[0];
        if (stale(c, ctx.t)) { S.expired[c.type] += c.n; S.w.exp += c.n; lane.q.shift(); continue; }
        var k2 = Math.min(c.n, ctx.budget);
        ctx.budget -= k2; c.n -= k2; if (c.n <= 0) lane.q.shift();
        attempt({ type: c.type, lane: L, t0: c.t0, attempt: 1, perm: c.perm }, k2, ctx);
      }
    }

    function tick() {
      var t = S.t;
      // 1. bản tin tới (Router đã gắn policy)
      TYPES.forEach(function (ty) {
        var n = RATE[ty], p = 0;
        if (ty !== 'otp' && on('perm')) { S.acc.perm += n * PERM; p = Math.floor(S.acc.perm + 1e-9); S.acc.perm -= p; }
        var q = S.lane[LANE[ty]].q;
        if (n - p > 0) q.push({ type: ty, lane: LANE[ty], t0: t, n: n - p, perm: false });
        if (p > 0) q.push({ type: ty, lane: LANE[ty], t0: t, n: p, perm: true });
      });
      var down = on('out') && t >= OUT.from && t < OUT.to;
      if (on('out') && t === OUT.from) note(t, 'SMS sập.');
      if (on('out') && t === OUT.to) note(t, 'SMS hoạt động lại.');
      // 2. ngắt mạch
      var budget = CAP, c = S.cb;
      if (cb.box.checked) {
        if (c.state === 'open' && t >= c.until) c.state = 'half';
        if (c.state === 'open') budget = 0;
        if (c.state === 'half') budget = CB_PROBE;
      }
      var ctx = { t: t, down: down, budget: budget, calls: 0, fails: 0 };
      S.tickStall = {};
      if (budget > 0) { runLane('otp', ctx); runLane('tx', ctx); }
      if (cb.box.checked) {
        if (c.state === 'closed' && ctx.calls >= 3 && ctx.fails / ctx.calls >= 0.5) { c.state = 'open'; c.until = t + CB_WAIT; note(t, 'Ngắt mạch mở: tạm dừng cả hai hàng, tin nằm yên trong Kafka.'); }
        else if (c.state === 'half') {
          if (ctx.fails > 0) { c.state = 'open'; c.until = t + CB_WAIT; }
          else { c.state = 'closed'; note(t, 'Ngắt mạch đóng: đọc lại và xả hàng chờ.'); }
        }
      }
      S.rtMax = Math.max(S.rtMax, rtCount());
      S.t++;
    }

    // ---------- tổng hợp ----------
    function rtCount(L) { return S.rt.reduce(function (s, r) { return s + (!L || r.lane === L ? r.n : 0); }, 0); }
    function laneWait(L) { var l = S.lane[L]; return l.q.reduce(function (s, co) { return s + co.n; }, 0) + (l.hold ? l.hold.n : 0); }
    function pending(type) {
      var n = 0;
      ['otp', 'tx'].forEach(function (L) { S.lane[L].q.forEach(function (co) { if (co.type === type) n += co.n; }); var h = S.lane[L].hold; if (h && h.type === type) n += h.n; });
      S.rt.forEach(function (r) { if (r.type === type) n += r.n; });
      return n;
    }
    function oldest(type) {
      var m = null;
      function see(co) { if (co.type === type && co.n > 0 && (m === null || co.t0 < m)) m = co.t0; }
      ['otp', 'tx'].forEach(function (L) { S.lane[L].q.forEach(see); if (S.lane[L].hold) see(S.lane[L].hold); });
      S.rt.forEach(see);
      return m;
    }
    function maxWait(type) { var o = oldest(type); return Math.max(S.wait[type], o === null ? 0 : S.t - 1 - o); }
    function sum(o) { return TYPES.reduce(function (s, k) { return s + o[k]; }, 0); }

    function paint() {
      var last = S.t - 1, down = S.t > 0 && on('out') && last >= OUT.from && last < OUT.to;
      var c = S.cb, cbOpen = cb.box.checked && c.state === 'open';
      st.setNode('R', { title: 'Router', sub: policy.box.checked ? 'gắn policy' : 'chung 5 lần', cls: 'client' });
      st.setNode('TO', { title: 'Hàng OTP', sub: 'chờ ' + fmt(laneWait('otp')), cls: laneWait('otp') > 200 ? 'hot' : '' });
      st.setNode('TT', { title: 'Hàng giao dịch', sub: 'chờ ' + fmt(laneWait('tx')), cls: laneWait('tx') > 500 ? 'hot' : '' });
      st.setNode('CB', { title: 'Ngắt mạch', sub: !cb.box.checked ? 'không dùng' : c.state === 'open' ? 'MỞ: tạm dừng' : c.state === 'half' ? 'thử lại' : 'đóng', cls: !cb.box.checked ? 'out' : c.state === 'open' ? 'bad' : c.state === 'half' ? 'hot' : 'ok' });
      var stalled = S.tickStall.otp || S.tickStall.tx;
      st.setNode('S', { title: 'Sender SMS', sub: cbOpen ? 'tạm dừng' : stalled ? 'đứng chờ' : '≤ ' + CAP + ' tin/giây', cls: stalled ? 'bad' : 'client' });
      st.setNode('P', { title: 'Nhà mạng SMS', sub: down ? 'SẬP' : on('flaky') ? 'lỗi lẻ 10%' : 'ổn định', cls: down ? 'dead' : '' });
      var rn = rtCount();
      st.setNode('RT', {
        title: { topic: 'Hàng thử lại', block: 'Thử tại chỗ', none: 'Không thử lại' }[mode.value],
        sub: mode.value === 'topic' ? 'chờ ' + fmt(rn) : mode.value === 'block' ? 'giữ đầu hàng' : '',
        cls: mode.value === 'none' ? 'out' : rn > 0 || stalled ? 'hot' : ''
      });
      var dn = sum(S.dltN), ln = sum(S.lost);
      st.setNode('DLT', exhaust.value === 'dlt' ? { title: 'DLQ', sub: fmt(dn), cls: dn ? 'bad' : '' } : { title: 'Bỏ qua (mất)', sub: fmt(ln), cls: ln ? 'bad' : 'out' });
      var xn = sum(S.expired) + S.failed.otp;
      st.setNode('X', { title: 'Quá hạn', sub: fmt(xn), cls: xn ? 'hot' : '' });
    }

    function verdicts() {
      var out = [], dn = sum(S.dltN), ln = sum(S.lost);
      var tr = per(); S.dlt.forEach(function (f) { if (!f.perm) tr[f.type] += f.n; });
      var transDlt = sum(tr);
      if (S.stall.tx + S.stall.otp > 0) out.push('Thử lại tại chỗ: hàng giao dịch đứng ' + S.stall.tx + ' giây, hàng OTP ' + S.stall.otp + ' giây; tin phía sau chờ theo dù không lỗi.');
      if (on('out') && transDlt > 0 && !cb.box.checked) out.push(fmt(transDlt) + ' tin hết lượt vì nhà mạng sập: thử lại không thay được ngắt mạch.');
      if (on('out') && policy.box.checked && mode.value === 'topic' && !cb.box.checked && tr.reg > 0 && tr.vip === 0) out.push('Khách ưu tiên (8 lần) chịu được 3 phút sập, khách thường (5 lần) thì không: policy theo phân khúc.');
      if (S.wasted > 0) out.push(fmt(S.wasted) + ' lần gửi phí cho số sai: lỗi vĩnh viễn bị thử lại như lỗi tạm thời.');
      if (ln > 0) out.push(fmt(ln) + ' tin mất im lặng: hết lượt mà không có DLQ.');
      if (S.late > 0) out.push(fmt(S.late) + ' OTP gửi khi đã quá ' + OTP_TTL + ' giây: thiếu hạn chót trong policy.');
      if (S.expired.otp > 0) out.push(fmt(S.expired.otp) + ' OTP quá hạn được đánh dấu hết hạn, không gửi: đúng, nguồn được báo để khách xin mã mới.');
      if (dn > 0 && exhaust.value === 'dlt') out.push('DLQ có ' + fmt(dn) + ' tin: cần cảnh báo, sửa nguyên nhân, rồi "Gửi lại từ DLQ".');
      return out;
    }

    function draw() {
      paint();
      table.innerHTML = '';
      var t = el('table'), h = el('tr');
      ['Loại (policy)', 'Đã gửi', 'Đang chờ', 'Quá hạn', exhaust.value === 'dlt' ? 'DLQ / thất bại' : 'Mất / thất bại', 'Chờ lâu nhất'].forEach(function (x) { h.appendChild(el('th', null, x)); });
      t.appendChild(h);
      TYPES.forEach(function (ty) {
        var tr = el('tr'), w = maxWait(ty);
        var pol = maxAtt(ty) + ' lần' + (deadline(ty) < Infinity ? ', hạn ' + deadline(ty) + ' giây' : '');
        var bad = exhaust.value === 'dlt' ? S.dltN[ty] : S.lost[ty];
        var last = ty === 'otp' && policy.box.checked ? fmt(S.failed.otp) + ' thất bại' : fmt(bad);
        [NAME[ty] + ' (' + pol + ')', fmt(S.sent[ty]), fmt(pending(ty)), fmt(S.expired[ty]), last, clock(w)].forEach(function (x, i) {
          var td = el('td', null, x);
          if (i === 4 && (bad > 0 || (ty === 'otp' && S.failed.otp > 0))) td.style.color = 'var(--bad)';
          if (i === 5 && ty === 'otp' && w > OTP_TTL) td.style.color = 'var(--bad)';
          tr.appendChild(td);
        });
        t.appendChild(tr);
      });
      table.appendChild(t);
      var v = verdicts(), bad = sum(S.lost) > 0 || S.late > 0 || S.wasted > 0 || S.stall.tx > 0 || S.dlt.some(function (f) { return !f.perm; });
      var head = 'Phút ' + clock(S.t) + '/' + clock(END) + '. Lần gửi tới nhà mạng: ' + fmt(S.calls) + (S.calls ? ' (' + Math.round(100 * S.retryCalls / S.calls) + '% là gửi lại)' : '') + '.';
      ledger.className = 'note' + (bad ? ' bad' : '');
      ledger.textContent = (bad ? '✗ ' : '✓ ') + head + (v.length ? ' ' + v.join(' ') : '');
      if (!pl.busy) cap.textContent = S.t ? '› ' + (S.lastNote || '') : 'Chọn kiểu thử lại, thang chờ, policy và sự cố. Đoán xem ai vào DLQ, rồi bấm "+30 giây" hoặc "Chạy hết 10 phút".';
      btnStep.disabled = btnRun.disabled = S.t >= END;
      btnReplay.disabled = !S.dlt.length;
    }

    function windowNote(from, to) {
      var w = S.w, evs = S.ev.filter(function (e) { return e[0] >= from && e[0] < to; }).map(function (e) { return clock(e[0]) + ' ' + e[1]; });
      var s = 'Phút ' + clock(from) + ' tới ' + clock(to) + ': gửi được ' + fmt(w.sent) + ', lỗi ' + fmt(w.fail);
      if (w.toRetry) s += ', sang hàng thử lại ' + fmt(w.toRetry);
      if (w.dead) s += ', hết lượt ' + fmt(w.dead);
      if (w.exp) s += ', quá hạn ' + fmt(w.exp);
      return s + '.' + (evs.length ? ' ' + evs.join(' ') : '');
    }
    function runWindow(n) {
      var from = S.t;
      S.w = { sent: 0, fail: 0, toRetry: 0, dead: 0, exp: 0, calls: 0 };
      for (var i = 0; i < n && S.t < END; i++) tick();
      S.lastNote = windowNote(from, S.t);
      return S.w;
    }

    // ---------- nút ----------
    var btnStep = el('button', null, '+30 giây'); btnStep.type = 'button';
    btnStep.addEventListener('click', function () {
      pl.run(function () {
        var w = runWindow(STEP), paused = cb.box.checked && S.cb.state === 'open';
        pl.par([['R', 'TO', '', 'msg'], ['R', 'TT', '', 'msg']]);
        if (w.calls) {
          pl.par([['TO', 'S', '', 'msg'], ['TT', 'S', '', 'msg']]);
          pl.tok('S', 'P', w.fail > w.sent ? 'lỗi' : 'gửi', w.fail > w.sent ? 'err' : 'msg');
        }
        var after = [];
        if (w.toRetry) after.push(['S', 'RT', 'thử lại', 'err']);
        if (w.dead) after.push(['S', 'DLT', 'hết lượt', 'err']);
        if (after.length) pl.par(after);
        if (paused) pl.pulse('CB');
        pl.call(paint);
        pl.say(S.lastNote, 300);
      }, draw);
    });
    var btnRun = el('button', null, 'Chạy hết 10 phút'); btnRun.type = 'button';
    btnRun.addEventListener('click', function () { runWindow(END - S.t); S.lastNote = 'Chạy tới phút ' + clock(S.t) + '. ' + S.ev.map(function (e) { return clock(e[0]) + ' ' + e[1]; }).slice(-4).join(' '); draw(); });
    var btnReplay = el('button', null, 'Gửi lại từ DLQ'); btnReplay.type = 'button';
    btnReplay.addEventListener('click', function () {
      var back = 0, keep = [];
      S.dlt.forEach(function (f) {
        if (f.perm) { keep.push(f); return; }
        S.lane[f.lane].q.push({ type: f.type, lane: f.lane, t0: f.t0, n: f.n, perm: false }); back += f.n; S.dltN[f.type] -= f.n;
      });
      S.dlt = keep;
      var permLeft = keep.reduce(function (s, f) { return s + f.n; }, 0);
      S.lastNote = 'Gửi lại ' + fmt(back) + ' tin về hàng chính, giữ nguyên mã tin, bắt đầu lượt thử mới.' + (permLeft ? ' ' + fmt(permLeft) + ' tin số sai vẫn ở lại DLQ: phải sửa dữ liệu, gửi lại không giúp.' : '');
      draw();
    });
    var btnReset = el('button', null, 'Làm lại'); btnReset.type = 'button';
    btnReset.addEventListener('click', function () { fresh(); draw(); });
    [btnStep, btnRun, btnReplay, btnReset].forEach(function (b) { rb.appendChild(b); });
    rb.appendChild(pl.controls());

    [mode, ladder, exhaust, policy.box, classify.box, cb.box].concat(Object.keys(ev).map(function (k) { return ev[k].box; })).forEach(function (x) {
      x.addEventListener('change', function () { fresh(); draw(); });
    });
    fresh(); draw();
  }
  global.RetrySim = { mount: mount };
})(window);
