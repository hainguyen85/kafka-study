/*
 * Mô phỏng bài toán Notification hub: OTP, thông báo giao dịch và campaign đi qua Kafka tới ba nhà cung cấp.
 * Tính theo từng giây. Mọi con số (tải, TPS nhà cung cấp, hạn OTP) là GIẢ ĐỊNH để thấy hình dạng vấn đề,
 * không phải số đo thật.
 *
 * Mô hình:
 *  - Thiết kế A: một topic chung, một nhóm worker. Worker đọc theo thứ tự offset trong partition, nên bản tin
 *    đứng đầu chưa gửi được (nhà cung cấp hết hạn mức hoặc đang sập) thì mọi bản tin phía sau phải chờ.
 *    Campaign lấp mọi partition, nên coi cả topic như một hàng FIFO. [KafkaConsumer: fetch sequentially]
 *  - Thiết kế B: mỗi mức ưu tiên một topic, một consumer group, một nhà cung cấp. Hàng này nghẽn không làm hàng
 *    khác chờ.
 *  - Retry tại chỗ: DefaultErrorHandler có back off, giao lại cùng record, partition đứng yên (Bài 6).
 *    Retry topic: chuyển record sang topic retry có mốc thời gian, hết lượt thì DLT; mất thứ tự. [Spring: retry topic]
 *  - Hub kiểm hạn OTP trước khi gửi: OTP quá hạn không gửi nữa (đếm "quá hạn").
 *  - Worker restart: phần đã gửi trong giây đó chưa commit offset, được giao lại (Bài 5).
 *  - Client gọi lại: API Connect timeout phía client rồi gọi lại cùng notificationId.
 *  - Timeout mơ hồ: hub không biết nhà cung cấp đã gửi hay chưa; giả định một nửa thật ra đã gửi.
 *
 * Dùng: <div id="hsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/hub-sim.js"></script>
 *       <script>HubSim.mount(document.getElementById('hsim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }
  function fmt(n) { return Math.round(n).toLocaleString('vi-VN'); }

  var RATE = { otp: 20, tx: 300 };              // bản tin/giây
  var CAMP = { from: 5, to: 10, total: 100000 };  // campaign đổ vào giây 5-9
  var CH = { otp: 'SMS', tx: 'Push', camp: 'Zalo' };
  var TPS = { SMS: 200, Push: 2000, Zalo: 1000 }; // hạn mức nhà cung cấp (giả định)
  var WCAP = { A: 3000, otp: 500, tx: 3000, camp: 2000 };
  var TTL = 30, SLA = 5, END = 60;            // hạn OTP rút gọn cho vừa 60 giây mô phỏng
  var OUT = { from: 10, to: 30 };                 // Zalo sập
  var DELAYS = [2, 4, 8];                          // retry-2000, retry-4000, retry-8000
  var RESTART = 20, CLIENT_DUP = 0.02, AMB = 0.02;
  var NAME = { otp: 'OTP', tx: 'Giao dịch', camp: 'Campaign' };

  function mount(root) {
    var S;
    var box = el('div', 'sim');
    var r1 = el('div', 'row'), r2 = el('div', 'row'), r3 = el('div', 'row'), r4 = el('div', 'row'), rb = el('div', 'row');
    function select(aria, list) {
      var s = el('select'); s.setAttribute('aria-label', aria);
      list.forEach(function (o) { var x = el('option', null, o[1]); x.value = o[0]; s.appendChild(x); });
      return s;
    }
    function check(text, on) { var l = el('label'), b = el('input'); b.type = 'checkbox'; b.checked = on; l.appendChild(b); l.appendChild(document.createTextNode(' ' + text)); return { lab: l, box: b }; }
    var design = select('thiết kế topic', [['A', 'Thiết kế A: một hàng chung, một nhóm worker'], ['B', 'Thiết kế B: mỗi mức ưu tiên một hàng, một nhóm worker']]);
    var retry = select('cách thử lại khi kênh lỗi', [['block', 'Kênh lỗi: thử lại tại chỗ (cả hàng đứng chờ)'], ['topic', 'Kênh lỗi: sang hàng thử lại rồi DLQ (không chặn)']]);
    var dedup = check('Chống trùng theo mã tin', false);
    var amb = select('xử lý khi không rõ đã gửi chưa', [['resend', 'Không rõ đã gửi chưa: gửi lại'], ['drop', 'Không rõ đã gửi chưa: bỏ, không gửi lại'], ['query', 'Không rõ đã gửi chưa: hỏi nhà cung cấp']]);
    var ev = {
      camp: check('Campaign 100.000 bản tin (giây 5)', true),
      out: check('Zalo sập (giây 10 tới 30)', true),
      restart: check('Worker khởi động lại (giây 20)', false),
      client: check('Nguồn gửi lại vì không nhận được trả lời (2% OTP)', false),
      amb: check('SMS hết giờ chờ, không rõ đã gửi chưa (2%)', false)
    };
    r1.appendChild(design); r1.appendChild(retry);
    r2.appendChild(dedup.lab); r2.appendChild(amb);
    r3.appendChild(el('span', null, 'Sự cố:'));
    Object.keys(ev).forEach(function (k) { r3.appendChild(ev[k].lab); });

    var stageHost = el('div'), cap = el('div', 'fa-cap'), table = el('div'), ledger = el('div', 'note');
    table.style.cssText = 'overflow-x:auto';
    [r1, r2, r3, r4, rb, stageHost, cap, table, ledger].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    var st = FlowAnim.stage(stageHost, { width: 640, height: 216, label: 'Ingest, các hàng, nhóm worker, ba nhà cung cấp SMS, Push, Zalo, hàng thử lại và DLQ.' });
    var pl = FlowAnim.player(box, st, cap);
    st.node('IN', { x: 6, y: 80, w: 112, h: 60 });
    st.node('TA', { x: 136, y: 80, w: 134, h: 60 });
    st.node('WA', { x: 288, y: 80, w: 122, h: 60 });
    [['1', 10], ['2', 82], ['3', 154]].forEach(function (p) {
      st.node('T' + p[0], { x: 136, y: p[1], w: 134, h: 56 });
      st.node('W' + p[0], { x: 288, y: p[1], w: 122, h: 56 });
    });
    st.node('SMS', { x: 428, y: 10, w: 98, h: 56 });
    st.node('Push', { x: 428, y: 82, w: 98, h: 56 });
    st.node('Zalo', { x: 428, y: 154, w: 98, h: 56 });
    st.node('RT', { x: 540, y: 40, w: 96, h: 56 });
    st.node('DLT', { x: 540, y: 126, w: 96, h: 56 });

    function on(k) { return ev[k].box.checked; }
    function groups() { return design.value === 'A' ? ['A'] : ['otp', 'tx', 'camp']; }
    function qOf(c) { return design.value === 'A' ? 'A' : c; }

    function fresh() {
      S = {
        t: 0, q: { A: [], otp: [], tx: [], camp: [] }, rt: [], dlt: 0,
        sent: { otp: 0, tx: 0, camp: 0 }, wait: { otp: 0, tx: 0, camp: 0 }, expired: 0,
        dup: { client: 0, restart: 0, amb: 0 }, blockedDup: 0, lost: 0, ambAcc: 0, resolved: 0,
        blocked: {}, sec: {}, note: ''
      };
    }

    // ---------- một giây ----------
    function send(co, k, g) {
      S.sent[co.c] += k;
      S.wait[co.c] = Math.max(S.wait[co.c], S.t - co.t);
      S.sec[g] = (S.sec[g] || 0) + k;
      if (co.dup) S.dup.client += k;
      if (CH[co.c] === 'SMS' && on('amb')) {
        S.ambAcc += k * AMB;
        var w = Math.floor(S.ambAcc); S.ambAcc -= w;
        if (amb.value === 'resend') S.dup.amb += w / 2;
        else if (amb.value === 'drop') S.lost += w / 2;
        else S.resolved += w;
      }
    }
    function tick() {
      var t = S.t, notes = [];
      S.sec = {}; S.blocked = {};
      // 1. bản tin tới
      var arr = [{ c: 'otp', n: RATE.otp }, { c: 'tx', n: RATE.tx }];
      if (on('camp') && t >= CAMP.from && t < CAMP.to) arr.push({ c: 'camp', n: CAMP.total / (CAMP.to - CAMP.from) });
      arr.forEach(function (a) { S.q[qOf(a.c)].push({ c: a.c, t: t, n: a.n }); });
      if (on('client')) {
        var d = RATE.otp * CLIENT_DUP;
        if (dedup.box.checked) S.blockedDup += d;
        else S.q[qOf('otp')].push({ c: 'otp', t: t, n: d, dup: true });
      }
      if (on('camp') && t === CAMP.from) notes.push('Campaign đổ 100.000 bản tin vào trong 5 giây.');
      var down = on('out') && t >= OUT.from && t < OUT.to;
      if (on('out') && t === OUT.from) notes.push('Zalo sập.');
      if (on('out') && t === OUT.to) notes.push('Zalo hoạt động lại.');
      var budget = { SMS: TPS.SMS, Push: TPS.Push, Zalo: down ? 0 : TPS.Zalo };
      // 2. topic retry (consumer riêng, dùng chung hạn mức nhà cung cấp)
      S.rt.forEach(function (r) {
        if (r.due > t || r.n <= 0) return;
        if (down) {
          if (r.attempt >= DELAYS.length) { S.dlt += r.n; r.n = 0; }
          else { r.due = t + DELAYS[r.attempt]; r.attempt++; }
          return;
        }
        var k = Math.min(r.n, budget[CH[r.c]]);
        if (k > 0) { send(r, k, 'rt'); r.n -= k; budget[CH[r.c]] -= k; }
      });
      S.rt = S.rt.filter(function (r) { return r.n > 0; });
      // 3. các hàng chính
      groups().forEach(function (g) {
        var q = S.q[g], capLeft = WCAP[g];
        while (capLeft > 0 && q.length) {
          var co = q[0], ch = CH[co.c];
          if (co.c === 'otp' && t - co.t > TTL) { S.expired += co.n; q.shift(); continue; }
          if (ch === 'Zalo' && down) {
            if (retry.value === 'block') { S.blocked[g] = 'Zalo sập'; break; }
            var mv = Math.min(co.n, capLeft);   // chuyển sang topic retry cũng là đọc rồi ghi lại: tốn công worker
            S.rt.push({ c: co.c, t: co.t, n: mv, attempt: 1, due: t + DELAYS[0] }); co.n -= mv; capLeft -= mv;
            if (co.n <= 1e-9) q.shift();
            continue;
          }
          var k = Math.min(co.n, capLeft, budget[ch]);
          if (k <= 0) { S.blocked[g] = 'chờ hạn mức ' + ch; break; }
          send(co, k, g); co.n -= k; capLeft -= k; budget[ch] -= k;
          if (co.n <= 1e-9) q.shift();
        }
      });
      // 4. worker restart: phần đã gửi trong giây này chưa commit, được giao lại
      if (on('restart') && t === RESTART) {
        var re = 0;
        groups().forEach(function (g) { re += Math.min(S.sec[g] || 0, 500); });
        if (dedup.box.checked) { S.blockedDup += re; notes.push('Worker khởi động lại: ' + fmt(re) + ' tin chưa đánh dấu "đã đọc" được giao lại; trạng thái "đã gửi" chặn cả.'); }
        else { S.dup.restart += re; notes.push('Worker khởi động lại: ' + fmt(re) + ' tin chưa đánh dấu "đã đọc" được giao lại và gửi lần hai.'); }
      }
      if (S.blocked.A) notes.push('Hàng chung đứng chờ: tin đầu hàng ' + S.blocked.A + ', OTP phía sau cũng phải chờ.');
      if (S.blocked.camp === 'Zalo sập') notes.push('Hàng campaign đứng chờ Zalo; OTP và giao dịch vẫn chạy.');
      S.t++;
      S.note = 'Giây ' + t + ': ' + (notes.length ? notes.join(' ') : 'gửi SMS ' + fmt(S.sent.otp) + ', Push ' + fmt(S.sent.tx) + ', Zalo ' + fmt(S.sent.camp) + ' (cộng dồn).');
    }

    // ---------- tổng hợp ----------
    function pending(c) {
      var n = 0;
      ['A', 'otp', 'tx', 'camp'].forEach(function (g) { S.q[g].forEach(function (co) { if (co.c === c) n += co.n; }); });
      S.rt.forEach(function (r) { if (r.c === c) n += r.n; });
      return n;
    }
    function oldest(c) {
      var m = null;
      ['A', 'otp', 'tx', 'camp'].forEach(function (g) { S.q[g].forEach(function (co) { if (co.c === c && co.n > 0 && (m === null || co.t < m)) m = co.t; }); });
      S.rt.forEach(function (r) { if (r.c === c && (m === null || r.t < m)) m = r.t; });
      return m;
    }
    // OTP quá hạn: đã bị bỏ khi tới lượt, cộng số còn nằm trong hàng nhưng đã quá hạn
    function expiredAll() {
      var n = S.expired;
      ['A', 'otp'].forEach(function (g) { S.q[g].forEach(function (co) { if (co.c === 'otp' && S.t - 1 - co.t > TTL) n += co.n; }); });
      return n;
    }
    function maxWait(c) { var o = oldest(c); return Math.max(S.wait[c], o === null ? 0 : S.t - 1 - o); }

    function paint() {
      var A = design.value === 'A', down = on('out') && S.t - 1 >= OUT.from && S.t - 1 < OUT.to && S.t > 0;
      st.setNode('IN', { title: 'Ingest', sub: 'API + Kafka', cls: 'client' });
      ['TA', 'WA'].forEach(function (id) { st.setNode(id, { cls: A ? '' : 'gone' }); });
      ['1', '2', '3'].forEach(function (i) { st.setNode('T' + i, { cls: A ? 'gone' : '' }); st.setNode('W' + i, { cls: A ? 'gone' : '' }); });
      function qn(g) { return S.q[g].reduce(function (s, co) { return s + co.n; }, 0); }
      if (A) {
        st.setNode('TA', { title: 'Hàng chung', sub: 'chờ: ' + fmt(qn('A')), cls: S.blocked.A ? 'bad' : qn('A') > 1000 ? 'hot' : '' });
        st.setNode('WA', { title: 'Worker', sub: S.blocked.A ? 'đứng chờ' : WCAP.A + '/s', cls: S.blocked.A ? 'bad' : 'client' });
      } else {
        [['1', 'otp', 'Hàng OTP'], ['2', 'tx', 'Hàng giao dịch'], ['3', 'camp', 'Hàng campaign']].forEach(function (p) {
          var b = S.blocked[p[1]];
          st.setNode('T' + p[0], { title: p[2], sub: 'chờ: ' + fmt(qn(p[1])), cls: b === 'Zalo sập' ? 'bad' : qn(p[1]) > 1000 ? 'hot' : '' });
          st.setNode('W' + p[0], { title: 'Nhóm ' + { otp: 'OTP', tx: 'giao dịch', camp: 'campaign' }[p[1]], sub: b === 'Zalo sập' ? 'đứng chờ' : WCAP[p[1]] + '/s', cls: b === 'Zalo sập' ? 'bad' : 'client' });
        });
      }
      st.setNode('SMS', { title: 'SMS', sub: 'gửi ' + fmt(S.sent.otp), cls: '' });
      st.setNode('Push', { title: 'Push', sub: 'gửi ' + fmt(S.sent.tx), cls: '' });
      st.setNode('Zalo', { title: 'Zalo', sub: down ? 'SẬP' : 'gửi ' + fmt(S.sent.camp), cls: down ? 'dead' : '' });
      var rtn = S.rt.reduce(function (s, r) { return s + r.n; }, 0);
      st.setNode('RT', { title: 'Thử lại', sub: retry.value === 'topic' ? fmt(rtn) : 'không dùng', cls: retry.value === 'topic' ? (rtn ? 'hot' : '') : 'out' });
      st.setNode('DLT', { title: 'DLQ', sub: retry.value === 'topic' ? fmt(S.dlt) : 'không dùng', cls: S.dlt ? 'bad' : 'out' });
      st.unlinkAll('l');
      if (A) {
        st.linkTo('l0', 'IN', 'TA', 'on'); st.linkTo('l1', 'TA', 'WA', 'on');
        ['SMS', 'Push', 'Zalo'].forEach(function (p, i) { st.linkTo('lp' + i, 'WA', p, 'on'); });
      } else {
        ['1', '2', '3'].forEach(function (i, j) {
          st.linkTo('li' + i, 'IN', 'T' + i, 'on'); st.linkTo('lw' + i, 'T' + i, 'W' + i, 'on');
          st.linkTo('lp' + i, 'W' + i, ['SMS', 'Push', 'Zalo'][j], 'on');
        });
      }
    }

    function draw() {
      paint();
      table.innerHTML = '';
      var t = el('table'), h = el('tr');
      ['Loại', 'Đã gửi', 'Đang chờ', 'Chờ lâu nhất', 'Quá hạn / DLQ'].forEach(function (x) { h.appendChild(el('th', null, x)); });
      t.appendChild(h);
      ['otp', 'tx', 'camp'].forEach(function (c) {
        var tr = el('tr'), w = maxWait(c);
        var last = c === 'otp' ? fmt(expiredAll()) + ' quá hạn' : c === 'camp' ? (retry.value === 'topic' ? fmt(S.dlt) + ' ở DLQ' : '0') : '0';
        [NAME[c] + ' → ' + CH[c], fmt(S.sent[c]), fmt(pending(c)), w + ' giây', last].forEach(function (x, i) {
          var td = el('td', null, x);
          if (c === 'otp' && i === 3 && w > SLA) td.style.color = 'var(--bad)';
          if (c === 'otp' && i === 4 && expiredAll() > 0) td.style.color = 'var(--bad)';
          if (c === 'camp' && i === 4 && S.dlt > 0) td.style.color = 'var(--bad)';
          tr.appendChild(td);
        });
        t.appendChild(tr);
      });
      table.appendChild(t);
      var dupN = S.dup.client + S.dup.restart + S.dup.amb, otpW = maxWait('otp');
      var exp = expiredAll(), bad = dupN >= 0.5 || S.lost >= 0.5 || otpW > SLA || exp > 0 || S.dlt > 0;
      var parts = [];
      parts.push('Giây ' + S.t + '/' + END + '. OTP chờ lâu nhất ' + otpW + ' giây (mục tiêu ≤ ' + SLA + ' giây' + (exp ? ', ' + fmt(exp) + ' OTP đã quá hạn ' + TTL + ' giây' : '') + ').');
      parts.push('Gửi trùng: ' + fmt(dupN) + (dupN >= 0.5 ? ' (nguồn gửi lại ' + fmt(S.dup.client) + ', worker khởi động lại ' + fmt(S.dup.restart) + ', không rõ đã gửi chưa ' + fmt(S.dup.amb) + ')' : '') + '.');
      parts.push('Mất: ' + fmt(S.lost) + (S.lost >= 0.5 ? ' (không rõ đã gửi chưa, không gửi lại; một nửa thật ra chưa tới tay khách)' : '') + '.');
      if (S.blockedDup >= 0.5) parts.push('Chống trùng đã chặn ' + fmt(S.blockedDup) + ' bản tin lặp.');
      if (S.resolved >= 0.5) parts.push('Hỏi nhà cung cấp gỡ được ' + fmt(S.resolved) + ' tin không rõ đã gửi chưa (chỉ làm được khi nhà cung cấp cho tra trạng thái).');
      if (S.dlt > 0) parts.push(fmt(S.dlt) + ' tin campaign nằm ở DLQ: cần cảnh báo và gửi lại khi Zalo lên lại.');
      ledger.className = 'note' + (bad ? ' bad' : '');
      ledger.textContent = (bad ? '✗ ' : '✓ ') + parts.join(' ');
      if (!pl.busy) cap.textContent = S.t ? '› ' + S.note : 'Chọn thiết kế, cách thử lại, chống trùng và sự cố; đoán OTP chờ bao lâu, rồi bấm "+5 giây" hoặc "Chạy tới giây 60".';
      btnStep.disabled = btnRun.disabled = S.t >= END;
      btnReplay.disabled = !(S.dlt > 0);
    }

    // ---------- nút ----------
    function stepAnimated() {
      var n = Math.min(5, END - S.t);
      for (var i = 0; i < n; i++) {
        tick();
        (function (note, A) {
          pl.call(function () { paint(); });
          if (A) pl.par([['IN', 'TA', '', 'msg'], ['WA', 'SMS', '', 'msg'], ['WA', 'Push', '', 'msg']].concat(S.blocked.A ? [] : [['WA', 'Zalo', '', 'msg']]));
          else pl.par([['IN', 'T1', '', 'msg'], ['IN', 'T2', '', 'msg'], ['W1', 'SMS', '', 'msg'], ['W2', 'Push', '', 'msg']].concat(S.blocked.camp ? [] : [['W3', 'Zalo', '', 'msg']]));
          pl.say(note, 250);
        })(S.note, design.value === 'A');
      }
    }
    var btnStep = el('button', null, '+5 giây'); btnStep.type = 'button';
    btnStep.addEventListener('click', function () { pl.run(stepAnimated, draw); });
    var btnRun = el('button', null, 'Chạy tới giây 60'); btnRun.type = 'button';
    btnRun.addEventListener('click', function () { while (S.t < END) tick(); draw(); });
    var btnReplay = el('button', null, 'Gửi lại từ DLQ'); btnReplay.type = 'button';
    btnReplay.addEventListener('click', function () {
      S.q[qOf('camp')].push({ c: 'camp', t: S.t, n: S.dlt }); S.note = 'Gửi lại ' + fmt(S.dlt) + ' tin từ DLQ về hàng campaign.'; S.dlt = 0; draw(); cap.textContent = '› ' + S.note;
    });
    var btnReset = el('button', null, 'Làm lại'); btnReset.type = 'button';
    btnReset.addEventListener('click', function () { fresh(); draw(); });
    [btnStep, btnRun, btnReplay, btnReset].forEach(function (b) { rb.appendChild(b); });
    rb.appendChild(pl.controls());

    [design, retry, amb, dedup.box].concat(Object.keys(ev).map(function (k) { return ev[k].box; })).forEach(function (x) {
      x.addEventListener('change', function () { fresh(); draw(); });
    });
    fresh(); draw();
  }
  global.HubSim = { mount: mount };
})(window);
