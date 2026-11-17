/*
 * Mô phỏng rebalance của một consumer group đọc topic 6 partition (giao thức classic).
 * Chỉ minh hoạ mô hình được tài liệu mô tả, không phải coordinator thật:
 *  - Rebalance khi member vào, rời, chết (hết session.timeout.ms) hoặc không poll trong max.poll.interval.ms (KafkaConsumer javadoc).
 *  - Eager: mọi member trả lại TOÀN BỘ partition rồi mới chia lại; cooperative: chỉ partition thật sự chuyển chủ bị thu hồi
 *    (ConsumerRebalanceListener javadoc).
 *  - Rời êm: commit trong onPartitionsRevoked nên không xử lý lại. Chết hoặc bị đá: offset chưa commit nên record được xử lý lại.
 *  - Static membership (group.instance.id): restart nhanh trong session timeout thì không rebalance.
 * Mỗi partition luôn có 2 record "đã xử lý nhưng chưa commit" để thấy trùng khi partition đổi chủ không êm.
 *
 * Hình động (assets/flow-anim.js, nạp trước file này nếu có): partition bị thu hồi bay từ consumer về coordinator,
 * rồi bay tới chủ mới; partition đang dừng có viền đứt.
 *
 * Dùng: <div id="rsim"></div> <script src="../assets/flow-anim.js"></script> <script src="../assets/rebalance-sim.js"></script>
 *       <script>RebalanceSim.mount(document.getElementById('rsim'));</script>
 */
(function (global) {
  var P = 6, UNCOMMITTED = 2;
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root) {
    var S;
    function fresh() {
      S = { members: ['C1', 'C2'], next: 3, owner: {}, rebalances: 0, paused: 0, dup: 0, events: [], lastPaused: [] };
      assign(true);
      S.rebalances = 0; S.paused = 0; S.lastPaused = [];
      S.events = ['Group có C1, C2 đọc 6 partition.'];
    }

    var box = el('div', 'sim');
    var row = el('div', 'row');
    var strat = el('select');
    [['eager', 'Eager (RangeAssignor)'], ['coop', 'Cooperative (CooperativeStickyAssignor)']].forEach(function (o) {
      var x = el('option', null, o[1]); x.value = o[0]; strat.appendChild(x);
    });
    strat.addEventListener('change', function () { fresh(); draw(); });
    var stat = el('input'); stat.type = 'checkbox';
    var sl = el('label'); sl.appendChild(stat); sl.appendChild(document.createTextNode(' static membership (group.instance.id)'));
    stat.addEventListener('change', function () { fresh(); draw(); });
    [strat, sl].forEach(function (x) { row.appendChild(x); });

    var row2 = el('div', 'row');
    var who = el('select'); who.setAttribute('aria-label', 'consumer');
    row2.appendChild(el('span', null, 'Consumer:'));
    row2.appendChild(who);

    var stageHost = el('div'), cap = el('div', 'fa-cap');
    var view = el('div', 'parts');
    var ledger = el('div', 'note');
    var evBox = el('div'); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row2, stageHost, cap, view, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    // ---------- sân khấu ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 280, label: 'Group coordinator, 6 partition và các consumer trong group.'
    }) : null;
    var pl = global.FlowAnim ? FlowAnim.player(box, st, cap) : null;
    if (pl) row.appendChild(pl.controls());
    if (st) {
      st.node('COORD', { x: 220, y: 6, w: 200, h: 48 });
      for (var p = 0; p < P; p++) st.node('p' + p, { x: 20 + p * 102, y: 92, w: 92, h: 46 });
    }
    // snapshot hình: thành viên (kể cả "bóng" của consumer vừa rời/chết), chủ của partition, partition đang dừng
    function snapshot(o) {
      o = o || {};
      return {
        members: (o.members || S.members).slice().sort(),
        owner: JSON.parse(JSON.stringify(o.owner || S.owner)),
        paused: (o.paused || []).slice(),
        ghost: o.ghost || {},          // tên → 'dead' | 'out'
        rebalances: S.rebalances
      };
    }
    function paint(s) {
      if (!st) return;
      st.setNode('COORD', { title: 'Group coordinator', sub: 'rebalance: ' + s.rebalances + ' · ' + (strat.value === 'eager' ? 'eager' : 'cooperative'), cls: 'hot' });
      var shown = s.members.slice();
      Object.keys(s.ghost).forEach(function (g) { if (shown.indexOf(g) < 0) shown.push(g); });
      shown.sort();
      var k = Math.max(1, shown.length), gap = 12, w = Math.min(150, (600 - (k - 1) * gap) / k);
      var startX = 20 + (600 - (k * w + (k - 1) * gap)) / 2;
      // ẩn mọi consumer, rồi hiện và xếp lại những ai đang hiện
      st._cons = st._cons || {};
      Object.keys(st._cons).forEach(function (c) { st.setNode('c:' + c, { cls: 'gone' }); });
      shown.forEach(function (c, i) {
        if (!st.hasNode('c:' + c)) { st.node('c:' + c, { x: startX, y: 200, w: w, h: 58 }); st._cons[c] = true; }
        st.moveNode('c:' + c, { x: startX + i * (w + gap), y: 200, w: w });
        var mine = []; for (var q = 0; q < P; q++) if (s.owner[q] === c) mine.push('P' + q);
        st.setNode('c:' + c, { title: c + (s.ghost[c] === 'dead' ? ' · CHẾT' : s.ghost[c] === 'out' ? ' · bị đá' : ''),
          sub: s.ghost[c] ? 'không đọc' : (mine.length ? mine.join(' ') : 'nhàn rỗi'),
          cls: (s.ghost[c] === 'dead' ? 'dead' : s.ghost[c] === 'out' ? 'out' : 'client') + (w < 110 ? ' small' : '') });
      });
      st.unlinkAll('L');
      for (var p = 0; p < P; p++) {
        var stopped = s.paused.indexOf(p) >= 0;
        st.setNode('p' + p, { title: 'P' + p, sub: stopped ? 'đang dừng' : (s.owner[p] ? '→ ' + s.owner[p] : 'không ai đọc'), cls: 'small' + (stopped ? ' pending' : '') });
        if (s.owner[p] && shown.indexOf(s.owner[p]) >= 0 && !stopped) st.linkTo('L' + p, 'p' + p, 'c:' + s.owner[p], s.ghost[s.owner[p]] ? '' : 'on');
      }
    }
    function show(o) { var s = snapshot(o); if (pl) pl.call(function () { paint(s); }); }

    [['Thêm consumer', join], ['Rời êm (shutdown)', leave], ['Crash (kill -9)', crash], ['Xử lý quá max.poll.interval.ms', slow], ['Deploy lại (restart nhanh)', restart], ['Làm lại', function () { fresh(); }]]
      .forEach(function (p) {
        var b = el('button', null, p[0]); b.type = 'button';
        b.addEventListener('click', function () { if (pl && p[0] !== 'Làm lại') pl.run(p[1], draw); else { p[1](); draw(); } });
        row2.appendChild(b);
      });

    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 7); if (pl) pl.say(t); }
    function sel() { return who.value; }

    // Chia lại partition. Trả danh sách partition bị dừng trong lần rebalance này.
    function assign(initial) {
      var m = S.members.slice().sort(), old = S.owner, next = {};
      if (!m.length) { S.owner = {}; return []; }
      if (strat.value === 'eager' || initial) {
        // RangeAssignor (một topic): chia các đoạn liên tiếp
        var per = Math.floor(P / m.length), extra = P % m.length, p = 0;
        m.forEach(function (c, i) { for (var k = 0; k < per + (i < extra ? 1 : 0); k++) next[p++] = c; });
      } else {
        // Sticky: giữ partition cũ tới mức cân bằng, chỉ chuyển phần thừa / mồ côi
        var target = {}, per2 = Math.floor(P / m.length), extra2 = P % m.length, load = {}, pool = [];
        m.forEach(function (c, i) { target[c] = per2 + (i < extra2 ? 1 : 0); load[c] = 0; });
        for (var q = 0; q < P; q++) {
          var o = old[q];
          if (o && load[o] != null && load[o] < target[o]) { next[q] = o; load[o]++; } else pool.push(q);
        }
        pool.forEach(function (q) { var c = m.filter(function (x) { return load[x] < target[x]; })[0]; next[q] = c; load[c]++; });
      }
      var paused = [];
      for (var r = 0; r < P; r++) {
        if (strat.value === 'eager' || old[r] !== next[r]) paused.push(r);
      }
      S.owner = next;
      return paused;
    }

    // gone: consumer vừa rời/chết (vẫn vẽ ở bước thu hồi), kind: 'dead' | 'out'
    function rebalance(reason, gone, kind) {
      var old = JSON.parse(JSON.stringify(S.owner));
      var ghost = {}; if (gone) ghost[gone] = kind || 'out';
      var paused = assign(false);
      S.rebalances++; S.paused += paused.length; S.lastPaused = paused;
      // 1) partition bị thu hồi: dừng, bay từ chủ cũ về coordinator
      show({ owner: old, paused: paused, ghost: ghost });
      if (pl) pl.par(paused.filter(function (p) { return old[p]; }).map(function (p) { return ['c:' + old[p], 'COORD', 'P' + p, 'err']; }));
      ev('REBALANCE (' + reason + '): ' + (strat.value === 'eager'
        ? 'mọi consumer trả lại toàn bộ partition, cả group dừng: P' + paused.join(', P') + '.'
        : (paused.length ? 'chỉ thu hồi partition đổi chủ: P' + paused.join(', P') + '; partition khác chạy tiếp.' : 'không partition nào đổi chủ.')));
      // 2) chia lại: bay từ coordinator tới chủ mới
      show({ paused: paused });
      if (pl) pl.par(paused.filter(function (p) { return S.owner[p]; }).map(function (p) { return ['COORD', 'c:' + S.owner[p], 'P' + p, 'fetch']; }));
      show({});
    }

    function owned(c) { var r = []; for (var p = 0; p < P; p++) if (S.owner[p] === c) r.push(p); return r; }

    function join() {
      var c = 'C' + S.next++; S.members.push(c);
      show({}); if (pl) pl.pulse('c:' + c);
      ev(c + ' tham gia group.');
      rebalance(c + ' vào');
    }
    function leave() {
      var c = sel(); if (!c) return;
      S.members = S.members.filter(function (x) { return x !== c; });
      ev(c + ' tắt êm: commit offset trong onPartitionsRevoked rồi rời group ngay. Không xử lý lại.');
      rebalance(c + ' rời', c, 'out');
    }
    function crash() {
      var c = sel(); if (!c) return;
      var ps = owned(c);
      var g = {}; g[c] = 'dead';
      show({ ghost: g, paused: ps }); if (pl) pl.pulse('c:' + c);
      if (stat.checked) {
        ev(c + ' chết (static member): partition P' + ps.join(', P') + ' KHÔNG được chia ngay; không ai đọc chúng cho tới khi hết session.timeout.ms (45 giây mặc định).');
      } else {
        ev(c + ' chết: không còn heartbeat; coordinator chỉ biết sau session.timeout.ms (45 giây mặc định). Trong lúc đó P' + ps.join(', P') + ' không ai đọc.');
      }
      if (pl) pl.wait(600);
      S.members = S.members.filter(function (x) { return x !== c; });
      S.dup += ps.length * UNCOMMITTED;
      ev('Offset chưa commit của ' + c + ' mất: ' + ps.length * UNCOMMITTED + ' record sẽ bị consumer mới xử lý lại (TRÙNG).');
      rebalance(c + ' hết session timeout', c, 'dead');
    }
    function slow() {
      var c = sel(); if (!c) return;
      var ps = owned(c);
      var g = {}; g[c] = 'out';
      show({ ghost: g, paused: ps }); if (pl) pl.pulse('c:' + c);
      if (stat.checked) {
        ev(c + ' không poll kịp (static member): ngừng heartbeat, partition chỉ bị chia lại sau session timeout.');
      }
      ev(c + ' không gọi poll() trong max.poll.interval.ms → tự rời group. Lúc xử lý xong, commit bị từ chối (CommitFailedException).');
      S.members = S.members.filter(function (x) { return x !== c; });
      S.dup += ps.length * UNCOMMITTED;
      rebalance(c + ' bị coi là chết', c, 'out');
      S.members.push(c);
      show({}); if (pl) pl.pulse('c:' + c);
      ev(c + ' gọi poll() lại → tham gia lại group.');
      rebalance(c + ' vào lại');
    }
    function restart() {
      var c = sel(); if (!c) return;
      var ps = owned(c);
      if (stat.checked) {
        var g = {}; g[c] = 'out';
        show({ ghost: g, paused: ps }); if (pl) pl.pulse('c:' + c);
        ev(c + ' restart nhanh với cùng group.instance.id, trong session timeout: KHÔNG rebalance. Chỉ P' + ps.join(', P') + ' dừng trong lúc restart.');
        S.lastPaused = ps; S.paused += ps.length;
        show({}); if (pl) pl.pulse('c:' + c);
        return;
      }
      ev(c + ' restart (dynamic member) = rời êm + vào lại: hai lần rebalance.');
      S.members = S.members.filter(function (x) { return x !== c; });
      rebalance(c + ' rời để restart', c, 'out');
      S.members.push(c);
      show({}); if (pl) pl.pulse('c:' + c);
      rebalance(c + ' vào lại');
    }

    function draw() {
      paint(snapshot());
      cap.textContent = S.events[0] ? '› ' + S.events[0] : '';
      var cur = who.value;
      who.innerHTML = '';
      S.members.slice().sort().forEach(function (c) { var o = el('option', null, c); o.value = c; who.appendChild(o); });
      if (S.members.indexOf(cur) >= 0) who.value = cur;
      view.innerHTML = '';
      S.members.slice().sort().forEach(function (c) {
        var col = el('div', 'part');
        col.appendChild(el('h4', null, c));
        owned(c).forEach(function (p) {
          var m = el('span', 'msg', 'P' + p + (S.lastPaused.indexOf(p) >= 0 ? ' · vừa bị dừng' : ''));
          if (S.lastPaused.indexOf(p) >= 0) m.style.outline = '1px dashed var(--bad)';
          col.appendChild(m);
        });
        if (!owned(c).length) col.appendChild(el('span', 'msg', 'nhàn rỗi'));
        view.appendChild(col);
      });
      if (!S.members.length) view.appendChild(el('div', 'part', 'Group không còn consumer: không ai đọc.'));
      ledger.className = 'note' + (S.dup ? ' bad' : '');
      ledger.textContent = 'Rebalance: ' + S.rebalances + ' · tổng lượt partition bị dừng: ' + S.paused + ' · record xử lý lại (TRÙNG): ' + S.dup;
      evBox.innerHTML = '';
      S.events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    fresh();
    draw();
  }
  global.RebalanceSim = { mount: mount };
})(window);
