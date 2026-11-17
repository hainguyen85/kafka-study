/*
 * Mô phỏng độ bền của một partition: 3 replica, acks, min.insync.replicas, kill/start broker, ISR co lại.
 * Chỉ để minh hoạ MÔ HÌNH ĐƯỢC TÀI LIỆU MÔ TẢ (docs "Design: replication"), không phải broker thật:
 *  - Producer ghi vào leader. Với acks=all, ghi chỉ được ack khi mọi replica trong ISR đã có bản ghi.
 *  - Nếu ISR nhỏ hơn min.insync.replicas lúc nhận, broker từ chối (NotEnoughReplicasException).
 *  - Follower chết vẫn nằm trong ISR cho tới khi tụt quá replica.lag.time.max.ms (nút "Chờ 30 giây").
 *  - Leader mới chỉ được bầu từ ISR; unclean.leader.election.enable=true cho phép bầu replica ngoài ISR (mất dữ liệu).
 *  - Kafka thật: follower TỰ fetch liên tục từ leader. Ở đây gom lại thành một nút ("Follower fetch từ leader")
 *    để bạn dừng thời gian ở cửa sổ "leader đã ghi, follower chưa kịp fetch" (ngoài đời thường rất ngắn).
 *
 * Phần hình động (assets/flow-anim.js, nạp trước file này nếu có): mỗi hành động ghi lại các bước
 * (token chạy, đổi trạng thái nút, chú thích) rồi phát lần lượt; không có flow-anim.js thì chỉ hiện bảng như cũ.
 *
 * Dùng:
 *   <div id="sim"></div>
 *   <script src="../assets/flow-anim.js"></script>
 *   <script src="../assets/ack-sim.js"></script>
 *   <script>AckSim.mount(document.getElementById('sim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root) {
    var S, fx = [];
    function fresh() {
      S = { b: [1, 2, 3].map(function (id) { return { id: id, alive: true, log: [] }; }),
            leader: 1, isr: [1, 2, 3], msgs: [], next: 1, offline: false, events: [] };
    }
    fresh();
    var box = el('div', 'sim');
    var row = el('div', 'row');
    var acks = sel([['1', 'acks=1'], ['all', 'acks=all']]);
    var min = sel([['1', 'min.insync.replicas=1'], ['2', 'min.insync.replicas=2'], ['3', 'min.insync.replicas=3']]);
    min.value = '2'; acks.value = 'all';
    var unclean = el('input'); unclean.type = 'checkbox';
    var uLabel = el('label', null); uLabel.appendChild(unclean); uLabel.appendChild(document.createTextNode(' unclean.leader.election.enable'));
    var speed = sel([['0.5', 'Chậm 0.5×'], ['1', 'Tốc độ 1×'], ['2', 'Nhanh 2×']]); speed.value = '1';
    [acks, min, uLabel, speed].forEach(function (x) { row.appendChild(x); });
    if (global.FlowAnim) row.appendChild(FlowAnim.motionToggle());
    var row2 = el('div', 'row');
    var bSend = btn('Gửi message', send);
    var bRep = btn('Follower fetch từ leader', replicate);
    var bWait = btn('Chờ 30 giây', shrink);
    var bReset = btn('Làm lại', function () { fresh(); });
    [bSend, bRep, bWait, bReset].forEach(function (x) { row2.appendChild(x); });
    var row3 = el('div', 'row');
    [1, 2, 3].forEach(function (id) {
      row3.appendChild(btn('Kill B' + id, function () { kill(id); }));
      row3.appendChild(btn('Start B' + id, function () { start(id); }));
    });
    var stageHost = el('div'), cap = el('div', 'fa-cap');
    var view = el('div', 'parts');
    var ledger = el('div', 'note');
    var evBox = el('div', null); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row2, row3, stageHost, cap, view, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    // ---------- sân khấu hình động ----------
    var st = global.FlowAnim ? FlowAnim.stage(stageHost, {
      width: 640, height: 250, rail: true,
      label: 'Producer gửi message tới leader của partition; ba broker B1, B2, B3 giữ bản sao.'
    }) : null;
    if (st) {
      st.node('P', { x: 20, y: 95, w: 150, h: 58 });
      [1, 2, 3].forEach(function (id) { st.node('B' + id, { x: 400, y: 10 + (id - 1) * 80, w: 220, h: 58 }); });
      [1, 2, 3].forEach(function (id) { st.link('P-B' + id, 'P', 'B' + id); });
      [[1, 2], [1, 3], [2, 3]].forEach(function (p) { st.link('B' + p[0] + '-B' + p[1], 'B' + p[0], 'B' + p[1]); });
      st.text('isr', { x: 20, y: 240, cls: 'strong' });
    }
    function snapshot() {
      return { leader: S.leader, isr: S.isr.slice(), alive: S.b.map(function (b) { return b.alive; }),
               logs: S.b.map(function (b) { return b.log.slice(); }), acks: acks.value, min: min.value };
    }
    function paint(s) {
      if (!st) return;
      st.setNode('P', { title: 'Producer', sub: 'acks=' + s.acks, cls: 'client' });
      [1, 2, 3].forEach(function (id) {
        var alive = s.alive[id - 1], inIsr = s.isr.indexOf(id) >= 0, lead = s.leader === id;
        var role = !alive ? 'CHẾT' : lead ? 'LEADER' : inIsr ? 'follower · ISR' : 'follower · ngoài ISR';
        var log = s.logs[id - 1], tail = log.slice(-6).map(function (m) { return '#' + m; }).join(' ');
        st.setNode('B' + id, { title: 'B' + id + '  ' + role, sub: 'log: ' + (log.length > 6 ? '… ' : '') + (tail || '(trống)'),
          cls: !alive ? 'dead' : lead ? 'leader' : inIsr ? '' : 'out' });
      });
      // Đường đang dùng: producer → leader, leader → follower còn sống trong ISR
      [1, 2, 3].forEach(function (id) { st.setLink('P-B' + id, s.leader === id ? 'on' : ''); });
      [[1, 2], [1, 3], [2, 3]].forEach(function (p) {
        var other = p[0] === s.leader ? p[1] : p[1] === s.leader ? p[0] : 0;
        st.setLink('B' + p[0] + '-B' + p[1], other && s.alive[other - 1] && s.isr.indexOf(other) >= 0 ? 'on' : '');
      });
      st.setText('isr', (s.leader ? 'Leader B' + s.leader : 'Leader: none (OFFLINE)') + ' · ISR {' + s.isr.join(',') + '} · min.insync.replicas=' + s.min);
    }
    // Các bước hình động được ghi lại trong lúc chạy logic
    function snap() { var s = snapshot(); fx.push(function () { paint(s); }); }
    function tok(from, to, label, kind, bounce) { fx.push(function () { return st && st.tok(from, to, { label: label, kind: kind, bounce: bounce }); }); }
    function par(list) { fx.push(function () { return st && Promise.all(list.map(function (a) { return st.tok(a[0], a[1], { label: a[2], kind: a[3] }); })); }); }
    function pulse(id) { fx.push(function () { return st && st.pulse(id); }); }

    function sel(opts) { var s = el('select'); opts.forEach(function (o) { var x = el('option', null, o[1]); x.value = o[0]; s.appendChild(x); }); s.addEventListener('change', function () { if (st) st.speed = +speed.value; draw(); }); return s; }
    function btn(t, fn) {
      var b = el('button', null, t); b.type = 'button';
      b.addEventListener('click', function () {
        if (box.classList.contains('busy')) return;
        fx = []; fn();
        if (!st || t === 'Làm lại') { draw(); return; }
        setBusy(true);
        FlowAnim.sequence(fx).then(function () { setBusy(false); draw(); }, function () { setBusy(false); draw(); });
      });
      return b;
    }
    function setBusy(on) {
      box.classList.toggle('busy', on);
      box.querySelectorAll('button, select, input').forEach(function (x) { x.disabled = on; });
    }
    function B(id) { return S.b[id - 1]; }
    function ev(t) {
      S.events.unshift(t); S.events = S.events.slice(0, 6);
      fx.push(function () { cap.textContent = '› ' + t; return st && st.wait(450); });
    }
    function leaderLog() { return S.leader ? B(S.leader).log : []; }

    function send() {
      var m = { id: S.next++, status: 'pending', lost: false };
      S.msgs.push(m);
      if (!S.leader) { m.status = 'failed'; ev('#' + m.id + ': partition không có leader, producer nhận lỗi và sẽ thử lại.'); return; }
      var minISR = +min.value;
      if (acks.value === 'all' && S.isr.length < minISR) {
        m.status = 'failed';
        tok('P', 'B' + S.leader, '#' + m.id, 'err', true);
        ev('#' + m.id + ': ISR=' + S.isr.length + ' < min.insync.replicas=' + minISR + ' → NotEnoughReplicasException, KHÔNG ghi (producer thử lại).');
        return;
      }
      tok('P', 'B' + S.leader, '#' + m.id, 'msg');
      B(S.leader).log.push(m.id);
      snap();
      if (acks.value === '1') {
        m.status = 'acked';
        tok('B' + S.leader, 'P', 'ack #' + m.id, 'ack');
        ev('#' + m.id + ': leader B' + S.leader + ' ghi xong và báo thành công ngay (acks=1).');
      } else { ev('#' + m.id + ': leader B' + S.leader + ' đã ghi, chờ mọi replica trong ISR {' + S.isr.join(',') + '} (acks=all).'); }
      settle();
    }

    function settle() {
      if (!S.leader) return;
      var L = leaderLog();
      S.msgs.forEach(function (m) {
        if (m.status !== 'pending' || L.indexOf(m.id) < 0) return;
        var all = S.isr.every(function (id) { return B(id).alive && B(id).log.indexOf(m.id) >= 0; });
        if (all) {
          m.status = 'acked';
          tok('B' + S.leader, 'P', 'ack #' + m.id, 'ack');
          ev('#' + m.id + ': mọi replica trong ISR đã có → ack.');
        }
      });
    }

    function replicate() {
      if (!S.leader) { ev('Không có leader để sao chép từ.'); return; }
      var L = leaderLog(), moves = [];
      S.b.forEach(function (b) {
        if (b.alive && b.id !== S.leader && S.isr.indexOf(b.id) >= 0) {
          var missing = L.filter(function (x) { return b.log.indexOf(x) < 0; });
          if (missing.length) moves.push(['B' + S.leader, 'B' + b.id, missing.map(function (x) { return '#' + x; }).join(' '), 'fetch']);
          b.log = L.slice();
        }
      });
      if (moves.length) par(moves);
      snap();
      ev('Follower còn sống trong ISR fetch từ leader, bắt kịp cuối log (Kafka thật làm việc này tự động, liên tục).');
      settle();
    }

    function shrink() {
      var before = S.isr.slice();
      S.isr = S.isr.filter(function (id) { return B(id).alive; });
      if (before.length !== S.isr.length) ev('Sau replica.lag.time.max.ms: ISR {' + before.join(',') + '} → {' + S.isr.join(',') + '}.');
      else ev('Không ai bị loại: mọi replica trong ISR còn sống.');
      snap();
      var minISR = +min.value;
      S.msgs.forEach(function (m) {
        if (m.status === 'pending' && S.leader && leaderLog().indexOf(m.id) >= 0 && S.isr.length < minISR) {
          m.status = 'failed';
          tok('B' + S.leader, 'P', 'lỗi #' + m.id, 'err');
          ev('#' + m.id + ': đã append nhưng ISR=' + S.isr.length + ' < ' + minISR + ' → NotEnoughReplicasAfterAppendException. Producer thử lại sẽ làm TRÙNG.');
        }
      });
      settle();
    }

    function elect() {
      var cand = S.isr.filter(function (id) { return B(id).alive; });
      if (cand.length) { S.leader = cand[0]; ev('Bầu leader mới từ ISR: B' + S.leader + '.'); snap(); pulse('B' + S.leader); return; }
      if (unclean.checked) {
        var any = S.b.filter(function (b) { return b.alive; });
        if (any.length) {
          S.leader = any[0].id; S.isr = [S.leader];
          ev('UNCLEAN election: B' + S.leader + ' (ngoài ISR) thành leader. Dữ liệu nó chưa có sẽ MẤT.');
          snap(); pulse('B' + S.leader); return;
        }
      }
      S.leader = 0; ev('Không có replica nào trong ISR còn sống: partition OFFLINE (Leader: none). Chọn nhất quán, không chọn sẵn sàng.');
      snap();
    }

    function kill(id) {
      if (!B(id).alive) return;
      B(id).alive = false;
      snap(); pulse('B' + id);
      ev('B' + id + ' bị kill -9.');
      if (S.leader === id) {
        S.leader = 0; elect();
        if (S.leader) {
          S.msgs.forEach(function (m) {
            if (m.status === 'pending' && leaderLog().indexOf(m.id) < 0) {
              m.status = 'failed';
              ev('#' + m.id + ': chưa ack và leader mới không có → producer hết hạn/thử lại (chưa mất, vì chưa từng báo thành công).');
            }
          });
        }
      }
      settle(); mark();
    }

    function start(id) {
      var b = B(id); if (b.alive) return;
      b.alive = true; snap(); pulse('B' + id);
      ev('B' + id + ' khởi động lại.');
      if (!S.leader) { // partition đang offline: replica trong ISR sống lại làm leader với log của chính nó
        if (S.isr.indexOf(id) >= 0 || unclean.checked) {
          S.leader = id; if (S.isr.indexOf(id) < 0) S.isr = [id];
          ev('B' + id + ' trở thành leader với log nó có.'); snap();
        } else { ev('B' + id + ' không thuộc ISR nên không được làm leader.'); }
      } else {
        // theo kịp: cắt phần log lệch rồi sao chép từ leader, và vào lại ISR
        var L = leaderLog();
        if (L.length) tok('B' + S.leader, 'B' + id, L.slice(-4).map(function (x) { return '#' + x; }).join(' '), 'fetch');
        b.log = L.slice();
        if (S.isr.indexOf(id) < 0) S.isr.push(id);
        snap();
        ev('B' + id + ' theo kịp leader B' + S.leader + ' và vào lại ISR.');
      }
      settle(); mark();
    }

    function mark() {
      var L = leaderLog();
      S.msgs.forEach(function (m) {
        if (m.status === 'acked') m.lost = S.leader ? L.indexOf(m.id) < 0 : m.lost;
      });
    }

    function draw() {
      mark();
      paint(snapshot());
      cap.textContent = S.events[0] ? '› ' + S.events[0] : (st ? 'Bấm "Gửi message" để bắt đầu.' : '');
      view.innerHTML = '';
      S.b.forEach(function (b) {
        var c = el('div', 'part');
        var role = !b.alive ? 'CHẾT' : (b.id === S.leader ? 'LEADER' : (S.isr.indexOf(b.id) >= 0 ? 'follower, trong ISR' : 'follower, ngoài ISR'));
        c.appendChild(el('h4', null, 'B' + b.id + ' · ' + role));
        if (!b.alive) c.style.opacity = '.5';
        b.log.forEach(function (id, off) { c.appendChild(el('span', 'msg', 'off ' + off + ' · #' + id)); });
        view.appendChild(c);
      });
      var acked = S.msgs.filter(function (m) { return m.status === 'acked'; });
      var lost = acked.filter(function (m) { return m.lost; });
      var failed = S.msgs.filter(function (m) { return m.status === 'failed'; });
      var pending = S.msgs.filter(function (m) { return m.status === 'pending'; });
      ledger.className = 'note' + (lost.length ? ' bad' : '');
      ledger.textContent = 'Đã gửi ' + S.msgs.length + ' · đã ack ' + acked.length + ' · chờ ack ' + pending.length + ' · bị từ chối ' + failed.length +
        ' · ACK NHƯNG MẤT: ' + lost.length + (lost.length ? ' (#' + lost.map(function (m) { return m.id; }).join(', #') + ')' : '') +
        (S.leader ? ' · Leader: B' + S.leader + ', ISR {' + S.isr.join(',') + '}' : ' · Leader: none (OFFLINE)');
      evBox.innerHTML = '';
      S.events.forEach(function (t) { evBox.appendChild(el('div', null, '› ' + t)); });
    }
    draw();
  }
  global.AckSim = { mount: mount };
})(window);
