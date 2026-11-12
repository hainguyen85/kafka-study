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
 * Dùng:
 *   <div id="sim"></div>
 *   <script src="../assets/ack-sim.js"></script>
 *   <script>AckSim.mount(document.getElementById('sim'));</script>
 */
(function (global) {
  function el(t, c, x) { var e = document.createElement(t); if (c) e.className = c; if (x != null) e.textContent = x; return e; }

  function mount(root) {
    var S;
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
    [acks, min, uLabel].forEach(function (x) { row.appendChild(x); });
    var row2 = el('div', 'row');
    var bSend = btn('Gửi message', send);
    var bRep = btn('Follower fetch từ leader', replicate);
    var bWait = btn('Chờ 30 giây', shrink);
    var bReset = btn('Làm lại', function () { fresh(); draw(); });
    [bSend, bRep, bWait, bReset].forEach(function (x) { row2.appendChild(x); });
    var row3 = el('div', 'row');
    [1, 2, 3].forEach(function (id) {
      row3.appendChild(btn('Kill B' + id, function () { kill(id); }));
      row3.appendChild(btn('Start B' + id, function () { start(id); }));
    });
    var view = el('div', 'parts');
    var ledger = el('div', 'note');
    var evBox = el('div', null); evBox.style.cssText = 'font-size:.78rem;margin-top:.5rem;color:var(--muted)';
    [row, row2, row3, view, ledger, evBox].forEach(function (x) { box.appendChild(x); });
    root.appendChild(box);

    function sel(opts) { var s = el('select'); opts.forEach(function (o) { var x = el('option', null, o[1]); x.value = o[0]; s.appendChild(x); }); s.addEventListener('change', draw); return s; }
    function btn(t, fn) { var b = el('button', null, t); b.type = 'button'; b.addEventListener('click', function () { fn(); draw(); }); return b; }
    function B(id) { return S.b[id - 1]; }
    function ev(t) { S.events.unshift(t); S.events = S.events.slice(0, 6); }
    function leaderLog() { return S.leader ? B(S.leader).log : []; }

    function send() {
      var m = { id: S.next++, status: 'pending', lost: false };
      S.msgs.push(m);
      if (!S.leader) { m.status = 'failed'; ev('#' + m.id + ': partition không có leader, producer nhận lỗi và sẽ thử lại.'); return; }
      var minISR = +min.value;
      if (acks.value === 'all' && S.isr.length < minISR) {
        m.status = 'failed';
        ev('#' + m.id + ': ISR=' + S.isr.length + ' < min.insync.replicas=' + minISR + ' → NotEnoughReplicasException, KHÔNG ghi (producer thử lại).');
        return;
      }
      B(S.leader).log.push(m.id);
      if (acks.value === '1') { m.status = 'acked'; ev('#' + m.id + ': leader B' + S.leader + ' ghi xong và báo thành công ngay (acks=1).'); }
      else { ev('#' + m.id + ': leader B' + S.leader + ' đã ghi, chờ mọi replica trong ISR {' + S.isr.join(',') + '} (acks=all).'); }
      settle();
    }

    function settle() {
      if (!S.leader) return;
      var L = leaderLog();
      S.msgs.forEach(function (m) {
        if (m.status !== 'pending' || L.indexOf(m.id) < 0) return;
        var all = S.isr.every(function (id) { return B(id).alive && B(id).log.indexOf(m.id) >= 0; });
        if (all) { m.status = 'acked'; ev('#' + m.id + ': mọi replica trong ISR đã có → ack.'); }
      });
    }

    function replicate() {
      if (!S.leader) { ev('Không có leader để sao chép từ.'); return; }
      var L = leaderLog();
      S.b.forEach(function (b) { if (b.alive && b.id !== S.leader && S.isr.indexOf(b.id) >= 0) b.log = L.slice(); });
      ev('Follower còn sống trong ISR fetch từ leader, bắt kịp cuối log (Kafka thật làm việc này tự động, liên tục).');
      settle();
    }

    function shrink() {
      var before = S.isr.slice();
      S.isr = S.isr.filter(function (id) { return B(id).alive; });
      if (before.length !== S.isr.length) ev('Sau replica.lag.time.max.ms: ISR {' + before.join(',') + '} → {' + S.isr.join(',') + '}.');
      else ev('Không ai bị loại: mọi replica trong ISR còn sống.');
      var minISR = +min.value;
      S.msgs.forEach(function (m) {
        if (m.status === 'pending' && S.leader && leaderLog().indexOf(m.id) >= 0 && S.isr.length < minISR) {
          m.status = 'failed';
          ev('#' + m.id + ': đã append nhưng ISR=' + S.isr.length + ' < ' + minISR + ' → NotEnoughReplicasAfterAppendException. Producer thử lại sẽ làm TRÙNG.');
        }
      });
      settle();
    }

    function elect() {
      var cand = S.isr.filter(function (id) { return B(id).alive; });
      if (cand.length) { S.leader = cand[0]; ev('Bầu leader mới từ ISR: B' + S.leader + '.'); return; }
      if (unclean.checked) {
        var any = S.b.filter(function (b) { return b.alive; });
        if (any.length) { S.leader = any[0].id; S.isr = [S.leader]; ev('UNCLEAN election: B' + S.leader + ' (ngoài ISR) thành leader. Dữ liệu nó chưa có sẽ MẤT.'); return; }
      }
      S.leader = 0; ev('Không có replica nào trong ISR còn sống: partition OFFLINE (Leader: none). Chọn nhất quán, không chọn sẵn sàng.');
    }

    function kill(id) {
      if (!B(id).alive) return;
      B(id).alive = false; ev('B' + id + ' bị kill -9.');
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
      b.alive = true; ev('B' + id + ' khởi động lại.');
      if (!S.leader) { // partition đang offline: replica trong ISR sống lại làm leader với log của chính nó
        if (S.isr.indexOf(id) >= 0 || unclean.checked) { S.leader = id; if (S.isr.indexOf(id) < 0) S.isr = [id]; ev('B' + id + ' trở thành leader với log nó có.'); }
        else { ev('B' + id + ' không thuộc ISR nên không được làm leader.'); }
      } else {
        // theo kịp: cắt phần log lệch rồi sao chép từ leader, và vào lại ISR
        b.log = leaderLog().slice();
        if (S.isr.indexOf(id) < 0) S.isr.push(id);
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
