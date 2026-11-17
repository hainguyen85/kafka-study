/*
 * FlowAnim: sân khấu SVG nhỏ cho các mô phỏng. Vẽ các "nút" (producer, broker, consumer...)
 * và cho "token" (message, ack, lỗi, fetch) chạy giữa chúng bằng Web Animations API.
 * Không thư viện ngoài. Màu lấy từ biến CSS trong style.css (sáng/tối).
 * Người dùng bật "giảm chuyển động" trong hệ điều hành thì token không chạy; trạng thái vẫn đổi.
 *
 * Dùng:
 *   var st = FlowAnim.stage(hostEl, { width: 640, height: 250, label: 'Mô tả cho trình đọc màn hình' });
 *   st.node('P', { x: 20, y: 90, w: 120, h: 60 });
 *   st.setNode('P', { title: 'Producer', sub: 'acks=all', cls: '' });   // cls: leader | out | dead | ...
 *   st.tok('P', 'B1', { label: '#3', kind: 'msg' })                      // kind: msg | ack | err | fetch
 *     .then(...);                                                         // Promise xong khi token tới nơi
 *   FlowAnim.sequence([fn1, fn2, ...])   // chạy lần lượt các hàm trả Promise (hoặc không)
 *   st.speed = 2;                        // 0.5, 1, 2
 */
(function (global) {
  var NS = 'http://www.w3.org/2000/svg';
  var BASE_MS = 650;   // thời gian một chặng ở tốc độ 1×

  function svgEl(tag, attrs, parent) {
    var e = document.createElementNS(NS, tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }

  // Mặc định theo hệ điều hành (prefers-reduced-motion); người xem có thể bật/tắt tay bằng motionToggle(),
  // lựa chọn được nhớ trong localStorage của trình duyệt (hỏng thì bỏ qua).
  var KEY = 'flowanim-motion';
  function override() { try { return localStorage.getItem(KEY); } catch (e) { return null; } }
  function osReduced() {
    try { return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (e) { return false; }
  }
  function reducedMotion() {
    var o = override();
    if (o === 'on') return false;
    if (o === 'off') return true;
    return osReduced();
  }
  // Ô chọn "Hiệu ứng chuyển động" đặt vào thanh điều khiển của mô phỏng
  function motionToggle() {
    var lab = document.createElement('label');
    var box = document.createElement('input'); box.type = 'checkbox'; box.checked = !reducedMotion();
    lab.appendChild(box); lab.appendChild(document.createTextNode(' Hiệu ứng chuyển động'));
    lab.title = osReduced() ? 'Hệ điều hành đang bật "giảm chuyển động"; tick để vẫn xem hiệu ứng' : 'Bỏ tick để tắt hiệu ứng';
    box.addEventListener('change', function () { try { localStorage.setItem(KEY, box.checked ? 'on' : 'off'); } catch (e) {} });
    return lab;
  }

  function stage(host, opt) {
    var W = opt.width || 640, H = opt.height || 240;
    var svg = svgEl('svg', { viewBox: '0 0 ' + W + ' ' + H, class: 'fa-stage', role: 'img', 'aria-label': opt.label || '' });
    host.appendChild(svg);
    var layerLinks = svgEl('g', {}, svg), layerNodes = svgEl('g', {}, svg), layerTok = svgEl('g', {}, svg);
    var nodes = {}, texts = {}, links = {};

    // Đường đi giữa hai nút: khác cột thì thẳng, cùng cột thì vòng ra bên trái (đường "ray" sao chép)
    // Đường đi giữa hai nút:
    //  - stage tạo với { rail: true }, hai nút cùng cột, cùng bề rộng (ví dụ broker → broker):
    //    vòng ra bên trái theo "ray" sao chép, để không đi xuyên qua nút ở giữa;
    //  - nút này nằm hẳn trên/dưới nút kia và lệch dọc nhiều hơn ngang: đi từ cạnh dưới/trên;
    //  - còn lại: đi từ cạnh bên.
    function route(a, b) {
      if (opt.rail && Math.abs(a.x - b.x) < 5 && a.w === b.w) {
        var ax = a.x, ay = a.y + a.h / 2, by = b.y + b.h / 2, out = ax - 34;
        return [[ax, ay], [out, ay], [out, by], [ax, by]];
      }
      var acx = a.x + a.w / 2, acy = a.y + a.h / 2, bcx = b.x + b.w / 2, bcy = b.y + b.h / 2;
      var apart = b.y >= a.y + a.h || a.y >= b.y + b.h;
      if (apart && Math.abs(bcy - acy) > Math.abs(bcx - acx) * 0.6) {
        var down = bcy > acy;
        return [[acx, down ? a.y + a.h : a.y], [bcx, down ? b.y : b.y + b.h]];
      }
      var aRight = a.x < b.x;
      return [[aRight ? a.x + a.w : a.x, acy], [aRight ? b.x : b.x + b.w, bcy]];
    }
    function pathD(pts) { return 'M' + pts.map(function (p) { return p[0] + ' ' + p[1]; }).join(' L'); }

    var api = {
      svg: svg, speed: 1,
      node: function (id, o) {
        var g = svgEl('g', { class: 'fa-node' }, layerNodes);
        svgEl('rect', { x: o.x, y: o.y, width: o.w, height: o.h, rx: 6 }, g);
        var t = svgEl('text', { x: o.x + 10, y: o.y + 21, class: 't' }, g);
        var s = svgEl('text', { x: o.x + 10, y: o.y + 40, class: 's' }, g);
        nodes[id] = { g: g, box: { x: o.x, y: o.y, w: o.w, h: o.h }, t: t, s: s };
        return g;
      },
      hasNode: function (id) { return !!nodes[id]; },
      // Dời nút tới chỗ mới (ví dụ xếp lại hàng consumer khi có người vào/ra)
      moveNode: function (id, o) {
        var n = nodes[id]; if (!n) return;
        var b = n.box; if (o.x != null) b.x = o.x; if (o.y != null) b.y = o.y; if (o.w != null) b.w = o.w;
        var r = n.g.querySelector('rect');
        r.setAttribute('x', b.x); r.setAttribute('y', b.y); r.setAttribute('width', b.w);
        n.t.setAttribute('x', b.x + 10); n.t.setAttribute('y', b.y + 21);
        n.s.setAttribute('x', b.x + 10); n.s.setAttribute('y', b.y + 40);
      },
      setNode: function (id, o) {
        var n = nodes[id]; if (!n) return;
        if (o.title != null) n.t.textContent = o.title;
        if (o.sub != null) n.s.textContent = o.sub;
        n.g.setAttribute('class', 'fa-node' + (o.cls ? ' ' + o.cls : ''));
      },
      // Đường nối tĩnh giữa hai nút; setLink đổi lớp (ví dụ 'on' = đường đang dùng)
      link: function (key, from, to) {
        links[key] = svgEl('path', { d: pathD(route(nodes[from].box, nodes[to].box)), class: 'fa-link' }, layerLinks);
      },
      setLink: function (key, cls) { if (links[key]) links[key].setAttribute('class', 'fa-link' + (cls ? ' ' + cls : '')); },
      // Tạo hoặc vẽ lại đường nối (dùng khi nút đổi chỗ hay đổi chủ)
      linkTo: function (key, from, to, cls) {
        if (!nodes[from] || !nodes[to]) return;
        var d = pathD(route(nodes[from].box, nodes[to].box));
        if (!links[key]) links[key] = svgEl('path', {}, layerLinks);
        links[key].setAttribute('d', d);
        links[key].setAttribute('class', 'fa-link' + (cls ? ' ' + cls : ''));
      },
      unlink: function (key) { if (links[key]) { links[key].remove(); delete links[key]; } },
      unlinkAll: function (prefix) { Object.keys(links).forEach(function (k) { if (!prefix || k.indexOf(prefix) === 0) api.unlink(k); }); },
      text: function (id, o) { texts[id] = svgEl('text', { x: o.x, y: o.y, class: 'fa-text' + (o.cls ? ' ' + o.cls : '') }, layerNodes); },
      setText: function (id, s, x) { if (texts[id]) { texts[id].textContent = s; if (x != null) texts[id].setAttribute('x', x); } },

      // Token chạy từ nút này sang nút kia. Hai nút cùng cột (ví dụ broker → broker) thì vòng ra bên trái.
      tok: function (from, to, o) {
        o = o || {};
        var a = nodes[from] && nodes[from].box, b = nodes[to] && nodes[to].box;
        if (!a || !b || reducedMotion()) return Promise.resolve();
        var kind = o.kind || 'msg', label = o.label || '';
        var g = svgEl('g', { class: 'fa-tok ' + kind }, layerTok);
        var w = Math.max(28, label.length * 7 + 16), h = 20;
        svgEl('rect', { x: -w / 2, y: -h / 2, width: w, height: h, rx: 10 }, g);
        var t = svgEl('text', { x: 0, y: 4, 'text-anchor': 'middle' }, g); t.textContent = label;

        var pts = route(a, b);
        if (o.half) {     // rơi giữa đường (ví dụ ack bị mất): dừng ở giữa rồi mờ đi
          var s0 = pts[0], s1 = pts[pts.length - 1];
          pts = [s0, [(s0[0] + s1[0]) / 2, (s0[1] + s1[1]) / 2]];
          o.fade = true;
        }
        if (o.bounce) {   // tới nơi rồi bật ngược lại (bị từ chối)
          var end = pts[pts.length - 1];
          var back = pts[0];
          var near = [end[0] + (back[0] - end[0]) * 0.12, end[1] + (back[1] - end[1]) * 0.12];
          pts = pts.slice(0, -1).concat([near]).concat(pts.slice().reverse().slice(1));
        }
        var segs = pts.length - 1;
        var frames = pts.map(function (p, i) {
          return { transform: 'translate(' + p[0] + 'px,' + p[1] + 'px)', offset: segs ? i / segs : 0, opacity: 1 };
        });
        frames[frames.length - 1].opacity = o.fade ? 0 : 1;
        var dur = BASE_MS * Math.max(1, segs * 0.7) / (api.speed || 1);
        if (!g.animate) { g.remove(); return Promise.resolve(); }
        var anim = g.animate(frames, { duration: dur, easing: 'ease-in-out', fill: 'forwards' });
        return new Promise(function (res) {
          var over = false;
          var done = function () { if (over) return; over = true; g.remove(); res(); };
          anim.onfinish = done; anim.oncancel = done;
          // onfinish chỉ chạy khi trình duyệt vẽ khung hình mới; tab ẩn thì nó tới rất muộn. Hẹn giờ dự phòng.
          setTimeout(done, dur + 150);
        });
      },

      // Nháy viền một nút (ví dụ broker vừa chết, leader mới)
      pulse: function (id) {
        var n = nodes[id]; if (!n || reducedMotion() || !n.g.animate) return Promise.resolve();
        var dur = 420 / (api.speed || 1);
        n.g.style.transformOrigin = (n.box.x + n.box.w / 2) + 'px ' + (n.box.y + n.box.h / 2) + 'px';
        n.g.style.transformBox = 'view-box';
        var a = n.g.animate([{ transform: 'scale(1)' }, { transform: 'scale(1.04)' }, { transform: 'scale(1)' }],
          { duration: dur, easing: 'ease-out' });
        return new Promise(function (res) {
          var over = false, done = function () { if (!over) { over = true; res(); } };
          a.onfinish = done; a.oncancel = done; setTimeout(done, dur + 150);
        });
      },

      wait: function (ms) {
        if (reducedMotion()) return Promise.resolve();
        return new Promise(function (res) { setTimeout(res, ms / (api.speed || 1)); });
      }
    };
    return api;
  }

  // Chạy lần lượt các bước; mỗi bước là hàm, có thể trả Promise.
  function sequence(steps) {
    return steps.reduce(function (p, f) { return p.then(function () { return f(); }); }, Promise.resolve());
  }

  // Bộ phát cho một mô phỏng: logic ghi các bước vào hàng đợi, rồi run() phát lần lượt.
  //   var pl = FlowAnim.player(boxEl, stage, captionEl);
  //   row.appendChild(pl.controls());                       // tốc độ + "Hiệu ứng chuyển động"
  //   pl.run(function () { ...logic, gọi pl.tok/pl.say/pl.call... }, draw);
  function player(box, st, cap) {
    var q = [];
    var p = {
      busy: false,
      call: function (f) { q.push(function () { f(); }); },                       // đổi trạng thái hình tại thời điểm này
      say: function (t, ms) { q.push(function () { if (cap) cap.textContent = '› ' + t; return st && st.wait(ms || 450); }); },
      tok: function (a, b, label, kind, o) {
        q.push(function () { if (!st) return; var x = o || {}; x.label = label; x.kind = kind; return st.tok(a, b, x); });
      },
      par: function (list) {   // nhiều token cùng lúc: [[from, to, label, kind, opt], ...]
        q.push(function () {
          if (!st) return;
          return Promise.all(list.map(function (x) { var o = x[4] || {}; o.label = x[2]; o.kind = x[3]; return st.tok(x[0], x[1], o); }));
        });
      },
      pulse: function (id) { q.push(function () { return st && st.pulse(id); }); },
      wait: function (ms) { q.push(function () { return st && st.wait(ms); }); },
      run: function (action, done) {
        if (p.busy) return;
        q = [];
        action();
        if (!st || !q.length) { done(); return; }
        setBusy(true);
        var fin = function () { setBusy(false); done(); };
        sequence(q).then(fin, fin);
      },
      controls: function () {
        var wrap = document.createElement('span'); wrap.style.cssText = 'display:inline-flex;gap:.5rem;align-items:center;flex-wrap:wrap';
        var sp = document.createElement('select'); sp.setAttribute('aria-label', 'tốc độ hình động');
        [['0.5', 'Chậm 0.5×'], ['1', 'Tốc độ 1×'], ['2', 'Nhanh 2×']].forEach(function (o) {
          var x = document.createElement('option'); x.value = o[0]; x.textContent = o[1]; sp.appendChild(x);
        });
        sp.value = '1';
        sp.addEventListener('change', function () { if (st) st.speed = +sp.value; });
        wrap.appendChild(sp); wrap.appendChild(motionToggle());
        return wrap;
      }
    };
    function setBusy(on) {
      p.busy = on;
      box.classList.toggle('busy', on);
      box.querySelectorAll('button, select, input').forEach(function (x) { x.disabled = on; });
    }
    return p;
  }

  global.FlowAnim = { stage: stage, sequence: sequence, reducedMotion: reducedMotion, motionToggle: motionToggle, player: player };
})(window);
