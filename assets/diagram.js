/* Diagram: vẽ sơ đồ Mermaid trong lesson, reference và portal.
 *
 * Cách dùng trong HTML (cùng quy ước với scripts/html2md.py, nên bản .md cũng có sơ đồ):
 *   <figure class="diagram">
 *   <script type="text/mermaid">
 *   sequenceDiagram
 *     A->>B: gửi
 *   </script>
 *   <figcaption>Chú thích.</figcaption>
 *   </figure>
 *   <script src="../assets/diagram.js"></script>
 *
 * Trang tự vẽ khi tải. Portal gọi Diagram.render(root) sau mỗi lần đổi nội dung;
 * khi đó nó nhận cả <pre><code class="language-mermaid"> do marked sinh ra.
 *
 * Màu lấy từ biến CSS của assets/style.css (sáng/tối). Trong nguồn sơ đồ, dùng
 * `class X bad` / `class X ok` / `class X warn` cho nút; classDef ghi màu sáng để
 * GitHub vẫn đọc được, component thay bằng màu theo theme hiện tại.
 */
(function () {
  var SRC = 'https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.min.js';
  var loading = null, seq = 0, boxes = [];

  function load() {
    if (window.mermaid) return Promise.resolve(window.mermaid);
    if (!loading) loading = new Promise(function (ok, fail) {
      var s = document.createElement('script');
      s.src = SRC;
      s.onload = function () { ok(window.mermaid); };
      s.onerror = fail;
      document.head.appendChild(s);
    });
    return loading;
  }

  function v(name, fallback) {
    var x = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return x || fallback;
  }

  function theme() {
    var t = {
      bg: v('--bg', '#fffdf8'), fg: v('--fg', '#1c1b19'), muted: v('--muted', '#6b665c'),
      rule: v('--rule', '#ddd6c6'), accent: v('--accent', '#1d5fa8'),
      ok: v('--ok', '#1b7a3d'), okBg: v('--ok-bg', '#e6f4ea'),
      bad: v('--bad', '#b3261e'), badBg: v('--bad-bg', '#fbe9e7'),
      card: v('--card', '#fbf8ef'), ctrl: v('--ctrl', '#dfeaf7'), node: v('--node', '#efe7d2')
    };
    return t;
  }

  function init(m, t) {
    m.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      theme: 'base',
      fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif',
      flowchart: { curve: 'basis', padding: 12 },
      // width nhỏ + wrap: chữ dài xuống dòng thay vì kéo giãn khoảng cách giữa các bên
      sequence: { mirrorActors: false, wrap: true, width: 120, actorMargin: 30, noteMargin: 8, boxMargin: 6, messageMargin: 30 },
      themeVariables: {
        fontSize: '14px',
        background: t.bg,
        primaryColor: t.node, primaryTextColor: t.fg, primaryBorderColor: t.muted,
        secondaryColor: t.ctrl, tertiaryColor: t.card,
        lineColor: t.muted, textColor: t.fg, mainBkg: t.node, nodeBorder: t.muted,
        clusterBkg: t.card, clusterBorder: t.rule, titleColor: t.fg, edgeLabelBackground: t.bg,
        actorBkg: t.ctrl, actorBorder: t.accent, actorTextColor: t.fg, actorLineColor: t.rule,
        signalColor: t.fg, signalTextColor: t.fg,
        labelBoxBkgColor: t.card, labelBoxBorderColor: t.rule, labelTextColor: t.fg, loopTextColor: t.fg,
        noteBkgColor: t.card, noteTextColor: t.fg, noteBorderColor: t.rule,
        activationBkgColor: t.node, activationBorderColor: t.muted,
        sequenceNumberColor: t.bg
      }
    });
  }

  // classDef bad/ok/warn trong nguồn mang màu sáng; đổi theo theme đang dùng.
  function recolor(src, t) {
    var map = {
      bad: 'fill:' + t.badBg + ',stroke:' + t.bad + ',color:' + t.fg,
      ok: 'fill:' + t.okBg + ',stroke:' + t.ok + ',color:' + t.fg,
      warn: 'fill:' + t.ctrl + ',stroke:' + t.accent + ',color:' + t.fg
    };
    return src.replace(/^(\s*classDef\s+)(bad|ok|warn)\s+.*$/gm, function (_, head, name) {
      return head + name + ' ' + map[name];
    });
  }

  function collect(root) {
    var found = [];
    root.querySelectorAll('script[type="text/mermaid"], pre > code.language-mermaid').forEach(function (el) {
      var host = el.tagName === 'CODE' ? el.parentNode : el;
      var box = document.createElement('div');
      box.className = 'mermaid-out';
      box.dataset.src = el.textContent.replace(/^\n+/, '').replace(/\s+$/, '');
      host.parentNode.insertBefore(box, host);
      host.parentNode.removeChild(host);
      found.push(box);
    });
    return found;
  }

  // Sơ đồ rộng hơn khung thì bị thu nhỏ; bấm vào để xem đúng cỡ (cuộn ngang), bấm lần nữa để thu lại.
  function zoomable(box) {
    var svg = box.querySelector('svg');
    if (!svg || !svg.viewBox || !svg.viewBox.baseVal) return;
    var w = svg.viewBox.baseVal.width;
    box.classList.toggle('can-zoom', w > box.clientWidth + 4);
    box.title = box.classList.contains('can-zoom') ? 'Bấm để xem đúng cỡ' : '';
    if (box.dataset.zoomBound) return;
    box.dataset.zoomBound = '1';
    box.addEventListener('click', function () {
      if (!box.classList.contains('can-zoom') && !box.classList.contains('zoom')) return;
      var s = box.querySelector('svg'); if (!s) return;
      var on = box.classList.toggle('zoom');
      s.style.maxWidth = on ? 'none' : s.viewBox.baseVal.width + 'px';
      s.style.width = on ? s.viewBox.baseVal.width + 'px' : '';
      box.title = on ? 'Bấm để thu lại' : 'Bấm để xem đúng cỡ';
    });
  }

  function draw(m, list, t) {
    return list.reduce(function (p, box) {
      return p.then(function () {
        var id = 'dgm' + (++seq);
        return m.render(id, recolor(box.dataset.src, t)).then(function (r) {
          box.innerHTML = r.svg;
          box.classList.remove('mermaid-err');
          zoomable(box);
        }, function (err) {
          var junk = document.getElementById('d' + id);
          if (junk) junk.remove();
          box.classList.add('mermaid-err');
          box.innerHTML = '';
          var pre = document.createElement('pre');
          pre.textContent = box.dataset.src;
          var msg = document.createElement('p');
          msg.textContent = 'Không vẽ được sơ đồ: ' + ((err && err.message) || err);
          box.appendChild(msg); box.appendChild(pre);
        });
      });
    }, Promise.resolve());
  }

  function render(root) {
    var list = collect(root || document);
    if (!list.length) return Promise.resolve();
    boxes = boxes.filter(function (b) { return b.isConnected; }).concat(list);
    return load().then(function (m) {
      var t = theme(); init(m, t);
      return draw(m, list, t);
    }, function () {
      list.forEach(function (box) {
        box.classList.add('mermaid-err');
        var pre = document.createElement('pre');
        pre.textContent = box.dataset.src;
        box.innerHTML = '<p>Không tải được Mermaid (cần mạng). Nguồn sơ đồ:</p>';
        box.appendChild(pre);
      });
    });
  }

  // Đổi sáng/tối thì vẽ lại bằng màu mới.
  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: dark)');
    var redraw = function () {
      boxes = boxes.filter(function (b) { return b.isConnected; });
      if (!boxes.length || !window.mermaid) return;
      var t = theme(); init(window.mermaid, t); draw(window.mermaid, boxes, t);
    };
    if (mq.addEventListener) mq.addEventListener('change', redraw); else if (mq.addListener) mq.addListener(redraw);
    // Trang nhúng (ví dụ artifact) có thể đổi theme bằng data-theme trên <html>
    if (window.MutationObserver) {
      new MutationObserver(redraw).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    }
  }

  window.Diagram = { render: render };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { render(document); });
  else render(document);
})();
