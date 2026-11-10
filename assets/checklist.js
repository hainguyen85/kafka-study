/*
 * Checklist cho bài thực hành. Mỗi bước là:
 *   <div class="step"> ... <label class="tick"><input type="checkbox" data-step="1"> Tôi đã xong bước 1</label></div>
 * Tick được nhớ trong localStorage của trình duyệt (chỉ là tiện ích cho người xem; hỏng thì bỏ qua).
 */
(function () {
  var prefix = 'checklist:' + location.pathname + ':';
  function get(k) { try { return localStorage.getItem(prefix + k); } catch (e) { return null; } }
  function set(k, v) { try { localStorage.setItem(prefix + k, v); } catch (e) {} }
  document.querySelectorAll('input[type=checkbox][data-step]').forEach(function (box) {
    var step = box.closest('.step'), k = box.getAttribute('data-step');
    function paint() { if (step) step.classList.toggle('done', box.checked); }
    box.checked = get(k) === '1';
    paint();
    box.addEventListener('change', function () { set(k, box.checked ? '1' : '0'); paint(); });
  });
})();
