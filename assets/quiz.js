/*
 * Quiz widget dùng chung. Phản hồi tức thì, khoá câu sau khi chọn, có điểm tổng và nút làm lại.
 *
 * Dùng:
 *   <div id="quiz"></div>
 *   <script src="../assets/quiz.js"></script>
 *   <script>
 *     Quiz.mount(document.getElementById('quiz'), [
 *       { q: 'Câu hỏi?', options: ['A', 'B', 'C', 'D'], answer: 2, why: 'Giải thích ngắn.' }
 *     ]);
 *   </script>
 *
 * Quy ước viết câu hỏi (theo skill teach): các đáp án dài bằng nhau (số từ, số ký tự),
 * và vị trí đáp án đúng thay đổi giữa các câu. Widget KHÔNG xáo trộn để giữ đúng thiết kế đó.
 */
(function (global) {
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function mount(root, items) {
    var answered = 0, correct = 0, wrong = [];
    var score = el('div', 'quiz-score');

    function render() {
      root.innerHTML = '';
      answered = 0; correct = 0; wrong = [];
      items.forEach(function (item, i) {
        var box = el('div', 'quiz-q');
        box.appendChild(el('p', 'prompt', (i + 1) + '. ' + item.q));
        var why = el('div', 'why');
        why.hidden = true;
        var btns = item.options.map(function (text, j) {
          var b = el('button', 'opt', String.fromCharCode(65 + j) + '. ' + text);
          b.type = 'button';
          b.addEventListener('click', function () {
            btns.forEach(function (x, k) {
              x.disabled = true;
              if (k === item.answer) x.classList.add('correct');
            });
            var ok = j === item.answer;
            if (!ok) b.classList.add('wrong');
            why.hidden = false;
            why.className = 'why ' + (ok ? 'ok' : 'no');
            why.textContent = (ok ? 'Đúng. ' : 'Chưa đúng. ') + item.why;
            answered++; if (ok) correct++; else wrong.push((i + 1) + ' (chọn ' + String.fromCharCode(65 + j) + ', đúng là ' + String.fromCharCode(65 + item.answer) + ')');
            updateScore();
          });
          box.appendChild(b);
          return b;
        });
        box.appendChild(why);
        root.appendChild(box);
      });
      root.appendChild(score);
      updateScore();
    }

    function updateScore() {
      score.innerHTML = '';
      if (answered < items.length) {
        score.textContent = 'Đã trả lời ' + answered + '/' + items.length;
        return;
      }
      score.appendChild(document.createTextNode('Kết quả: ' + correct + '/' + items.length + '. '));
      var again = el('button', null, 'Làm lại');
      again.addEventListener('click', render);
      score.appendChild(again);

      // Dòng tóm tắt để dán cho giáo viên (quiz chạy trong trình duyệt, không gửi gì đi).
      var line = 'Quiz "' + document.title + '": ' + correct + '/' + items.length +
        (wrong.length ? ', sai câu ' + wrong.join(', ') : ', không sai câu nào');
      var box = el('div', null, null);
      box.style.marginTop = '.6rem';
      var input = el('input');
      input.type = 'text'; input.readOnly = true; input.value = line;
      input.style.cssText = 'width:100%;font:inherit;font-size:.85rem;padding:.3rem .5rem';
      input.addEventListener('focus', function () { input.select(); });
      var copy = el('button', null, 'Sao chép để dán cho giáo viên');
      copy.style.marginTop = '.4rem';
      copy.addEventListener('click', function () {
        input.select();
        try { navigator.clipboard.writeText(line); copy.textContent = 'Đã sao chép'; }
        catch (e) { document.execCommand && document.execCommand('copy'); }
      });
      box.appendChild(input); box.appendChild(copy);
      score.appendChild(box);
    }

    render();
  }

  global.Quiz = { mount: mount };
})(window);
