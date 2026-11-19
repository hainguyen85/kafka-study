#!/usr/bin/env python3
"""Gom mọi usecase trong lessons/*.html thành một trang tra cứu: reference/0005-usecase-index.html.

Chạy (từ gốc workspace), TRƯỚC html2md.py và build_portal.mjs:
    python scripts/build_usecases.py

Quy ước để usecase được gom:
  - Bài có một <h2> chứa chữ "Usecase"; mỗi usecase là một <h3>(a) Miền: tình huống</h3> trong mục đó.
  - Script gắn id="uc-a", id="uc-b"... cho các <h3> đó (để link thẳng tới đúng usecase) và thêm một dòng
    trỏ về trang tổng hợp ngay dưới <h2>. Chạy lại nhiều lần không sao.
  - "Chuyện gì xảy ra" = 1-2 câu đầu của đoạn đầu tiên; "Cách xử lý" = câu bắt đầu bằng "Xử lý", "Sửa",
    "Cách xử lý" (nếu có). Không thấy thì để trống: đọc trong bài.
Không sửa trang tổng hợp bằng tay: lần chạy sau sẽ ghi đè.
"""
import html
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Thứ tự giống portal (scripts/build_portal.mjs, PARTS)
ORDER = ['0001-kafka-big-picture', '0002-key-partition-offset', '0003-build-cluster',
         '0004-producer-durability', '0010-idempotent-producer',
         '0005-consumer-offset-commit', '0011-rebalance', '0012-delivery-semantics',
         '0006-error-handling-dlt', '0007-idempotent-consumer-transactions',
         '0008-topic-design', '0014-throughput-latency', '0015-replay-retention',
         '0016-schema-evolution', '0009-final-design-test', '0017-notification-hub']
OUT = ROOT / 'reference' / '0005-usecase-index.html'
INDEX_LINK = '<p class="uc-index-link"><a href="../reference/0005-usecase-index.html">Xem mọi usecase của khoá học</a></p>'


def text_of(fragment):
    fragment = re.sub(r'<figure.*?</figure>', ' ', fragment, flags=re.S)
    fragment = re.sub(r'<pre.*?</pre>', ' ', fragment, flags=re.S)
    t = html.unescape(re.sub(r'<[^>]+>', '', fragment))
    return re.sub(r'\s+', ' ', t).strip()


def sentences(t):
    return [x.strip() for x in re.split(r'(?<=[.!?)])\s+(?=[A-ZĐÂĂÊÔƠƯÁÀẢÃẠÉÈẺẼẸÍÌỈĨỊÓÒỎÕỌÚÙỦŨỤÝỲỶỸỴ"(])', t) if x.strip()]


def clip(t, n=260):
    return t if len(t) <= n else t[:n].rsplit(' ', 1)[0] + ' …'


def scan(stem):
    p = ROOT / 'lessons' / f'{stem}.html'
    s = p.read_text(encoding='utf-8')
    title = re.search(r'<title>(.*?)</title>', s).group(1)
    m = re.search(r'<h2>[^<]*Usecase[^<]*</h2>', s)
    if not m:
        return title, []
    sec_start, sec_end = m.end(), s.find('<h2', m.end())
    if sec_end < 0:
        sec_end = len(s)
    # dòng trỏ về trang tổng hợp, ngay dưới <h2>
    if 'class="uc-index-link"' not in s[m.end():m.end() + 300]:
        s = s[:m.end()] + '\n' + INDEX_LINK + s[m.end():]
        sec_end += len(INDEX_LINK) + 1
    rows = []
    heads = list(re.finditer(r'<h3( id="[^"]*")?>(.*?)</h3>', s[sec_start:sec_end], flags=re.S))
    out, cursor = [], 0
    section = s[sec_start:sec_end]
    for k, h in enumerate(heads):
        raw_title = text_of(h.group(2))
        lm = re.match(r'\(([a-z])\)\s*(.*)', raw_title)
        if not lm:
            continue                                  # ví dụ "Hỏi nhanh, đáp gọn"
        letter, name = lm.group(1), lm.group(2)
        body_end = heads[k + 1].start() if k + 1 < len(heads) else len(section)
        body = section[h.end():body_end]
        first_p = re.search(r'<p>(.*?)</p>', body, flags=re.S)
        what = clip(' '.join(sentences(text_of(first_p.group(1)))[:2])) if first_p else ''
        fix = ''
        for sent in sentences(text_of(body)):
            if re.match(r'(Cách xử lý|Xử lý|Sửa)\b', sent):
                fix = clip(re.sub(r'^(Cách xử lý|Xử lý|Sửa)\s*(theo mức)?\s*:?\s*', '', sent), 220)
                break
        if name.startswith('Nếu là ngân hàng'):
            # usecase "ngân hàng" là một dòng cấu hình tham chiếu: đưa nó vào cột cách xử lý
            fix = clip(text_of(first_p.group(1)), 300) if first_p else fix
            what = 'Cấu hình tham chiếu cho sự kiện tiền.'
        domain = name.split(':', 1)[0].strip() if ':' in name else name
        rows.append({'id': f'uc-{letter}', 'letter': letter, 'name': name, 'domain': domain, 'what': what, 'fix': fix})
        # gắn id cho <h3>
        if not h.group(1):
            out.append((h.start(), h.end(), f'<h3 id="uc-{letter}">{h.group(2)}</h3>'))
    for a, b, rep in reversed(out):
        section = section[:a] + rep + section[b:]
    s = s[:sec_start] + section + s[sec_end:]
    p.write_text(s, encoding='utf-8', newline='\n')
    return title, rows


def main():
    groups, total = [], 0
    for stem in ORDER:
        if not (ROOT / 'lessons' / f'{stem}.html').exists():
            continue
        title, rows = scan(stem)
        if rows:
            groups.append((stem, title, rows))
            total += len(rows)
    e = lambda x: html.escape(x, quote=False)
    body = []
    for stem, title, rows in groups:
        body.append(f'<h2>{e(title)}</h2>\n<table class="uc">\n<tr><th>Usecase</th><th>Chuyện gì xảy ra</th><th>Cách xử lý</th></tr>')
        for r in rows:
            body.append(f'<tr><td><a href="../lessons/{stem}.html#{r["id"]}">({r["letter"]}) {e(r["name"])}</a></td>'
                        f'<td>{e(r["what"])}</td><td>{e(r["fix"]) or "<i>đọc trong bài</i>"}</td></tr>')
        body.append('</table>')
    page = f'''<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tổng hợp usecase</title>
<link rel="stylesheet" href="../assets/style.css">
<style>
main {{ max-width: 64rem; }}
table.uc {{ font-size: .82rem; }}
table.uc td:first-child {{ width: 26%; font-weight: 600; }}
table.uc td:last-child {{ width: 30%; }}
#ucFilter {{ font: inherit; font-size: .9rem; width: 100%; max-width: 28rem; padding: .4rem .6rem; border: 1px solid var(--rule); border-radius: 6px; background: var(--bg); color: var(--fg); }}
</style>
</head>
<body>
<main>
<nav class="crumbs"><a href="../README.md">Kafka</a> / Bảng tra · <a href="0003-tricky-scenarios.html">Sổ tay usecase (triệu chứng, nguyên nhân, code)</a></nav>
<h1>Tổng hợp usecase</h1>
<p class="lede">{total} usecase từ {len(groups)} bài, theo thứ tự học. Bấm tên usecase để mở đúng chỗ trong bài, nơi có sơ đồ và giải thích đầy đủ.</p>
<p><input id="ucFilter" type="search" placeholder="Lọc theo từ khoá: thanh toán, rebalance, acks, DLT..." aria-label="Lọc usecase"></p>
<p>Trang này do <code>scripts/build_usecases.py</code> sinh từ các bài học; cột "Cách xử lý" lấy câu bắt đầu bằng "Xử lý" hoặc "Sửa" trong usecase.</p>

{chr(10).join(body)}
</main>
<script>
(function () {{
  var box = document.getElementById('ucFilter'); if (!box) return;
  box.addEventListener('input', function () {{
    var q = box.value.trim().toLowerCase();
    document.querySelectorAll('table.uc tr').forEach(function (tr, i) {{
      if (!tr.querySelector('td')) return;
      tr.hidden = q && tr.textContent.toLowerCase().indexOf(q) < 0;
    }});
  }});
}})();
</script>
</body>
</html>
'''
    OUT.write_text(page, encoding='utf-8', newline='\n')
    print(f'{OUT.relative_to(ROOT)}: {total} usecase, {len(groups)} bài')


if __name__ == '__main__':
    main()
