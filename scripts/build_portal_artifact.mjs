#!/usr/bin/env node
// Đóng gói portal/index.html thành MỘT file tự chứa để publish làm Claude artifact.
//   node scripts/build_portal.mjs            (trước: sinh portal/course.js)
//   node scripts/build_portal_artifact.mjs   (sau: ra dist/kafka-portal.html)
//
// Khác với portal chạy tại chỗ:
//   - style.css, course.js, quiz.js, các mô phỏng, diagram.js được nhúng thẳng vào file
//     (trang artifact không đọc được ../assets/);
//   - không có <!doctype>/<html>/<head>/<body>: nền tảng tự bọc khung trang;
//   - link tới file trong repo (labs/, research/, scripts/...) không mở được trên artifact,
//     nên đổi thành chữ thường kèm tooltip đường dẫn.
// Thư viện ngoài giữ dạng CDN (marked, mermaid trên cdn.jsdelivr.net, nằm trong danh sách cho phép).
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const src = read('portal/index.html');

const pick = (re, what) => { const m = src.match(re); if (!m) throw new Error('Không thấy ' + what); return m[1]; };
const title = pick(/<title>([\s\S]*?)<\/title>/, '<title>');
const portalCss = pick(/<style>([\s\S]*?)<\/style>/, '<style>');
let body = pick(/<body>([\s\S]*?)<\/body>/, '<body>');

const safeJs = (js) => js.replace(/<\/script/gi, '<\\/script');

// Thay mỗi <script src="..."> cục bộ bằng nội dung của nó
body = body.replace(/<script src="([^"]+)"><\/script>/g, (_, s) => {
  if (/^https?:/.test(s)) return `<script src="${s}"></script>`;
  const p = s.startsWith('../') ? s.slice(3) : 'portal/' + s;
  return `<script>/* ${p} */\n${safeJs(read(p))}\n</script>`;
});

// Link tới file trong repo: thành chữ thường
const LOCAL = "a.href = '../' + p; a.target = '_blank'; a.rel = 'noopener';  // labs/, research/... mở file gốc";
if (!body.includes(LOCAL)) throw new Error('Không thấy dòng xử lý link cục bộ trong portal/index.html');
body = body.replace(LOCAL,
  "var sp = document.createElement('span'); sp.className = 'repo-file'; sp.title = 'Tệp trong repo học: ' + p; sp.textContent = a.textContent; a.replaceWith(sp);  // bản artifact: không có file repo");

// Script CDN trong <head> của portal (marked) phải nạp trước script nội tuyến
const headScripts = [...src.slice(0, src.indexOf('<body>')).matchAll(/<script src="(https:[^"]+)"><\/script>/g)]
  .map((m) => `<script src="${m[1]}"></script>`).join('\n');

const css = read('assets/style.css') + '\n/* ---- portal ---- */\n' + portalCss +
  '\n  .repo-file { border-bottom: 1px dotted var(--muted); cursor: help; }\n';

const out = `<title>${title}</title>
<style>
${css}
</style>
${headScripts}
${body.trim()}
`;

mkdirSync(join(ROOT, 'dist'), { recursive: true });
writeFileSync(join(ROOT, 'dist/kafka-portal.html'), out);
console.log(`dist/kafka-portal.html: ${(out.length / 1024).toFixed(0)} KB`);
