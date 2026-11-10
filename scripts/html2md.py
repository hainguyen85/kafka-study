#!/usr/bin/env python3
"""Chuyển lessons/*.html và reference/*.html thành .md song song (cùng tên, cùng thư mục)
để đọc được trên GitHub mà không cần Pages. Đồng thời sinh INDEX.md.

Chạy:  python scripts/html2md.py        (từ thư mục gốc workspace)

Quy ước cho HTML để chuyển sạch:
  - Chỉ dùng: h1-h3, p, ul/ol/li, table, blockquote(+cite), pre/code, strong/em/a, figure/figcaption.
  - div.mission / .win / .ask / .source thành blockquote.
  - <svg> bị bỏ. Muốn có hình trong .md, đặt ngay trong <figure>:
        <script type="text/mermaid">
        flowchart LR
          A --> B
        </script>
  - Quiz (Quiz.mount trong <script>) được chuyển thành danh sách câu hỏi + đáp án gập trong <details>.
Không sửa .md bằng tay: lần chạy sau sẽ ghi đè.
"""
import json
import re
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUOTE_CLASSES = {"mission", "win", "ask", "source"}
SKIP_TAGS = {"head", "script", "style", "svg"}


def quiz_to_md(html):
    m = re.search(r"Quiz\.mount\([^,]+,\s*(\[.*?\])\s*\);", html, re.S)
    if not m:
        return ""
    # Mảng JS do chính ta viết; để node đọc hộ thay vì tự parse.
    out = subprocess.run(
        ["node", "-e", "console.log(JSON.stringify(eval(process.argv[1])))", m.group(1)],
        capture_output=True, text=True, encoding="utf-8", check=True,
    ).stdout
    items = json.loads(out)
    blocks = []
    for i, it in enumerate(items, 1):
        opts = "\n".join(f"- **{chr(65 + j)}.** {o}" for j, o in enumerate(it["options"]))
        ans = f"{chr(65 + it['answer'])}. {it['options'][it['answer']]}"
        blocks.append(
            f"**{i}. {it['q']}**\n\n{opts}\n\n"
            f"<details><summary>Đáp án</summary>\n\n**{ans}**\n\n{it['why']}\n\n</details>"
        )
    return "\n\n".join(blocks)


class Converter(HTMLParser):
    def __init__(self, quiz_md):
        super().__init__(convert_charrefs=True)
        self.quiz_md = quiz_md
        self.ctx = [{"blocks": [], "inline": "", "kind": "root"}]
        self.skip = 0
        self.div_kinds = []
        self.in_pre = False
        self.pre_buf = ""
        self.heading = 0
        self.links = []
        self.lists = []  # mỗi phần tử: {"ordered": bool, "items": []}
        self.table = None
        self.cell = None
        self.in_figcaption = False
        self.capture = None  # bộ đệm cho <script type="text/mermaid">

    # --- tiện ích ---
    @property
    def cur(self):
        return self.ctx[-1]

    def add(self, text):
        if self.cell is not None:
            self.cell["text"] += text
        else:
            self.cur["inline"] += text

    def flush(self):
        if self.cell is not None:
            return
        t = self.cur["inline"].strip()
        if t:
            if self.heading:
                t = "#" * self.heading + " " + t
                self.heading = 0
            self.cur["blocks"].append(t)
        self.cur["inline"] = ""

    def push_quote(self):
        self.flush()
        self.ctx.append({"blocks": [], "inline": "", "kind": "quote"})

    def pop_quote(self):
        self.flush()
        c = self.ctx.pop()
        if c["blocks"]:
            lines = "\n>\n".join("\n".join("> " + l for l in b.split("\n")) for b in c["blocks"])
            self.cur["blocks"].append(lines)

    @staticmethod
    def fix_href(h):
        if h and not re.match(r"^[a-z]+:", h) and ".html" in h:
            return h.replace(".html", ".md")
        return h

    # --- sự kiện ---
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag in SKIP_TAGS:
            self.skip += 1
            if tag == "script" and a.get("type") == "text/mermaid":
                self.capture = ""
            return
        if self.skip:
            return
        cls = set((a.get("class") or "").split())
        if tag in ("h1", "h2", "h3"):
            self.flush()
            self.heading = int(tag[1])
        elif tag in ("p", "nav", "figure"):
            self.flush()
        elif tag == "div":
            if cls & QUOTE_CLASSES:
                self.push_quote()
                self.div_kinds.append("quote")
            elif a.get("id") == "quiz":
                self.flush()
                self.cur["blocks"].append(self.quiz_md)
                self.div_kinds.append("plain")
            else:
                self.flush()
                self.div_kinds.append("plain")
        elif tag == "blockquote":
            self.push_quote()
        elif tag == "cite":
            self.flush()
        elif tag in ("ul", "ol"):
            self.flush()
            self.lists.append({"ordered": tag == "ol", "items": []})
        elif tag == "li":
            self.flush()
        elif tag == "table":
            self.flush()
            self.table = {"rows": [], "row": None, "pending": {}, "has_th": False}
        elif tag == "tr":
            self.table["row"] = []
        elif tag in ("td", "th"):
            self.cell = {"text": "", "span": int(a.get("rowspan") or 1), "th": tag == "th"}
        elif tag == "pre":
            self.flush()
            self.in_pre = True
            self.pre_buf = ""
        elif tag in ("strong", "b"):
            self.add("**")
        elif tag in ("em", "i"):
            self.add("*")
        elif tag == "code":
            if not self.in_pre:
                self.add("`")
        elif tag == "a":
            self.links.append(self.fix_href(a.get("href", "")))
            self.add("[")
        elif tag == "br":
            if self.cell is not None:
                self.add("<br>")
            else:
                self.flush()
        elif tag == "figcaption":
            self.flush()
            self.in_figcaption = True
            self.add("*")

    def handle_endtag(self, tag):
        if tag in SKIP_TAGS:
            self.skip -= 1
            if tag == "script" and self.capture is not None:
                self.flush()
                self.cur["blocks"].append("```mermaid\n" + self.capture.strip() + "\n```")
                self.capture = None
            return
        if self.skip:
            return
        if tag in ("h1", "h2", "h3", "p", "nav", "figure", "cite"):
            self.flush()
        elif tag == "div":
            kind = self.div_kinds.pop() if self.div_kinds else "plain"
            if kind == "quote":
                self.pop_quote()
            else:
                self.flush()
        elif tag == "blockquote":
            self.pop_quote()
        elif tag == "li":
            text = self.cur["inline"].strip()
            self.cur["inline"] = ""
            lst = self.lists[-1]
            prefix = f"{len(lst['items']) + 1}. " if lst["ordered"] else "- "
            lst["items"].append(prefix + text)
        elif tag in ("ul", "ol"):
            lst = self.lists.pop()
            self.cur["blocks"].append("\n".join(lst["items"]))
        elif tag in ("td", "th"):
            t = re.sub(r"\s+", " ", self.cell["text"]).strip().replace("|", "\\|")
            self.table["row"].append((t, self.cell["span"]))
            self.table["has_th"] |= self.cell["th"]
            self.cell = None
        elif tag == "tr":
            self.finish_row()
        elif tag == "table":
            self.finish_table()
        elif tag == "pre":
            self.cur["blocks"].append("```\n" + self.pre_buf.strip("\n") + "\n```")
            self.in_pre = False
        elif tag in ("strong", "b"):
            self.add("**")
        elif tag in ("em", "i"):
            self.add("*")
        elif tag == "code":
            if not self.in_pre:
                self.add("`")
        elif tag == "a":
            self.add(f"]({self.links.pop()})")
        elif tag == "figcaption":
            self.add("*")
            self.in_figcaption = False
            self.flush()

    def handle_data(self, data):
        if self.capture is not None:
            self.capture += data
            return
        if self.skip:
            return
        if self.in_pre:
            self.pre_buf += data
            return
        self.add(re.sub(r"\s+", " ", data))

    # --- bảng (hỗ trợ rowspan bằng ô trống ở các hàng sau) ---
    def finish_row(self):
        t = self.table
        row, final, col = t["row"], [], 0
        cells = iter(row)

        def drain():
            nonlocal col
            while col in t["pending"] and t["pending"][col] > 0:
                final.append("")
                t["pending"][col] -= 1
                col += 1

        for text, span in cells:
            drain()
            final.append(text)
            if span > 1:
                t["pending"][col] = span - 1
            col += 1
        drain()
        t["rows"].append(final)
        t["row"] = None

    def finish_table(self):
        rows = self.table["rows"]
        width = max(len(r) for r in rows)
        rows = [r + [""] * (width - len(r)) for r in rows]
        head, body = rows[0], rows[1:]
        lines = ["| " + " | ".join(head) + " |", "|" + "---|" * width]
        lines += ["| " + " | ".join(r) + " |" for r in body]
        self.cur["blocks"].append("\n".join(lines))
        self.table = None

    def result(self):
        self.flush()
        return "\n\n".join(self.ctx[0]["blocks"])


def title_of(html):
    m = re.search(r"<title>(.*?)</title>", html, re.S)
    return m.group(1).strip() if m else ""


def convert(path):
    html = path.read_text(encoding="utf-8")
    conv = Converter(quiz_to_md(html))
    conv.feed(html)
    body = conv.result()
    note = f"<!-- Tự sinh từ {path.name} bằng scripts/html2md.py. Đừng sửa tay. -->"
    path.with_suffix(".md").write_text(f"{note}\n\n{body}\n", encoding="utf-8", newline="\n")
    return title_of(html)


def main():
    index = ["# Mục lục", "", "Bản `.md` được sinh tự động từ `.html` (xem `scripts/html2md.py`). Mở `.html` trong trình duyệt để có sơ đồ và quiz tương tác.", ""]
    for folder, heading in (("lessons", "Bài học"), ("reference", "Bảng tra")):
        index += [f"## {heading}", ""]
        for p in sorted((ROOT / folder).glob("*.html")):
            title = convert(p)
            index.append(f"- [{title}]({folder}/{p.stem}.md) · [html]({folder}/{p.name})")
            print("ok", p.relative_to(ROOT))
        index.append("")
    index += ["## Khác", "", "- [Mission](MISSION.md)", "- [Tài nguyên](RESOURCES.md)", "- [Ghi chú và lộ trình](NOTES.md)", ""]
    (ROOT / "INDEX.md").write_text("\n".join(index), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    sys.exit(main())
