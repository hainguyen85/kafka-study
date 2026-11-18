#!/usr/bin/env python3
"""Chuyển lessons/*.html và reference/*.html thành .md song song (cùng tên, cùng thư mục)
để đọc được trên GitHub mà không cần Pages. Đồng thời sinh INDEX.md.

Chạy:  python scripts/html2md.py        (từ thư mục gốc workspace)

Quy ước cho HTML để chuyển sạch:
  - Chỉ dùng: h1-h3, p, ul/ol/li, table, blockquote(+cite), pre/code, strong/em/a, figure/figcaption.
  - div.mission / .win / .ask / .source thành blockquote.
  - <svg> được tách thành img/<tên bài>-figN.svg (cùng thư mục) và hiện trong .md như một ảnh.
    Hoặc vẽ bằng Mermaid, đặt ngay trong <figure>:
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


def quizzes_of(html):
    """{id của div: [câu hỏi]} cho mọi Quiz.mount(document.getElementById('id'), [...]) trong trang."""
    found = re.findall(r"Quiz\.mount\(document\.getElementById\('([^']+)'\),\s*(\[.*?\])\s*\);", html, re.S)
    if not found:
        return {}
    # Mảng JS do chính ta viết; để node đọc hộ thay vì tự parse.
    js = "[" + ",".join(arr for _, arr in found) + "]"
    out = subprocess.run(
        ["node", "-e", "console.log(JSON.stringify(eval(process.argv[1])))", js],
        capture_output=True, text=True, encoding="utf-8", check=True,
    ).stdout
    arrays = json.loads(out)
    return {qid: items for (qid, _), items in zip(found, arrays)}


def quiz_to_md(qid, items):
    blocks = []
    for i, it in enumerate(items, 1):
        opts = "\n".join(f"- **{chr(65 + j)}.** {o}" for j, o in enumerate(it["options"]))
        ans = f"{chr(65 + it['answer'])}. {it['options'][it['answer']]}"
        blocks.append(
            f"**{i}. {it['q']}**\n\n{opts}\n\n"
            f"<details><summary>Đáp án</summary>\n\n**{ans}**\n\n{it['why']}\n\n</details>"
        )
    # Đánh dấu để portal thay bằng quiz tương tác (comment không hiện khi đọc .md).
    return f"<!-- quiz:{qid} -->\n\n" + "\n\n".join(blocks) + "\n\n<!-- /quiz -->"


class Converter(HTMLParser):
    def __init__(self, quizzes):
        super().__init__(convert_charrefs=True)
        self.quizzes = quizzes
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
            elif a.get("id") in self.quizzes:
                self.flush()
                self.cur["blocks"].append(quiz_to_md(a["id"], self.quizzes[a["id"]]))
                self.div_kinds.append("plain")
            else:
                self.flush()
                self.div_kinds.append("plain")
        elif tag == "blockquote":
            self.push_quote()
        elif tag == "cite":
            self.flush()
        elif tag in ("ul", "ol"):
            if self.lists and self.lists[-1].get("in_li"):
                # list lồng trong <li>: giữ phần chữ đã có của mục cha, list con sẽ thụt lề dưới nó
                parent = self.lists[-1]
                parent["pending"] = (parent["pending"] + " " + self.cur["inline"]).strip()
                self.cur["inline"] = ""
            else:
                self.flush()
            self.lists.append({"ordered": tag == "ol", "items": []})
        elif tag == "li":
            self.flush()
            self.lists[-1].update(in_li=True, pending="", sub=[])
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
            lst = self.lists[-1]
            text = (lst.get("pending", "") + " " + self.cur["inline"]).strip()
            self.cur["inline"] = ""
            prefix = f"{len(lst['items']) + 1}. " if lst["ordered"] else "- "
            item = prefix + text
            if lst.get("sub"):
                item += "\n" + "\n".join(lst["sub"])
            lst["items"].append(item)
            lst.update(in_li=False, pending="", sub=[])
        elif tag in ("ul", "ol"):
            lst = self.lists.pop()
            if self.lists and self.lists[-1].get("in_li"):
                # list con: thụt lề dưới mục cha
                self.lists[-1]["sub"].extend("   " + line for item in lst["items"] for line in item.split("\n"))
            else:
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


SVG_RE = re.compile(r"<svg\b[^>]*>.*?</svg>", re.S)


def standalone_css():
    """CSS cho file .svg đứng riêng: biến màu (sáng + tối) và các luật svg.diagram, lấy từ assets/style.css."""
    css = (ROOT / "assets" / "style.css").read_text(encoding="utf-8")
    light = re.search(r":root\s*\{(.*?)\}", css, re.S).group(1)
    dark = re.search(r":root\[data-theme=\"dark\"\]\s*\{(.*?)\}", css, re.S).group(1)
    rules = [r for r in re.findall(r"^svg\.diagram[^{]*\{[^}]*\}", css, re.M) if not r.startswith("svg.diagram {")]
    rules = [r.replace("svg.diagram ", "") for r in rules]
    flat = lambda s: " ".join(s.split())
    return ("<style>\nsvg { " + flat(light) + " background: var(--bg); font-family: system-ui, sans-serif; }\n"
            "@media (prefers-color-scheme: dark) { svg { " + flat(dark) + " } }\n" + "\n".join(rules) + "\n</style>")


def extract_svgs(html, path):
    """Mỗi <svg> thành file <thư mục>/img/<tên bài>-figN.svg, trong .md là một ảnh.
    GitHub hiện ảnh; portal (build_portal.mjs) chèn lại SVG vào trang để dùng màu theo theme."""
    img = path.parent / "img"
    for old in img.glob(f"{path.stem}-fig*.svg"):
        old.unlink()
    n = 0

    def sub(m):
        nonlocal n
        n += 1
        raw = m.group(0)
        label = re.search(r'aria-label="([^"]*)"', raw)
        label = label.group(1).replace("[", "(").replace("]", ")") if label else f"Hình {n}"
        open_tag = re.match(r"<svg\b[^>]*>", raw).group(0)
        tag = open_tag
        if "xmlns=" not in tag:
            tag = tag.replace("<svg", '<svg xmlns="http://www.w3.org/2000/svg"', 1)
        vb = re.search(r'viewBox="[\d.]+ [\d.]+ ([\d.]+) ([\d.]+)"', tag)
        if vb and "width=" not in tag:
            tag = tag.replace("<svg", f'<svg width="{vb.group(1)}" height="{vb.group(2)}"', 1)
        body = raw[len(open_tag):]
        name = f"{path.stem}-fig{n}.svg"
        img.mkdir(exist_ok=True)
        (img / name).write_text(f"{tag}\n{standalone_css()}{body}\n", encoding="utf-8", newline="\n")
        return f"<p>![{label}](img/{name})</p>"

    return SVG_RE.sub(sub, html)


def convert(path):
    html = path.read_text(encoding="utf-8")
    html = extract_svgs(html, path)
    conv = Converter(quizzes_of(html))
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
        if folder == "lessons":  # tóm tắt từng bài: viết tay bằng Markdown, chỉ liệt kê
            index += ["## Tóm tắt từng bài (ôn nhanh)", ""]
            for m in sorted((ROOT / "summaries").glob("*.md")):
                first = m.read_text(encoding="utf-8").splitlines()[0].lstrip("# ").strip()
                index.append(f"- [{first}](summaries/{m.name})")
            index.append("")
    index += ["## Khác", "", "- [Learning portal (mở trong trình duyệt)](portal/index.html): dựng lại bằng `node scripts/build_portal.mjs`", "- [Mission](MISSION.md)", "- [Tài nguyên](RESOURCES.md)", "- [Ghi chú và lộ trình](NOTES.md)", ""]
    (ROOT / "INDEX.md").write_text("\n".join(index), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    sys.exit(main())
