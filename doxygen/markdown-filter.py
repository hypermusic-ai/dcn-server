#!/usr/bin/env python3
"""Doxygen input filter that turns ```mermaid fences in markdown files into
<pre class="mermaid"> blocks rendered client-side by mermaid.js.

Used via FILTER_PATTERNS in the Doxyfile. Keeps the line count of the input
unchanged: the opening fence becomes the \\htmlonly + <pre> line (plus, for the
first diagram of a file, a one-line <script type="module"> loader — module
scripts are deferred, so it runs after all diagrams are in the DOM), and the
closing fence becomes the </pre>\\endhtmlonly line.

Sub-directory README.md files (any README.md except the repository root one)
additionally get their leading "# Title" heading rewritten into a
"\\page <slug> Title" command, so each module README becomes a standalone
Doxygen page at a deterministic URL derived from its path
(src/events/README.md -> readme_src_events.html) instead of being absorbed
into the directory reference by IMPLICIT_DIR_DOCS. The files on disk stay
plain markdown; the rewrite only exists in the filtered stream.
"""
import html
import os
import re
import sys


def page_slug(md_path: str) -> str | None:
    """Deterministic page id for a sub-README, None for other files."""
    rel = os.path.relpath(md_path).replace("\\", "/")
    if os.path.basename(rel).lower() != "readme.md" or "/" not in rel:
        return None
    return "readme_" + re.sub(r"[^a-z0-9]+", "_", os.path.dirname(rel).lower())

MERMAID_LOADER = (
    '<script type="module">'
    'import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";'
    'import elkLayouts from "https://cdn.jsdelivr.net/npm/@mermaid-js/layout-elk@0/dist/mermaid-layout-elk.esm.min.mjs";'
    'mermaid.registerLayoutLoaders(elkLayouts);'
    'mermaid.initialize({startOnLoad:false});'
    'await mermaid.run({querySelector:"pre.mermaid"});'
    "</script>"
)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    in_mermaid = False
    loader_emitted = False
    slug = page_slug(sys.argv[1])
    with open(sys.argv[1], encoding="utf-8") as source:
        for raw_line in source:
            line = raw_line.rstrip("\n")
            stripped = line.strip()
            if not in_mermaid and slug and stripped.startswith("# "):
                print(f"\\page {slug} {stripped[2:].strip()}")
                slug = None
            elif not in_mermaid and stripped.startswith("```mermaid"):
                in_mermaid = True
                loader = "" if loader_emitted else MERMAID_LOADER
                loader_emitted = True
                print(f'\\htmlonly[block]{loader}<pre class="mermaid">')
            elif in_mermaid and stripped == "```":
                in_mermaid = False
                print("</pre>\\endhtmlonly")
            elif in_mermaid:
                print(html.escape(line, quote=False))
            else:
                print(line)


if __name__ == "__main__":
    main()
