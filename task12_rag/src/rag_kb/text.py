import hashlib
import re

PROFILES = {"small": (800, 120), "large": (2400, 300)}
MIN_CHARS = 40
MIN_BODY = 200

_FRONTMATTER = re.compile(r"\A---\n(.*?)\n---\n", re.S)
_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^(#{1,4})\s+(.*?)\s*#*\s*$")
_ASIDE = re.compile(r"^\s*<Aside\b([^>]*)>\s*$")
_TITLE = re.compile(r'title="([^"]*)"')
_TYPE = re.compile(r'type="([^"]*)"')
_OPEN_TAG = re.compile(r"^\s*<[A-Z][A-Za-z]*\b[^>]*$")
_TAG_LINE = re.compile(r"^\s*</?[A-Z][A-Za-z]*\b[^>]*>\s*$")
_INLINE_TAG = re.compile(r"</?[A-Z][A-Za-z]*\b[^>]*?/?>")
_COMMENT = re.compile(r"<!--.*?-->", re.S)
_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_LINK = re.compile(r"\[([^\]]+)\]\([^)]*\)")


def parse_frontmatter(text):
    text = text.replace("\r\n", "\n")
    match = _FRONTMATTER.match(text)
    if not match:
        return {}, text
    meta = {}
    for line in match.group(1).splitlines():
        key, sep, value = line.partition(":")
        if sep:
            meta[key.strip()] = value.strip().strip("'\"")
    return meta, text[match.end():]


def clean_mdx(body):
    body = _COMMENT.sub("", body)
    lines, in_code, fence_indent, pending = [], False, 0, None
    for line in body.splitlines():
        if pending is not None:
            pending += " " + line.strip()
            if ">" not in line:
                continue
            line, pending = pending, None
        elif not in_code and _OPEN_TAG.match(line):
            pending = line.strip()
            continue
        if _FENCE.match(line):
            if not in_code:
                fence_indent = len(line) - len(line.lstrip())
            in_code = not in_code
            lines.append(line[fence_indent:].rstrip())
            continue
        if in_code:
            lines.append(line[fence_indent:] if line[:fence_indent].strip() == "" else line)
            continue
        if re.match(r"^\s*(import|export)\s", line):
            continue
        aside = _ASIDE.match(line)
        if aside:
            title = _TITLE.search(aside.group(1)) or _TYPE.search(aside.group(1))
            lines.append(f"{title.group(1).capitalize() if title else 'Note'}:")
            continue
        if _TAG_LINE.match(line):
            continue
        line = _INLINE_TAG.sub("", line)
        line = _IMAGE.sub("", line)
        line = _LINK.sub(r"\1", line)
        lines.append(line.strip())
    text = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def split_sections(text):
    sections, path, buffer, in_code = [], [], [], False
    for line in text.splitlines():
        if _FENCE.match(line):
            in_code = not in_code
        heading = None if in_code else _HEADING.match(line)
        if heading:
            if "\n".join(buffer).strip():
                sections.append((" > ".join(path), "\n".join(buffer).strip()))
            level = len(heading.group(1))
            path = path[: level - 1] + [heading.group(2).strip("`")]
            buffer = []
        else:
            buffer.append(line)
    if "\n".join(buffer).strip():
        sections.append((" > ".join(path), "\n".join(buffer).strip()))
    return sections


def _blocks(text):
    blocks, current, in_code = [], [], False
    for line in text.splitlines():
        if _FENCE.match(line):
            in_code = not in_code
        if not line.strip() and not in_code:
            if current:
                blocks.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        blocks.append("\n".join(current))
    return blocks


def _hard_split(block, max_chars):
    pieces, start = [], 0
    while start < len(block):
        end = min(start + max_chars, len(block))
        if end < len(block):
            cut = block.rfind("\n", start, end)
            if cut <= start:
                cut = block.rfind(" ", start, end)
            end = cut if cut > start else end
        pieces.append(block[start:end].strip())
        start = end
    return [p for p in pieces if p]


def chunk_text(text, max_chars, overlap):
    units = []
    for block in _blocks(text):
        units.extend([block] if len(block) <= max_chars else _hard_split(block, max_chars))
    chunks, current = [], ""
    for unit in units:
        candidate = f"{current}\n\n{unit}" if current else unit
        if len(candidate) <= max_chars:
            current = candidate
            continue
        chunks.append(current)
        tail = current[-overlap:] if overlap else ""
        tail = tail[tail.find(" ") + 1:] if " " in tail else tail
        current = f"{tail}\n\n{unit}" if tail and len(tail) + len(unit) + 2 <= max_chars else unit
    if current:
        chunks.append(current)
    return chunks


def chunk_document(doc_path, raw_text, profile):
    max_chars, overlap = PROFILES[profile]
    meta, body = parse_frontmatter(raw_text)
    title = meta.get("title") or doc_path
    rows = []
    for section, section_text in split_sections(clean_mdx(body)):
        header = f"{title} > {section}" if section else title
        for piece in chunk_text(section_text, max(max_chars - len(header) - 2, MIN_BODY), overlap):
            if len(piece) < MIN_CHARS:
                continue
            index = len(rows)
            content = f"{header}\n\n{piece}"
            rows.append({
                "chunk_id": f"{profile}:{doc_path}:{index:04d}",
                "chunk_profile": profile,
                "doc_path": doc_path,
                "doc_title": title,
                "section": section,
                "chunk_index": index,
                "content": content,
                "char_count": len(content),
                "content_hash": hashlib.sha256(content.encode()).hexdigest(),
            })
    return rows
