from rag_kb.text import PROFILES, chunk_document, chunk_text, clean_mdx, parse_frontmatter, split_sections

DOC = """---
title: What are deletion vectors?
description: Learn about deletion vectors.
---

import { Tabs, TabItem, Aside } from "@astrojs/starlight/components";

<Aside type="caution" title="Warning">
Enabling upgrades the protocol. See [versioning](/versioning/).
</Aside>

Intro paragraph about deletion vectors. ![diagram](img.png)

## Enable deletion vectors

<Tabs>
  <TabItem label="SQL">
    ```sql
    -- not a heading
    # also not a heading
    ALTER TABLE t SET TBLPROPERTIES('delta.enableDeletionVectors' = true);
    ```
  </TabItem>
</Tabs>

### Limitations

Some text here.
"""


def test_frontmatter_is_parsed_and_removed():
    meta, body = parse_frontmatter(DOC)
    assert meta["title"] == "What are deletion vectors?"
    assert not body.startswith("---")


def test_clean_removes_mdx_but_keeps_code_and_link_text():
    text = clean_mdx(parse_frontmatter(DOC)[1])
    assert "import" not in text and "<Tabs>" not in text and "<TabItem" not in text
    assert "Warning:" in text
    assert "See versioning." in text
    assert "img.png" not in text
    assert "```sql\n-- not a heading" in text


def test_sections_follow_headings_outside_code():
    sections = split_sections(clean_mdx(parse_frontmatter(DOC)[1]))
    names = [name for name, _ in sections]
    assert names == ["", "Enable deletion vectors", "Enable deletion vectors > Limitations"]
    assert "# also not a heading" in sections[1][1]


def test_chunks_respect_max_size_and_overlap():
    text = "\n\n".join(f"Paragraph {i} " + "word " * 40 for i in range(20))
    chunks = chunk_text(text, 500, 100)
    assert len(chunks) > 1
    assert all(len(c) <= 500 for c in chunks)
    assert chunks[1].split("\n\n")[0] in chunks[0]


def test_long_block_is_split():
    chunks = chunk_text("x " * 2000, 300, 0)
    assert all(len(c) <= 300 for c in chunks)
    assert sum(len(c.replace(" ", "")) for c in chunks) == 2000


def test_chunk_document_rows_are_stable_and_bounded():
    for profile, (max_chars, _) in PROFILES.items():
        rows = chunk_document("delta-deletion-vectors.mdx", DOC, profile)
        assert rows == chunk_document("delta-deletion-vectors.mdx", DOC, profile)
        assert len({r["chunk_id"] for r in rows}) == len(rows)
        assert all(r["char_count"] <= max_chars for r in rows)
        assert all(r["content"].startswith("What are deletion vectors?") for r in rows)
        assert {r["chunk_profile"] for r in rows} == {profile}
