from rag_kb.eval_set import OUT_OF_SCOPE, QUESTIONS
from rag_kb.qa import (
    ABSTAIN,
    build_judge_messages,
    build_messages,
    cited_sources,
    hit_rank,
    is_abstention,
    parse_judgement,
)

CHUNKS = [
    {"doc_path": "a.mdx", "doc_title": "A", "section": "", "content": "alpha"},
    {"doc_path": "b.mdx", "doc_title": "B", "section": "Usage", "content": "beta"},
]


def test_rag_prompt_numbers_the_context():
    system, user = build_messages("What is beta?", CHUNKS)
    assert ABSTAIN in system["content"]
    assert "[1] A > Overview\nalpha" in user["content"]
    assert "[2] B > Usage\nbeta" in user["content"]
    assert user["content"].endswith("Question: What is beta?")


def test_plain_prompt_has_no_context():
    messages = build_messages("What is beta?")
    assert len(messages) == 2 and "excerpts" not in messages[1]["content"]


def test_judge_prompt_marks_missing_context():
    assert "Context:\n(none)" in build_judge_messages("q", "r", "a", None)[1]["content"]


def test_abstention_is_detected():
    assert is_abstention(ABSTAIN)
    assert is_abstention("  i don't know based on the documentation, sorry")
    assert not is_abstention("The default is 7 days [1].")


def test_judgement_parsing_is_tolerant():
    assert parse_judgement('Scores: {"correct": 1, "relevant": true, "grounded": 0}') == {
        "correct": 1, "relevant": 1, "grounded": 0,
    }
    assert parse_judgement("no json") == {"correct": 0, "relevant": 0, "grounded": 0}
    assert parse_judgement("{broken") == {"correct": 0, "relevant": 0, "grounded": 0}


def test_citations_map_to_documents():
    assert cited_sources("See [2] and [1], not [7].", CHUNKS) == ["a.mdx", "b.mdx"]


def test_hit_rank():
    assert hit_rank("b.mdx", ["a.mdx", "b.mdx"]) == 2
    assert hit_rank("c.mdx", ["a.mdx"]) is None


def test_eval_set_is_well_formed():
    ids = [q["id"] for q in QUESTIONS]
    assert len(ids) == len(set(ids))
    for q in QUESTIONS:
        assert q["question"].endswith("?")
        assert (q["doc"] is None) == (q["reference"] == OUT_OF_SCOPE)
