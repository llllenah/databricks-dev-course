import json
import re

ABSTAIN = "I don't know based on the documentation."

RAG_SYSTEM = (
    "You are a Delta Lake documentation assistant. Answer only from the numbered excerpts. "
    "Cite the excerpts you used as [1], [2]. Keep the answer under 120 words. "
    f"If the excerpts do not contain the answer, reply exactly: {ABSTAIN}"
)
PLAIN_SYSTEM = "You are a Delta Lake expert. Answer the question in under 120 words."

JUDGE_SYSTEM = (
    "You grade answers about Delta Lake. Return only a JSON object with integer scores 0 or 1: "
    '{"correct": ..., "relevant": ..., "grounded": ...}. '
    "correct: the answer agrees with the reference and contains no contradicting claim. "
    "If the reference says the documentation does not cover the topic, correct is 1 only when the answer declines to answer. "
    "relevant: the answer addresses the question. "
    "grounded: every claim in the answer is supported by the context; use 0 when no context is given."
)


def format_context(chunks):
    return "\n\n".join(
        f"[{i}] {chunk['doc_title']} > {chunk['section'] or 'Overview'}\n{chunk['content']}"
        for i, chunk in enumerate(chunks, start=1)
    )


def build_messages(question, chunks=None):
    if chunks is None:
        return [{"role": "system", "content": PLAIN_SYSTEM}, {"role": "user", "content": question}]
    user = f"Documentation excerpts:\n\n{format_context(chunks)}\n\nQuestion: {question}"
    return [{"role": "system", "content": RAG_SYSTEM}, {"role": "user", "content": user}]


def build_judge_messages(question, reference, answer, context):
    user = (
        f"Question: {question}\n\nReference: {reference}\n\n"
        f"Context:\n{context or '(none)'}\n\nAnswer: {answer}"
    )
    return [{"role": "system", "content": JUDGE_SYSTEM}, {"role": "user", "content": user}]


def is_abstention(answer):
    return answer.strip().lower().startswith(ABSTAIN.lower()[:24])


def parse_judgement(text):
    match = re.search(r"\{.*?\}", text, re.S)
    scores = {"correct": 0, "relevant": 0, "grounded": 0}
    if not match:
        return scores
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return scores
    for key in scores:
        value = data.get(key, 0)
        scores[key] = 1 if value in (1, True, "1", "yes", "true") else 0
    return scores


def cited_sources(answer, chunks):
    numbers = {int(n) for n in re.findall(r"\[(\d+)\]", answer)}
    return [chunks[n - 1]["doc_path"] for n in sorted(numbers) if 0 < n <= len(chunks)]


def hit_rank(expected_doc, retrieved_docs):
    for rank, doc in enumerate(retrieved_docs, start=1):
        if doc == expected_doc:
            return rank
    return None
