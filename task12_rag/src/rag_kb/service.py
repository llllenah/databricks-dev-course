import json
import time
import uuid

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from rag_kb.qa import build_messages, cited_sources, is_abstention

RETRIEVED_COLUMNS = ["chunk_id", "doc_path", "doc_title", "section", "chunk_profile", "content"]

LOG_COLUMNS = (
    "request_id string, source string, question string, mode string, chunk_profile string, top_k int, "
    "filters string, retrieved_ids array<string>, retrieved_docs array<string>, scores array<double>, "
    "cited_docs array<string>, answer string, abstained boolean, model string, prompt_tokens int, "
    "completion_tokens int, latency_ms int"
)
LOG_FIELDS = [column.split(" ")[0] for column in LOG_COLUMNS.split(", ")]


class Assistant:
    def __init__(self, index_name, llm_endpoint, client=None):
        self.index_name = index_name
        self.llm_endpoint = llm_endpoint
        self.client = client or WorkspaceClient()

    def retrieve(self, question, top_k=5, profile="small", doc_path=None):
        filters = {"chunk_profile": profile}
        if doc_path:
            filters["doc_path"] = doc_path
        response = self.client.vector_search_indexes.query_index(
            index_name=self.index_name,
            columns=RETRIEVED_COLUMNS,
            query_text=question,
            num_results=top_k,
            filters_json=json.dumps(filters),
        )
        names = [c.name for c in response.manifest.columns]
        return [dict(zip(names, row)) for row in (response.result.data_array or [])], filters

    def chat(self, messages, endpoint=None, max_tokens=400):
        response = self.client.serving_endpoints.query(
            name=endpoint or self.llm_endpoint,
            messages=[ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"]) for m in messages],
            max_tokens=max_tokens,
            temperature=0.0,
        )
        usage = response.usage
        return response.choices[0].message.content.strip(), usage

    def ask(self, question, use_retrieval=True, top_k=5, profile="small", doc_path=None, source="assistant"):
        started = time.perf_counter()
        chunks, filters = self.retrieve(question, top_k, profile, doc_path) if use_retrieval else (None, None)
        answer, usage = self.chat(build_messages(question, chunks))
        return {
            "request_id": str(uuid.uuid4()),
            "source": source,
            "question": question,
            "mode": "rag" if use_retrieval else "no_rag",
            "chunk_profile": profile if use_retrieval else None,
            "top_k": top_k if use_retrieval else None,
            "filters": json.dumps(filters) if filters else None,
            "retrieved_ids": [c["chunk_id"] for c in chunks or []],
            "retrieved_docs": [c["doc_path"] for c in chunks or []],
            "scores": [float(c["score"]) for c in chunks or []],
            "cited_docs": cited_sources(answer, chunks or []),
            "answer": answer,
            "abstained": is_abstention(answer),
            "model": self.llm_endpoint,
            "prompt_tokens": getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "completion_tokens", None),
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "chunks": chunks,
        }


def log_interactions(spark, table, results):
    rows = [tuple(result.get(field) for field in LOG_FIELDS) for result in results]
    (
        spark.createDataFrame(rows, LOG_COLUMNS)
        .selectExpr("request_id", "current_timestamp() AS logged_at", "current_user() AS user_name", *LOG_FIELDS[1:])
        .write.mode("append").saveAsTable(table)
    )
