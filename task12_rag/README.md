# Lab 12 — RAG knowledge assistant and AI-assisted development

A question-answering assistant over the Delta Lake documentation, built on Databricks Vector Search
and Foundation Model APIs and deployed with an Asset Bundle through GitHub Actions. Most of it was
built by an AI coding agent (Claude Code) using the Databricks AI Dev Kit.

Workspace: personal Databricks Free Edition, catalog `workspace`. Vector Search is too expensive to
keep running in the course PROD, so Łukasz agreed that this lab runs on Free Edition while the other
labs stay in PROD.

```
GitHub delta-io/delta @ 7197686 (Apache-2.0)
   │  00_ingest: 46 .md/.mdx pages
   ▼
raw_docs volume ──▶ docs_raw (one row per page)
   │  01_chunk: clean MDX, split by headings, two chunk profiles, metadata
   ▼
docs_chunks (Delta, Change Data Feed, MERGE by content hash)
   │  02_index: Delta Sync index, managed embeddings databricks-gte-large-en
   ▼
docs_chunks_index on endpoint lena066636-rag-docs-<target>
   │  top-k retrieval with metadata filters
   ▼
Assistant: retrieve → prompt with numbered excerpts → databricks-meta-llama-3-3-70b-instruct
   │                                      every call logged
   ▼                                           ▼
03_evaluate / 04_ask                        qa_log, eval_results ──▶ RAG monitoring dashboard
```

## Part A — RAG on Databricks

### Data and preparation

- **Dataset:** the Delta Lake documentation from `delta-io/delta`, pinned to one commit so every run
  sees the same corpus. 44 of the 46 pages contain enough text to chunk.
- **Raw layer:** `00_ingest` downloads the pages into the volume `raw_docs`, one folder per commit, and
  writes `docs_raw` with one row per page (title, description, raw text, source URL, commit).
- **Cleaning:** `01_chunk` removes the front matter, MDX imports and components, images and link targets.
  It keeps code blocks, link text, and the callout titles (`Note:`, `Warning:`).
- **Chunking:** the text is split by headings, then packed into chunks of whole paragraphs. Each chunk
  starts with `page title > section`, so it is understandable on its own.
- **Metadata per chunk:** `chunk_profile`, `doc_path`, `doc_title`, `section`, `chunk_index`,
  `source_url`, `doc_commit`, `content_hash`.

Two chunk profiles are built from the same pages to compare chunk sizes:

| Profile | Max chars | Overlap | Chunks | Median chars |
|---|---:|---:|---:|---:|
| `small` | 800 | 120 | 1209 | 654 |
| `large` | 2400 | 300 | 555 | 929 |

The cleaning and chunking code lives in `src/rag_kb/text.py` and is unit tested.

### Embeddings, storage and index

- **Table:** `docs_chunks` is a Unity Catalog Delta table with a primary key on `chunk_id` and Change
  Data Feed enabled.
- **Updates:** it is maintained with `MERGE`, so a chunk is rewritten, and embedded again, only when its
  `content_hash` changes. Pages that disappear are deleted.
- **Index:** `docs_chunks_index` is a Delta Sync index with Databricks-managed embeddings
  (`databricks-gte-large-en`) and a `TRIGGERED` pipeline. A rerun syncs only the changed rows.
- **Endpoint:** the Vector Search endpoint is a bundle resource. The index is created by `02_index`,
  because it needs the source table to exist first.

### Retrieval and QA

`src/rag_kb/service.py` contains the `Assistant` class:

- **Retrieval:** `query_index` with `query_text`, `num_results` (top-k) and a metadata filter.
  `chunk_profile` is always set in the filter, and `doc_path` is optional, for example to search only
  `delta-streaming/index.mdx`.
- **Generation:** the top chunks are passed as numbered excerpts. The model must cite them as `[n]` and
  reply exactly `I don't know based on the documentation.` when the excerpts lack the answer.
- **Logging:** each answer is logged with the retrieved chunk ids and scores, the cited pages, tokens
  and latency.

`04_ask` is the interactive entry point, with widgets for the question, the chunk profile, a page
filter and top-k. The job runs it with a sample question. The answer was: "You can remove old files
from a Delta table by running the `vacuum` command. The default retention threshold for files is 7
days [1]."

### Evaluation

`03_evaluate` uses 18 hand-written questions with reference answers taken from the pages, in
`src/rag_kb/eval_set.py`:

- 15 questions are answered by a specific page.
- 3 are about things the documentation does not cover.

Each question is answered three ways with the same model: RAG on small chunks, RAG on large chunks, and
no retrieval. A second model (`databricks-llama-4-maverick`) judges every answer against the reference
for `correct`, `relevant` and `grounded`.

Dev run on Free Edition:

| Mode | Expected page at rank 1 | In top 5 | Correct | Grounded | Declined out-of-scope | Avg latency | Avg tokens |
|---|---:|---:|---:|---:|---:|---:|---:|
| RAG, small chunks | 0.80 | 1.00 | **1.00** | 1.00 | 3/3 | 1.5 s | 868 |
| RAG, large chunks | 0.87 | 1.00 | 0.93 | 1.00 | 3/3 | 2.1 s | 1392 |
| No retrieval | – | – | 0.33 | – | 0/3 | 1.1 s | 124 |

Correct, grounded and latency are averaged over the 15 in-scope questions. In both RAG modes the model
also never refused an in-scope question.

**What the comparison shows:**

- **Hallucination without retrieval.** The model answers confidently and wrongly. It claimed 5
  clustering columns (the docs say 4), invented the property `delta.stats.collection.max.columns`, and
  said MERGE started using deletion vectors in 0.6.1 (the docs say 3.1.0). It also answered all three
  out-of-scope questions, for example "Delta Lake 1.0.0 added native support for Avro data files",
  which is not true.
- **Missing context.** With retrieval the model declined every out-of-scope question and never declined
  an in-scope one. The expected page was in the top 5 for every question, so the questions this
  measured were not missing context.
- **Chunk size.** Large chunks put the right page first slightly more often (0.87 vs 0.80). The only
  wrong RAG answer came from the large-chunk run, on q15: it named the Spark session extension but left
  out the catalog setting from the reference. Small chunks were as good or better, used 38% fewer
  tokens and answered about 0.6 s faster.

### Monitoring, logging and access

- **Logs:** every call goes to `qa_log`: question, mode, filters, retrieved ids and scores, cited pages,
  answer, abstention flag, model, tokens, latency, user and time. `eval_results` stores every
  evaluation run.
- **Dashboard:** the bundle dashboard **RAG monitoring** shows requests, latency, token use, the latest
  evaluation, and the recent prompts with their responses.
- **Access:** readers (`account users`) get `SELECT` on `docs_chunks` and on the index only. The raw
  volume, `qa_log` and `eval_results` stay with the owner, because logged prompts can contain user data.
- **Credentials:** none appear in code. Notebooks use the job identity, and CI uses the
  `FREE_DATABRICKS_TOKEN` secret.

### Discussion

**Why RAG beats prompting alone.** The model knows Delta Lake in general, but it does not know the
exact property names, version numbers and limits of this documentation, and it does not say when it
is unsure. Retrieval grounds the answer in a specific version of the docs and produces citations that
can be checked. It also turns "not documented" into an explicit refusal instead of an invention. In
the evaluation, correct answers rose from 0.33 to 1.00. Updating knowledge means re-indexing pages,
not retraining the model.

**Chunk size vs retrieval quality.**
- Small chunks match a precise question more closely, cost fewer tokens per answer, and leave room for
  more excerpts.
- They can, however, cut an answer that spans paragraphs. Large chunks keep that context but dilute
  the embedding and fill the prompt with text the question does not need.
- The title and section header on every chunk keeps small chunks understandable. On this corpus,
  800-character chunks with overlap gave the best accuracy for the cost.
- Keeping both profiles in one index, separated by a metadata filter, made the comparison a single
  parameter.

**Governance and cost.**
- **Governance:** Unity Catalog covers the source pages, chunks, index and logs, with lineage from page
  to answer. Readers can query the index without seeing the raw files or other users' prompts. The
  corpus is pinned to a commit, so an answer can be traced to an exact source. Prompt and response
  logs are personal data and need retention rules.
- **Cost:** pay-per-token model calls are small (about 0.9–1.4k tokens per RAG answer, plus the judge).
  The main cost is the Vector Search endpoint, which runs all the time, plus embedding at sync time.
  Hence this lab uses Free Edition, a triggered (not continuous) sync, embedding only changed chunks,
  and removing the dev endpoint after development.

## Part B — AI-assisted development with the AI Dev Kit

### Setup

- **Skills:** the kit's installer was run in the repo root (project scope, Claude Code, Databricks
  profile `trial3`, later `free`):
  ```bash
  bash install.sh --profile trial3 --tools claude --skills-profile ai-ml-engineer,data-engineer
  ```
  It installed 30 skills into `.claude/skills`, through `databricks aitools` (skills 0.2.10) plus
  MLflow skills. The skills are not committed: they are vendor content that `databricks aitools update`
  manages and that the command above reproduces.
- **MCP server:** following `databricks-mcp-server/README.md`, the server was installed in a separate
  venv and registered for the project with
  `claude mcp add-json databricks -s project ...`. This created `.mcp.json` in the repo root, with
  profile `free`. The server exposes 44 tools. The kit's README says 40+; the "75+" figure in the task
  is older.

### What the agent did

| Step | How |
|---|---|
| Found the right workspace | MCP `manage_serving_endpoint` and `execute_sql`, plus the CLI, showed that trial_3 had run out of credits (Vector Search, model serving and serverless SQL all refused). The AI part moved to Free Edition. |
| Inspected the source and models | listed the Delta docs tree at a pinned commit, chose the embedding and chat endpoints from the workspace's serving endpoints |
| Vector Search design | followed the `databricks-vector-search` skill: Delta Sync index, managed embeddings, triggered sync, dict filters on a standard endpoint |
| Bundle config | wrote `databricks.yml` and `resources/lab12.yml`: schema, volume, Vector Search endpoint, job, dashboard, `dev` and `prod` targets. Checked the schema of `vector_search_endpoints` with `databricks bundle schema` and validated both targets. |
| Code and tests | wrote `src/rag_kb` (chunking, prompts, judge parsing, Assistant) and 14 unit tests |
| Ran and checked the job | started runs with MCP `manage_job_runs`, fixed failures, read the results with MCP `execute_sql` |
| Dashboard | created a draft dashboard with MCP `manage_dashboard`, checked each dataset query with `execute_sql`, then moved it into the bundle (`dashboards/rag_monitoring.lvdash.json`) and deleted the draft |
| CI/CD | added `.github/workflows/task12-rag.yml`, using the same flow as Week 8 |

### Guardrails

- **No direct deploys to PROD.** Agent output goes through a pull request. CI runs the unit tests and
  `bundle validate -t prod`, and only a merge to `main` deploys the `prod` target and runs the job. The
  agent deployed only the `dev` target.
- **Evaluation as a gate.** The job rebuilds the index and runs the evaluation on every deploy, so a
  regression in retrieval or answers shows up in `eval_results` and on the dashboard.
- **Least privilege.** Readers get only the chunks and the index. Logs and raw files stay private.
- **Secrets stay with the human.** The agent's permission policy blocks it from writing secrets.
  Tokens, the Free Edition login and the GitHub secret were done by the user.
- **Deterministic inputs.** A pinned corpus commit and temperature 0 make the evaluation runs
  comparable.

### Where the agent helped and where human review was needed

**Where the agent sped things up:**
- Scaffolding the bundle, notebooks, tests and CI in one pass.
- Finding the right SDK and bundle fields from the skills and the CLI schema instead of searching the
  docs.
- Running SQL, jobs and dashboards through MCP without opening the UI.
- Diagnosing the workspace credit problem in minutes.

**What a human had to catch or decide:**
- **Correctness.** The first job run failed on a `MERGE` the agent wrote (`UPDATE SET *, col = ...` is
  not valid SQL). The agent first called the MCP dashboard tool with a wrong action name. Both were
  caught by running the code, not by reading it. The evaluation references were checked against the
  pages by hand, because a wrong reference would make the judge score the wrong thing. The judge
  itself is strict and sometimes wrong: q15 lost a point for leaving out one detail.
- **Cost.** A Vector Search endpoint costs money while it exists. The agent's first bundle gave `dev`
  and `prod` the same endpoint name. `bundle summary` showed the clash, and the name now carries the
  target. Where to run this lab at all was the mentor's decision.
- **Governance.** Who can read what is a policy decision. The agent proposed the grants, the human
  approved them. Logged prompts are personal data.
- **Access and accounts.** Creating the Free Edition account, logging in and creating tokens were done
  by the user.

## Run

```bash
pip install ".[test]" && pytest
databricks bundle deploy -p free
databricks bundle run rag_build -p free
```

Unit tests and bundle validation run on every pull request. A merge to `main` deploys the `prod`
target to Free Edition and runs `rag-build`.
