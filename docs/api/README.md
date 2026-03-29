# API Spec

UI can use either of these:

- static spec: `docs/api/openapi.yaml`
- live spec from FastAPI: `/openapi.json`

Recommended Phase 1 call sequence for AI Vocabulary:

1. `POST /ai-vocabulary/samples/generate`
2. `GET /ai-vocabulary/samples/versions`
3. `POST /ai-vocabulary/runs`
4. `POST /ai-vocabulary/runs/{run_id}/execute` or `POST /ai-vocabulary/runs/{run_id}/execute-async`
5. `GET /ai-vocabulary/runs/{run_id}/summary`
6. `GET /ai-vocabulary/candidates`
7. `GET /ai-vocabulary/candidates/{candidate_id}/evidence`
8. `POST /ai-vocabulary/candidates/{candidate_id}/review`

Notes:

- AI candidate lifecycle entry is `term_candidate`
- `/ai-vocabulary/candidates` reads AI-derived rows from `term_candidate`
- sample generation, prompt versioning, run execution, raw terms, and review endpoints are all included in `openapi.yaml`
