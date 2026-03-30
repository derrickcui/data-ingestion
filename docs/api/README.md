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
6. `GET /ai-vocabulary/runs/{run_id}/invalid-breakdown`
7. `GET /ai-vocabulary/runs/{run_id}/terms`
8. `GET /ai-vocabulary/runs/{run_id}/top-candidates`
9. `POST /ai-vocabulary/runs/{run_id}/rerun`
10. `GET /ai-vocabulary/runs/{run_id}/compare?targetRunId=...`
11. `POST /ai-vocabulary/raw-terms/{raw_term_id}/candidate`
12. `POST /ai-vocabulary/raw-terms/{raw_term_id}/ignore`
13. `POST /ai-vocabulary/raw-terms/{raw_term_id}/unignore`
14. `GET /ai-vocabulary/candidates`
15. `GET /ai-vocabulary/candidates/{candidate_id}/evidence`
16. `POST /ai-vocabulary/candidates/{candidate_id}/review`

Notes:

- AI candidate lifecycle entry is `term_candidate`
- `/ai-vocabulary/candidates` reads AI-derived rows from `term_candidate`
- run detail now exposes summary, invalid breakdown, paginated terms, top candidates, compare, rerun, add-to-candidate, ignore, and unignore endpoints
- sample generation, prompt versioning, run execution, raw terms, and review endpoints are all included in FastAPI live OpenAPI at `/openapi.json`
