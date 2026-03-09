# Final Gap Audit - March 6, 2026

Scope audited against requested remaining gaps:

1. Mailbox provider wiring for `inbound@patrici.us`
2. Full LLM multi-agent extraction runtime
3. File-tree management actions (rename/delete/drag-drop reassignment)
4. Document-grade evidence viewers
5. Account onboarding + magic-link + password flow

## Result

All five gaps are implemented in this repo and covered by tests.

## Gap-by-gap status

### 1) Mailbox provider wiring
Status: **Closed**

Implemented:
- Provider endpoints with verification and normalization:
  - `POST /inbound/v1/providers/postmark`
  - `POST /inbound/v1/providers/mailgun`
  - `POST /inbound/v1/providers/sendgrid`
- Attachment persistence to local ingest storage and conversion to internal ingest contract.
- Shared internal forwarding path to:
  - `/internal/v1/packages:ingest`
  - `/internal/v1/packages/{package_id}:process`

Files:
- `src/agent_app_dataset/inbound_gateway.py`
- `tools/run_inbound_gateway.py`
- `tests/test_inbound_gateway.py`

### 2) LLM multi-agent extraction runtime
Status: **Closed**

Implemented:
- New LLM runtime with explicit agent responsibilities:
  - Agent 2: package/file classification
  - Agent 3: concept extraction with evidence locators
  - Agent 4: independent verification with rejection feedback loops
- `extraction_mode=llm` integrated into processing pipeline.
- Internal API process default now set to `llm`.

Files:
- `src/agent_app_dataset/llm_runtime.py`
- `src/agent_app_dataset/internal_processing.py`
- `src/agent_app_dataset/internal_api.py`
- `tests/test_llm_runtime.py`

### 3) File-tree management actions
Status: **Closed**

Implemented:
- Deal metadata model (`display_name`, archived state).
- API endpoints:
  - `PATCH /internal/v1/deals/{deal_id}` (rename)
  - `DELETE /internal/v1/deals/{deal_id}` (archive/delete)
  - `POST /internal/v1/packages/{package_id}:reassign` (package reassignment)
- UI actions in left panel:
  - rename
  - archive
  - drag-drop package between deals

Files:
- `src/agent_app_dataset/internal_store.py`
- `src/agent_app_dataset/internal_api.py`
- `src/agent_app_dataset/ui/app.js`
- `src/agent_app_dataset/ui/app.css`
- `tests/test_deal_management.py`

### 4) Document-grade evidence viewers
Status: **Closed**

Implemented:
- File download endpoint for raw evidence:
  - `GET /internal/v1/packages/{package_id}/files/{file_id}:download`
- Evidence payload now includes `download_url`.
- PDF viewer upgraded to embedded full document iframe.
- XLSX viewer upgraded to workbook/tabs rendering (SheetJS) with target-cell highlighting.

Files:
- `src/agent_app_dataset/internal_api.py`
- `src/agent_app_dataset/ui/app.js`
- `src/agent_app_dataset/ui/app.css`
- `src/agent_app_dataset/ui/index.html`
- `tests/test_document_viewer_api.py`

### 5) Account onboarding + magic-link + password flow
Status: **Closed**

Implemented:
- User, magic-link, and session persistence.
- Auth endpoints:
  - `POST /auth/v1/onboarding:ensure`
  - `POST /auth/v1/magic-link/request`
  - `POST /auth/v1/magic-link/consume`
  - `POST /auth/v1/login`
  - `GET /auth/v1/me`
  - `POST /auth/v1/logout`
- Inbound gateway now triggers onboarding flow after ingest/process.

Files:
- `src/agent_app_dataset/auth.py`
- `src/agent_app_dataset/internal_store.py`
- `src/agent_app_dataset/internal_api.py`
- `src/agent_app_dataset/inbound_gateway.py`
- `tests/test_auth_flow.py`

## Test evidence

Executed on March 6, 2026:
- `pytest -q`
- Result: **43 passed, 0 failed**
