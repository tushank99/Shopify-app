# Kanban board (GitHub Projects)

Columns: **Backlog -> Ready -> In progress -> In review -> Done**. One issue per story, one PR per issue, with `Closes #<n>` in the PR description so the board moves the card automatically. WIP limit: 2 in "In progress".

Suggested issues (labels: `erp`, `story` / `tech-debt` / `docs`):

| # | Title | Status |
|---|-------|--------|
| 1 | US-1 Sell without overselling (reservation + optimistic locking) | Done |
| 2 | US-2 Saga audit trail and compensation | Done |
| 3 | US-3 Low-stock report and purchase orders | Done |
| 4 | US-4 Publish shipped orders to the ledger (outbox) | Done |
| 5 | Multi-module Maven build | Done |
| 6 | OpenAPI / Swagger + design notes | Done |
| 7 | Automated tests (unit, Testcontainers) per `docs/test-plan.md` | Backlog |
| 8 | Ledger reversal event for cancelled-after-publish orders (contract v2) | Backlog |
| 9 | Saga recovery job for orders stuck mid-flow | Backlog |
| 10 | Authentication on the ERP API | Backlog |
| 11 | Deploy script for a Linux VM / EC2 | Backlog |
