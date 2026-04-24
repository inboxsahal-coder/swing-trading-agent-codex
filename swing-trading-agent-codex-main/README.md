# swing-trading-agent-codex

## Quick start (paper mode)

1. Create and activate a virtual environment:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
2. Install dependencies:
   - `pip install -r requirements.txt`
   - If your environment blocks PyPI, the app can still read `config.json` as a fallback for config loading.
3. Run system checks:
   - `python main.py status`
4. Run paper flow:
   - `python main.py run --paper --provider chatgpt_project`
   - complete analysis in your ChatGPT Project using `CODEX.md` + `analysis_input.json`
   - write output to `analysis_output_<run_id>.json` using the `run_id` shown in terminal (also saved in `run_context.json`)
   - `python main.py finalize --paper`
5. Track readiness for live deployment:
   - `python main.py metrics`

## Paper-test go/no-go rule

By default, paper mode is considered ready for live only after:
- at least 20 paper trades
- paper win rate >= 50%

This threshold is configurable in `config.yaml` via:
- `paper_mode_go_nogo_threshold_pct`

## Data completeness policy (strict gate)

Paper runs now use multi-source enrichment and a hard completeness gate before `analysis_input.json` is generated:

1. **Fundamentals (PE, D/E, quarterly revenue)** are fetched via ordered fallback:
   - yfinance
   - Yahoo Quote Summary endpoint
   - NSE Quote Equity metadata (PE fallback)
2. **Sector classification** is fetched via ordered fallback:
   - NSE Quote Equity (industry metadata)
   - Yahoo Quote Summary (asset profile sector)
   - yfinance sector
3. **Delivery %** is fetched from Bhavcopy.
   - If live/archive fetch fails, the engine reuses the latest non-empty local Bhavcopy cache as a fallback.

Candidates missing required inputs (`sector`, `delivery_pct`, `pe_ratio`, `debt_equity`, `revenue_q1..q4`) are skipped, logged to `data_blockers.json`, and excluded from `analysis_input.json`. The run only aborts when **all** candidates are blocked.

Completeness strictness is configurable:
- `require_delivery_pct` (default: `false`)
- `require_sector_classification` (default: `false`)

If strict mode blocks all symbols, the engine automatically retries in graceful mode before aborting.

For paper runs, an additional safety fallback is enabled by default:
- `allow_best_effort_when_all_blocked` (default: `true`)

If strict + graceful both block all candidates, paper mode can still proceed with best-effort analysis on the original candidate set.

## Analysis provider modes

`main.py run` supports:
- `--provider chatgpt_project` (default): produces a run-scoped handoff (`run_context.json`, `analysis_request_<run_id>.md`) and expects analysis output in `analysis_output_<run_id>.json`.
- `--provider local_file`: keeps legacy behavior and expects `analysis_output.json`.

`main.py finalize` validates output schema before ranking and can automatically pick run-scoped files from `run_context.json`.

## Timing modes (Phase 4)

`main.py run` also supports `--timing-mode`:
- `eod_strict`: run only after safe EOD window (8 PM IST), blocks market-hours and early post-close runs.
- `post_close_fast` (default): allows post-close runs before 8 PM and flags potential partial data.
- `manual_force`: always proceed using latest available data.

## Data quality artifacts (Phase 2)

Each run now generates:
- `data_quality_report_<run_id>.json` (run-scoped)
- `data_quality_report.json` (latest pointer)

Reports include per-ticker status (`COMPLETE`, `MISSING`, `STALE`, `CONFLICTED`), missing fields, source metadata, and valuation conflict checks.

## Framework trust and governance artifacts (Phases 3, 5, 7)

Finalize now produces:
- `compliance_report_<run_id>.json` + `compliance_report_latest.json`
- `recency_bias_report_<run_id>.json` + `recency_bias_report_latest.json`

And persists run lifecycle metadata in SQLite `run_registry` (provider, timing mode, artifact paths, schema/compliance status, recency-bias averages).

## Elliott wave policy (Phase 8)

Elliott wave logic is intentionally **not** part of core signal generation in this engine. Core execution remains framework-driven (L1–L6 rubric, skip flags, and risk controls). Elliott-style methods remain optional experimental overlays only.

## Universe mode

Set `universe_mode` in config:
- `full` (default): analyze full fetched universe after basic data sanity checks.
- `prefiltered`: apply strict momentum pre-filters and cap to top 25 by RS (legacy behavior).

## Storyline / catalyst weight in ranking

For swing/momentum setups, ranking now supports catalyst weightage when analysis output includes:
- `storyline_score` (0..10)
- `storyline_notes` (short catalyst description)

Ranking uses:
- `composite_rank_score = research_score + storyline_weight * storyline_score`

Config:
- `storyline_weight` (default: `0.35`)

## Merge-conflict safe workflow

To avoid repeated PR conflicts on hot files (`main.py`, `data/fetch.py`, `data/prefilter.py`):
1. Branch from latest `main`.
2. Keep PR scope narrow (one concern per PR).
3. Rebase your branch on latest `main` before opening/reviewing PR.
4. Prefer landing conflict-prone runtime changes via a single integration PR.
