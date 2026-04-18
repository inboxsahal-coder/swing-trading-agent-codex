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
   - `python main.py run --paper`
   - complete analysis in Codex using `analysis_input.json`
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

If any candidate is missing required inputs (`sector`, `delivery_pct`, `pe_ratio`, `debt_equity`, `revenue_q1..q4`), the run aborts with a blocker report and does not proceed to paper testing.
