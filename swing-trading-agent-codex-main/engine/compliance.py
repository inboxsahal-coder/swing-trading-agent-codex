import datetime
import json
from typing import Dict, List, Tuple


VALID_STATES = {
    "STRONG_BULL",
    "WEAK_BULL",
    "SECTOR_ROTATION",
    "BEAR_CORRECTION",
    "HIGH_VOLATILITY",
    "SIDEWAYS",
    "CRISIS",
}


def _check_candidate(c: Dict) -> Tuple[int, int, List[str]]:
    checks = 0
    passed = 0
    issues: List[str] = []

    checks += 1
    if c.get("ticker"):
        passed += 1
    else:
        issues.append("missing_ticker")

    checks += 1
    if c.get("signal") in {"BUY", "NO_TRADE"}:
        passed += 1
    else:
        issues.append("invalid_signal")

    for layer in ["l1", "l2", "l3", "l4", "l5", "l6"]:
        checks += 1
        v = c.get(layer)
        if isinstance(v, (int, float)) and -2 <= float(v) <= 2:
            passed += 1
        else:
            issues.append(f"invalid_{layer}")

    checks += 1
    if isinstance(c.get("research_score"), (int, float)):
        passed += 1
    else:
        issues.append("missing_research_score")

    checks += 1
    if isinstance(c.get("skip_flags", []), list):
        passed += 1
    else:
        issues.append("invalid_skip_flags")

    if c.get("signal") == "BUY":
        for key in ["entry", "stop", "t1", "t2", "entry_timing", "reasoning", "entry_type"]:
            checks += 1
            if c.get(key) is not None and c.get(key) != "":
                passed += 1
            else:
                issues.append(f"missing_{key}")
        checks += 1
        entry = c.get("entry")
        stop = c.get("stop")
        if isinstance(entry, (int, float)) and isinstance(stop, (int, float)) and entry > stop:
            passed += 1
        else:
            issues.append("invalid_entry_stop")

    return passed, checks, issues


def build_compliance_report(run_id: str, analysis: Dict) -> Dict:
    now = datetime.datetime.now(datetime.UTC).isoformat()
    issues: List[str] = []
    checks = 0
    passed = 0

    checks += 1
    state = analysis.get("market_state")
    if state in VALID_STATES:
        passed += 1
    else:
        issues.append("invalid_market_state")

    candidates = analysis.get("candidates")
    checks += 1
    if isinstance(candidates, list):
        passed += 1
    else:
        candidates = []
        issues.append("candidates_not_list")

    per_ticker = []
    for c in candidates:
        p, ch, t_issues = _check_candidate(c)
        passed += p
        checks += ch
        per_ticker.append(
            {
                "ticker": c.get("ticker"),
                "passed": p,
                "checks": ch,
                "adherence_pct": round((p / ch) * 100, 2) if ch else 0.0,
                "issues": t_issues,
            }
        )

    adherence_pct = round((passed / checks) * 100, 2) if checks else 0.0
    return {
        "run_id": run_id,
        "generated_at": now,
        "adherence_pct": adherence_pct,
        "passed_checks": passed,
        "total_checks": checks,
        "issues": issues,
        "per_ticker": per_ticker,
    }


def write_compliance_report(report: Dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
