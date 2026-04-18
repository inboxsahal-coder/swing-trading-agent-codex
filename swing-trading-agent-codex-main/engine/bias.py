import datetime
import json
from typing import Dict, List


def _score_candidate_bias(c: Dict) -> Dict:
    l1 = abs(float(c.get("l1", 0) or 0))
    l2 = abs(float(c.get("l2", 0) or 0))
    l3 = abs(float(c.get("l3", 0) or 0))
    l4 = abs(float(c.get("l4", 0) or 0))
    l5 = abs(float(c.get("l5", 0) or 0))
    l6 = abs(float(c.get("l6", 0) or 0))

    recency = l1 + l2 + l6
    structural = l3 + l4 + l5
    total = recency + structural
    recency_ratio = (recency / total) if total > 0 else 0.0
    structural_confirmed = structural >= 2.0

    if recency_ratio >= 0.70 and not structural_confirmed:
        label = "HIGH"
    elif recency_ratio >= 0.55:
        label = "MEDIUM"
    else:
        label = "LOW"

    return {
        "ticker": c.get("ticker"),
        "recency_ratio": round(recency_ratio, 4),
        "recency_bias_label": label,
        "structural_score_abs": round(structural, 2),
        "structural_confirmed": structural_confirmed,
    }


def build_recency_bias_report(run_id: str, analysis: Dict) -> Dict:
    candidates: List[Dict] = analysis.get("candidates", []) if isinstance(analysis, dict) else []
    rows = [_score_candidate_bias(c) for c in candidates if isinstance(c, dict)]
    avg = round(sum(r["recency_ratio"] for r in rows) / len(rows), 4) if rows else 0.0
    high = sum(1 for r in rows if r["recency_bias_label"] == "HIGH")
    medium = sum(1 for r in rows if r["recency_bias_label"] == "MEDIUM")
    low = sum(1 for r in rows if r["recency_bias_label"] == "LOW")
    return {
        "run_id": run_id,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "summary": {
            "candidate_count": len(rows),
            "avg_recency_ratio": avg,
            "high": high,
            "medium": medium,
            "low": low,
        },
        "tickers": rows,
    }


def write_recency_bias_report(report: Dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(report, f, indent=2)


def enrich_analysis_with_bias(analysis: Dict, report: Dict) -> Dict:
    lookup = {r["ticker"]: r for r in report.get("tickers", [])}
    for c in analysis.get("candidates", []):
        row = lookup.get(c.get("ticker"))
        if row:
            c["recency_bias"] = {
                "ratio": row["recency_ratio"],
                "label": row["recency_bias_label"],
                "structural_confirmed": row["structural_confirmed"],
            }
            if row["recency_bias_label"] == "HIGH" and not row["structural_confirmed"]:
                flags = c.get("skip_flags", [])
                if isinstance(flags, list) and "RECENCY_BIAS_HIGH" not in flags:
                    flags.append("RECENCY_BIAS_HIGH")
                    c["skip_flags"] = flags
    analysis["recency_bias_summary"] = report.get("summary", {})
    return analysis
