import datetime
import json
from typing import Dict, List, Tuple


REQUIRED_FUND_KEYS = ["pe_ratio", "debt_equity", "revenue_q1", "revenue_q2", "revenue_q3", "revenue_q4"]


def _is_conflicted(candidates: List[Dict], tolerance_pct: float = 20.0) -> Tuple[bool, float]:
    values = [float(c["value"]) for c in candidates if c.get("value") is not None]
    if len(values) < 2:
        return False, 0.0
    lo = min(values)
    hi = max(values)
    if lo == 0:
        return False, 0.0
    spread_pct = ((hi - lo) / abs(lo)) * 100
    return spread_pct > tolerance_pct, round(spread_pct, 2)


def build_data_quality_report(
    run_id: str,
    candidates: List[Dict],
    fundamentals: Dict[str, Dict],
    dynamic_sectors: Dict[str, Dict],
    blockers: List[Dict],
    max_fund_age_days: int = 90,
) -> Dict:
    blocker_map = {b.get("ticker"): set(b.get("missing", [])) for b in blockers or []}
    generated_at = datetime.datetime.now(datetime.UTC).isoformat()

    per_ticker = []
    missing_any = 0
    stale_any = 0
    conflict_any = 0
    complete_count = 0

    for c in candidates:
        ticker = c.get("ticker")
        if not ticker:
            continue

        fund = fundamentals.get(ticker, {})
        sector_payload = (dynamic_sectors or {}).get(ticker, {})
        sector = sector_payload.get("sector") if isinstance(sector_payload, dict) else sector_payload
        data_age_days = fund.get("data_age_days", 999)
        stale = isinstance(data_age_days, (int, float)) and data_age_days > max_fund_age_days and data_age_days < 999

        pe_conflicted, pe_spread = _is_conflicted(fund.get("pe_candidates", []))
        de_conflicted, de_spread = _is_conflicted(fund.get("de_candidates", []))
        conflict = pe_conflicted or de_conflicted

        missing_fields = sorted(list(blocker_map.get(ticker, set())))
        status = "COMPLETE"
        if missing_fields:
            status = "MISSING"
            missing_any += 1
        elif conflict:
            status = "CONFLICTED"
            conflict_any += 1
        elif stale:
            status = "STALE"
            stale_any += 1
        else:
            complete_count += 1

        source_info = {
            "fundamental_sources": fund.get("sources", []),
            "sector_source": sector_payload.get("source") if isinstance(sector_payload, dict) else None,
        }

        per_ticker.append(
            {
                "ticker": ticker,
                "status": status,
                "missing_fields": missing_fields,
                "stale_fundamentals": stale,
                "fundamental_data_age_days": data_age_days,
                "sector": sector,
                "source_info": source_info,
                "conflicts": {
                    "pe_conflicted": pe_conflicted,
                    "pe_spread_pct": pe_spread,
                    "de_conflicted": de_conflicted,
                    "de_spread_pct": de_spread,
                },
                "required_fields_present": all(fund.get(k) is not None for k in REQUIRED_FUND_KEYS) and bool(sector) and c.get("delivery_pct") is not None,
            }
        )

    total = len(per_ticker)
    overall_status = "COMPLETE" if total > 0 and complete_count == total else "PARTIAL"

    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "overall_status": overall_status,
        "summary": {
            "total_candidates": total,
            "complete": complete_count,
            "missing": missing_any,
            "stale": stale_any,
            "conflicted": conflict_any,
            "blocked_candidates": len(blockers or []),
        },
        "tickers": per_ticker,
    }


def write_data_quality_report(report: Dict, path: str) -> None:
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
