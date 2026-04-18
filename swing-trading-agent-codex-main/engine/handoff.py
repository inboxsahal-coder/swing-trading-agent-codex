import datetime
import hashlib
import json
import os
import uuid
from typing import Dict, List, Optional, Tuple

RUN_CONTEXT_PATH = "run_context.json"


def generate_run_id() -> str:
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{ts}_{uuid.uuid4().hex[:8]}"


def file_sha256(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_run_context(context: Dict) -> None:
    with open(RUN_CONTEXT_PATH, "w") as f:
        json.dump(context, f, indent=2)


def load_run_context() -> Optional[Dict]:
    if not os.path.exists(RUN_CONTEXT_PATH):
        return None
    with open(RUN_CONTEXT_PATH, "r") as f:
        return json.load(f)


def write_analysis_request_markdown(context: Dict) -> str:
    run_id = context.get("run_id", "unknown")
    provider = context.get("provider", "chatgpt_project")
    input_path = context.get("analysis_input_path", "analysis_input.json")
    output_path = context.get("expected_analysis_output_path", "analysis_output.json")
    out_md = f"analysis_request_{run_id}.md"

    lines = [
        f"# Analysis Request — {run_id}",
        "",
        f"Provider mode: **{provider}**",
        "",
        "Use the files below:",
        f"- Input: `{input_path}`",
        f"- Framework: `CODEX.md`",
        "",
        "Produce output JSON and save it exactly at:",
        f"- `{output_path}`",
        "",
        "Required JSON structure:",
        "- top-level keys: `market_state`, `candidates`",
        "- `candidates` must be a list",
        "- each candidate must include: `ticker`, `signal`, `research_score`",
        "",
        "Strongly recommended candidate fields for downstream ranking/reporting:",
        "`score_band`, `l1`, `l2`, `l3`, `l4`, `l5`, `l6`, `entry_type`, `entry`, `stop`, `t1`, `t2`,",
        "`entry_timing`, `reasoning`, `skip_flags`, `sector`, `themes`",
        "",
        "Validation rule:",
        f"- include `run_id` at top-level and set it to `{run_id}`",
    ]

    with open(out_md, "w") as f:
        f.write("\n".join(lines) + "\n")

    return out_md


def validate_analysis_output(path: str, expected_run_id: Optional[str] = None) -> Tuple[bool, List[str], Optional[Dict]]:
    errors: List[str] = []
    if not os.path.exists(path):
        return False, [f"File not found: {path}"], None

    try:
        with open(path, "r") as f:
            payload = json.load(f)
    except Exception as e:
        return False, [f"Invalid JSON in {path}: {e}"], None

    if not isinstance(payload, dict):
        errors.append("Top-level output must be a JSON object")
        return False, errors, payload

    if expected_run_id:
        found = payload.get("run_id")
        if found and found != expected_run_id:
            errors.append(f"run_id mismatch: expected {expected_run_id}, found {found}")

    if "market_state" not in payload:
        errors.append("Missing top-level key: market_state")

    cands = payload.get("candidates")
    if not isinstance(cands, list):
        errors.append("Missing/invalid top-level key: candidates (must be list)")
    else:
        for i, c in enumerate(cands):
            if not isinstance(c, dict):
                errors.append(f"candidates[{i}] must be object")
                continue
            for req in ("ticker", "signal", "research_score"):
                if req not in c:
                    errors.append(f"candidates[{i}] missing required field: {req}")

    return len(errors) == 0, errors, payload
