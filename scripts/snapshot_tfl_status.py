import os
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any

BASE_URL = "https://api.tfl.gov.uk"
MODES = "tube,dlr,overground,elizabeth-line,tram"
TIMEOUT_SECS = 30
BATCH_SIZE = 20  # avoids overly long URLs

def chunk(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def get_lines_by_modes(headers: Dict[str, str], modes: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/Line/Mode/{modes}"
    r = requests.get(url, headers=headers, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    return r.json()

def get_status_for_line_ids(headers: Dict[str, str], line_ids: List[str]) -> List[Dict[str, Any]]:
    ids_csv = ",".join(line_ids)
    url = f"{BASE_URL}/Line/{ids_csv}/Status"
    r = requests.get(url, headers=headers, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    return r.json()

def flatten_statuses(status_payload: List[Dict[str, Any]], snapshot_time: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in status_payload:
        line_id = line.get("id")
        line_name = line.get("name")
        mode_name = line.get("modeName")

        statuses = line.get("lineStatuses") or []
        if not statuses:
            rows.append({
                "snapshot_utc": snapshot_time,
                "line_id": line_id,
                "line_name": line_name,
                "mode_name": mode_name,
                "statusSeverity": None,
                "statusSeverityDescription": None,
                "reason": None,
                "valid_from_utc": None,
                "valid_to_utc": None,
                "isNow": None
            })
            continue

        for st in statuses:
            validity_periods = st.get("validityPeriods") or [None]
            for vp in validity_periods:
                rows.append({
                    "snapshot_utc": snapshot_time,
                    "line_id": line_id,
                    "line_name": line_name,
                    "mode_name": mode_name,
                    "statusSeverity": st.get("statusSeverity"),
                    "statusSeverityDescription": st.get("statusSeverityDescription"),
                    "reason": st.get("reason"),
                    "valid_from_utc": (vp.get("fromDate") if vp else None),
                    "valid_to_utc": (vp.get("toDate") if vp else None),
                    "isNow": (vp.get("isNow") if vp else None),
                })
    return rows

def main() -> None:
    api_key = os.environ.get("TFL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing environment variable TFL_API_KEY")

    headers = {"Ocp-Apim-Subscription-Key": api_key}

    snapshot_dt = datetime.now(timezone.utc)
    snapshot_utc = snapshot_dt.isoformat()

    raw_lines = get_lines_by_modes(headers, MODES)
    line_ids = [l["id"] for l in raw_lines if "id" in l]

    status_payloads: List[Dict[str, Any]] = []
    for batch_ids in chunk(line_ids, BATCH_SIZE):
        status_payloads.extend(get_status_for_line_ids(headers, batch_ids))

    rows = flatten_statuses(status_payloads, snapshot_utc)

    df = pd.DataFrame(rows)
    df["snapshot_utc"] = pd.to_datetime(df["snapshot_utc"], utc=True)
    df["valid_from_utc"] = pd.to_datetime(df["valid_from_utc"], utc=True, errors="coerce")
    df["valid_to_utc"] = pd.to_datetime(df["valid_to_utc"], utc=True, errors="coerce")

    # Single output folder
out_dir = os.path.join("data", "snapshots")
os.makedirs(out_dir, exist_ok=True)

# Filename format: YYYY-MM-DD-HHMMSS.parquet
file_name = snapshot_dt.strftime("%Y-%m-%d-%H%M%S") + ".parquet"
out_path = os.path.join(out_dir, file_name)

    df.to_parquet(out_path, index=False)
    print(f"Wrote {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()
