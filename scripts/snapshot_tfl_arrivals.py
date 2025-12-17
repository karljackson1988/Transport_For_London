import os
import time
import random
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.tfl.gov.uk"
MODES = "tube,dlr,overground,elizabeth-line,tram"
TIMEOUT_SECS = 30

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def get_lines_by_modes(session: requests.Session, headers: Dict[str, str], modes: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/Line/Mode/{modes}"
    r = session.get(url, headers=headers, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    return r.json()

def get_arrivals_for_line(session: requests.Session, headers: Dict[str, str], line_id: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/Line/{line_id}/Arrivals"
    r = session.get(url, headers=headers, timeout=TIMEOUT_SECS)
    r.raise_for_status()
    return r.json()

def main() -> None:
    api_key = os.environ.get("TFL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing environment variable TFL_API_KEY")

    headers = {"Ocp-Apim-Subscription-Key": api_key}
    session = make_session()

    snapshot_dt = datetime.now(timezone.utc)
    snapshot_utc = snapshot_dt.isoformat()

    raw_lines = get_lines_by_modes(session, headers, MODES)
    line_dim = [
        {"line_id": l.get("id"), "line_name": l.get("name"), "mode_name": l.get("modeName")}
        for l in raw_lines
        if l.get("id")
    ]

    arrival_rows: List[Dict[str, Any]] = []

    for l in line_dim:
        line_id = l["line_id"]
        try:
            arrivals = get_arrivals_for_line(session, headers, line_id)
        except Exception as e:
            print(f"Arrivals failed for line {line_id}: {e}")
            continue

        # small jitter to reduce burstiness
        time.sleep(0.25 + random.random() * 0.25)

        for a in arrivals:
            arrival_rows.append({
                "snapshot_utc": snapshot_utc,
                "line_id": l["line_id"],
                "line_name": l["line_name"],
                "mode_name": l["mode_name"],
                "stop_point_id": a.get("naptanId"),
                "station_name": a.get("stationName"),
                "platform_name": a.get("platformName"),
                "direction": a.get("direction"),
                "destination_name": a.get("destinationName"),
                "expected_arrival": a.get("expectedArrival"),
                "time_to_station_sec": a.get("timeToStation"),
                "vehicle_id": a.get("vehicleId"),
            })

    df = pd.DataFrame(arrival_rows)

    # Defensive typing + dedupe (API can occasionally repeat rows)
    if not df.empty:
        df["snapshot_utc"] = pd.to_datetime(df["snapshot_utc"], utc=True)
        df["expected_arrival"] = pd.to_datetime(df["expected_arrival"], utc=True, errors="coerce")

        df.drop_duplicates(
            subset=[
                "snapshot_utc",
                "line_id",
                "stop_point_id",
                "platform_name",
                "direction",
                "expected_arrival",
                "vehicle_id",
            ],
            inplace=True
        )

        df.sort_values(
            by=["line_id", "station_name", "direction", "expected_arrival"],
            inplace=True,
            na_position="last"
        )

        df.reset_index(drop=True, inplace=True)

    # Partitioned output path (daily folder)
day_folder = snapshot_dt.strftime("%Y-%m-%d")
out_dir = os.path.join("data", "snapshots", f"dt={day_folder}")
os.makedirs(out_dir, exist_ok=True)

# One file per run (simple + append-only)
file_stamp = snapshot_dt.strftime("%H%M%S")
out_path = os.path.join(out_dir, f"tfl_status_{day_folder}_{file_stamp}Z.parquet")
 

    df.to_parquet(out_path, index=False)
    print(f"Wrote {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()
