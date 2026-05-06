#!/usr/bin/env python3
"""
Generates data/attendance.json from either:
  --from-xlsx  : parses the local XLSX file (bootstrap / dev mode)
  (default)    : fetches from Smartsheet API using env vars
                 SMARTSHEET_API_KEY and SMARTSHEET_SHEET_ID
"""

import argparse
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent.parent
DEFAULT_XLSX_PATH = ROOT / "Resident Attendance View AY'25-'26.xlsx"
OUT_PATH = ROOT / "data" / "attendance.json"

MAX_ASYNC_HOURS = 18.5
ACADEMIC_YEAR = "AY25-26"

REQUIRED_HOURS = {
    "PGY1": 190,
    "PGY2": 155,
    "PGY3": 185,
    "PGY4": 195,
}

# Columns (0-indexed): 0=name, 1=SID, 2=PGY, 3=totalHrs, 4=confHrs, 5=asyncHrs, 6+=sessions
NAME_COL, SID_COL, PGY_COL = 0, 1, 2
TOTAL_COL, CONF_COL, ASYNC_COL = 3, 4, 5
SESSION_START = 6

NON_RESIDENT_PGY = {"Policy"}  # PGY1-4 rows without SIDs are caught by the SID check below


def parse_date(raw: str) -> Optional[date]:
    """Parse a date string like '6/25/25', '7/16/25 JC', '10/22/25* DISASTER DAY'."""
    raw = str(raw).strip()
    # Take only the first token (the date part)
    token = raw.split()[0].replace("*", "").strip()
    try:
        parts = token.split("/")
        m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
        if y < 100:
            y += 2000
        return date(y, m, d)
    except Exception:
        return None


def parse_tag(raw: str) -> str:
    """Extract the label/tag after the date, e.g. 'JC', 'TC DAY', 'RETREAT'."""
    raw = str(raw).strip().replace("*", "")
    parts = raw.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def normalize_session_value(val):
    """Return numeric hours or a string label for a session cell."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s in ("Vacation", "LOA", "A. Elective", "A.Elective"):
        return s
    try:
        return float(s)
    except ValueError:
        return s


def from_xlsx(xlsx_path: Optional[Path] = None) -> dict:
    try:
        import openpyxl
    except ImportError:
        sys.exit("openpyxl is required: pip install openpyxl")

    path = xlsx_path or DEFAULT_XLSX_PATH
    if not path.exists():
        sys.exit(f"XLSX file not found: {path}")
    wb = openpyxl.load_workbook(str(path), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    # Row 0 = headers, Row 1 = max-hours-per-session row, Rows 2+ = data
    header_row = rows[0]
    max_row = rows[1]

    # Parse last-updated from max_row[2] (col 3 = PGY LEVEL cell stores the date)
    last_updated_raw = max_row[2]
    if isinstance(last_updated_raw, (date, datetime)):
        last_updated = last_updated_raw.strftime("%Y-%m-%d") if isinstance(last_updated_raw, datetime) else last_updated_raw.isoformat()
    else:
        last_updated = str(last_updated_raw) if last_updated_raw else ""

    # Build sessions list from header row columns SESSION_START+
    sessions = []
    for col_idx in range(SESSION_START, len(header_row)):
        raw_header = header_row[col_idx]
        if not raw_header:
            continue
        dt = parse_date(str(raw_header))
        if dt is None:
            continue
        max_hrs_val = max_row[col_idx]
        max_hrs = float(max_hrs_val) if isinstance(max_hrs_val, (int, float)) else 0.0
        sessions.append({
            "colIdx": col_idx,
            "date": dt.isoformat(),
            "label": str(raw_header).replace("*", "").strip(),
            "tag": parse_tag(str(raw_header)),
            "maxHours": max_hrs,
        })

    # Total possible hours from max_row col 3 (TOTAL DIDACTIC HOURS header)
    total_possible = float(max_row[TOTAL_COL]) if isinstance(max_row[TOTAL_COL], (int, float)) else 250.0

    # Parse resident rows (skip special rows)
    residents = []
    for row in rows[2:]:
        sid = row[SID_COL]
        if not sid or not str(sid).startswith("S"):
            continue

        session_vals = []
        for s in sessions:
            raw = row[s["colIdx"]] if s["colIdx"] < len(row) else None
            session_vals.append(normalize_session_value(raw))

        residents.append({
            "name": str(row[NAME_COL]).strip() if row[NAME_COL] else "",
            "sid": str(sid).strip(),
            "pgyLevel": str(row[PGY_COL]).strip() if row[PGY_COL] else "",
            "totalHours": float(row[TOTAL_COL]) if isinstance(row[TOTAL_COL], (int, float)) else 0.0,
            "conferenceHours": float(row[CONF_COL]) if isinstance(row[CONF_COL], (int, float)) else 0.0,
            "asyncHours": float(row[ASYNC_COL]) if isinstance(row[ASYNC_COL], (int, float)) else 0.0,
            "sessions": session_vals,
        })

    # Strip colIdx from output sessions (internal use only)
    out_sessions = [{k: v for k, v in s.items() if k != "colIdx"} for s in sessions]

    return {
        "lastUpdated": last_updated,
        "academicYear": ACADEMIC_YEAR,
        "maxAsyncHours": MAX_ASYNC_HOURS,
        "totalPossibleHours": total_possible,
        "requiredHours": REQUIRED_HOURS,
        "sessions": out_sessions,
        "residents": residents,
    }


def from_api() -> dict:
    import urllib.request

    api_key = os.environ.get("SMARTSHEET_API_KEY", "").strip()
    sheet_id = os.environ.get("SMARTSHEET_SHEET_ID", "").strip()
    if not api_key or not sheet_id:
        sys.exit("Set SMARTSHEET_API_KEY and SMARTSHEET_SHEET_ID environment variables.")

    url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    with urllib.request.urlopen(req) as resp:
        sheet = json.loads(resp.read())

    columns = sheet.get("columns", [])
    col_map = {c["id"]: i for i, c in enumerate(columns)}

    # Identify session columns (those after the first 6)
    session_col_ids = [c["id"] for i, c in enumerate(columns) if i >= SESSION_START]

    # Row 0 in the API is row index 0 (the header/max row) — find by looking for "Last Updated:" cell
    api_rows = sheet.get("rows", [])

    def cell_val(row, col_id):
        for cell in row.get("cells", []):
            if cell.get("columnId") == col_id:
                return cell.get("value")
        return None

    # Find max-hours row (row where SID col = "Last Updated:")
    sid_col_id = columns[SID_COL]["id"] if len(columns) > SID_COL else None
    pgy_col_id = columns[PGY_COL]["id"] if len(columns) > PGY_COL else None
    total_col_id = columns[TOTAL_COL]["id"] if len(columns) > TOTAL_COL else None
    conf_col_id = columns[CONF_COL]["id"] if len(columns) > CONF_COL else None
    async_col_id = columns[ASYNC_COL]["id"] if len(columns) > ASYNC_COL else None
    name_col_id = columns[NAME_COL]["id"] if len(columns) > NAME_COL else None

    sessions = []
    last_updated = date.today().isoformat()
    total_possible = 250.0

    for api_row in api_rows:
        sid_val = cell_val(api_row, sid_col_id) if sid_col_id else None
        if str(sid_val) == "Last Updated:":
            # This is the max-hours row
            pgy_val = cell_val(api_row, pgy_col_id)
            if pgy_val:
                try:
                    if hasattr(pgy_val, "strftime"):
                        last_updated = pgy_val.strftime("%Y-%m-%d")
                    else:
                        last_updated = str(pgy_val)
                except Exception:
                    pass
            tot_val = cell_val(api_row, total_col_id)
            if isinstance(tot_val, (int, float)):
                total_possible = float(tot_val)
            for col in columns[SESSION_START:]:
                raw_title = col.get("title", "")
                dt = parse_date(raw_title)
                if dt is None:
                    continue
                max_hrs_val = cell_val(api_row, col["id"])
                max_hrs = float(max_hrs_val) if isinstance(max_hrs_val, (int, float)) else 0.0
                sessions.append({
                    "colId": col["id"],
                    "date": dt.isoformat(),
                    "label": raw_title.replace("*", "").strip(),
                    "tag": parse_tag(raw_title),
                    "maxHours": max_hrs,
                })
            break

    # Parse residents
    residents = []
    for api_row in api_rows:
        sid = cell_val(api_row, sid_col_id) if sid_col_id else None
        if not sid or not str(sid).startswith("S"):
            continue

        session_vals = [normalize_session_value(cell_val(api_row, s["colId"])) for s in sessions]
        residents.append({
            "name": str(cell_val(api_row, name_col_id) or "").strip(),
            "sid": str(sid).strip(),
            "pgyLevel": str(pgy).strip(),
            "totalHours": float(v) if isinstance((v := cell_val(api_row, total_col_id)), (int, float)) else 0.0,
            "conferenceHours": float(v) if isinstance((v := cell_val(api_row, conf_col_id)), (int, float)) else 0.0,
            "asyncHours": float(v) if isinstance((v := cell_val(api_row, async_col_id)), (int, float)) else 0.0,
            "sessions": session_vals,
        })

    out_sessions = [{k: v for k, v in s.items() if k != "colId"} for s in sessions]
    return {
        "lastUpdated": last_updated,
        "academicYear": ACADEMIC_YEAR,
        "maxAsyncHours": MAX_ASYNC_HOURS,
        "totalPossibleHours": total_possible,
        "requiredHours": REQUIRED_HOURS,
        "sessions": out_sessions,
        "residents": residents,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate attendance.json")
    parser.add_argument("--from-xlsx", action="store_true", help="Parse local XLSX instead of Smartsheet API")
    parser.add_argument("--xlsx-path", type=Path, default=None, help="Path to XLSX file (default: repo-root XLSX)")
    args = parser.parse_args()

    if args.from_xlsx or args.xlsx_path:
        print(f"Mode: XLSX ({args.xlsx_path or DEFAULT_XLSX_PATH})")
        data = from_xlsx(args.xlsx_path)
    else:
        print("Mode: Smartsheet API")
        data = from_api()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(data, indent=2))
    print(f"Written {len(data['residents'])} residents, {len(data['sessions'])} sessions → {OUT_PATH}")


if __name__ == "__main__":
    main()
