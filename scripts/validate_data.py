#!/usr/bin/env python3
"""
QA validation for data/attendance.json.
Exits with code 0 on success, 1 on failure.
Run: python3 scripts/validate_data.py
"""

import json
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).parent.parent
DATA_PATH = ROOT / "data" / "attendance.json"

REQUIRED_PGY = {"PGY1", "PGY2", "PGY3", "PGY4"}
EXPECTED_REQUIRED_HOURS = {"PGY1": 190, "PGY2": 155, "PGY3": 185, "PGY4": 195}
MIN_RESIDENTS = 10
MIN_SESSIONS = 40
VALID_SESSION_TYPES = (int, float, str)

errors = []
warnings = []


def fail(msg): errors.append(f"  ✗ {msg}")
def warn(msg): warnings.append(f"  ⚠ {msg}")
def ok(msg): print(f"  ✓ {msg}")


def validate():
    # ── File exists ──
    if not DATA_PATH.exists():
        fail(f"data/attendance.json not found at {DATA_PATH}")
        return

    # ── JSON parses ──
    try:
        data = json.loads(DATA_PATH.read_text())
    except json.JSONDecodeError as e:
        fail(f"attendance.json is not valid JSON: {e}")
        return

    ok("attendance.json exists and is valid JSON")

    # ── Top-level keys ──
    required_keys = {"lastUpdated", "academicYear", "sessions", "residents", "requiredHours", "maxAsyncHours"}
    missing = required_keys - set(data.keys())
    if missing:
        fail(f"Missing top-level keys: {missing}")
    else:
        ok(f"All required top-level keys present")

    # ── requiredHours ──
    rh = data.get("requiredHours", {})
    for pgy, expected in EXPECTED_REQUIRED_HOURS.items():
        if rh.get(pgy) != expected:
            fail(f"requiredHours[{pgy}] = {rh.get(pgy)!r}, expected {expected}")
    if not errors:
        ok(f"Required hours by PGY correct: {rh}")

    # ── maxAsyncHours ──
    if data.get("maxAsyncHours") != 18.5:
        warn(f"maxAsyncHours = {data.get('maxAsyncHours')!r}, expected 18.5")
    else:
        ok("maxAsyncHours = 18.5")

    # ── Sessions ──
    sessions = data.get("sessions", [])
    if len(sessions) < MIN_SESSIONS:
        fail(f"Only {len(sessions)} sessions; expected >= {MIN_SESSIONS}")
    else:
        ok(f"{len(sessions)} sessions found")

    session_keys = {"date", "label", "tag", "maxHours"}
    bad_sessions = [i for i, s in enumerate(sessions) if not session_keys.issubset(s.keys())]
    if bad_sessions:
        fail(f"Sessions missing required keys at indices: {bad_sessions[:5]}")
    else:
        ok("All sessions have required keys (date, label, tag, maxHours)")

    # ── Session dates are sorted ──
    dates = [s.get("date", "") for s in sessions]
    if dates != sorted(dates):
        warn("Sessions are not in chronological order")
    else:
        ok("Sessions are in chronological order")

    # ── Session maxHours are positive ──
    zero_hrs = [s["label"] for s in sessions if s.get("maxHours", 0) == 0]
    if zero_hrs:
        warn(f"{len(zero_hrs)} sessions have maxHours=0: {zero_hrs[:3]}")

    # ── Residents ──
    residents = data.get("residents", [])
    if len(residents) < MIN_RESIDENTS:
        fail(f"Only {len(residents)} residents; expected >= {MIN_RESIDENTS}")
    else:
        ok(f"{len(residents)} residents found")

    # ── Resident schema ──
    res_keys = {"sid", "pgyLevel", "totalHours", "conferenceHours", "asyncHours", "sessions"}
    bad_res = [r.get("sid", f"idx={i}") for i, r in enumerate(residents) if not res_keys.issubset(r.keys())]
    if bad_res:
        fail(f"Residents missing required keys: {bad_res[:5]}")
    else:
        ok("All residents have required keys")

    # ── SID format ──
    bad_sids = [r["sid"] for r in residents if not str(r.get("sid", "")).startswith("S")]
    if bad_sids:
        fail(f"Residents with malformed SIDs: {bad_sids[:5]}")
    else:
        ok("All SIDs start with 'S'")

    # ── Unique SIDs ──
    sids = [r["sid"] for r in residents]
    if len(sids) != len(set(sids)):
        dupes = [s for s in set(sids) if sids.count(s) > 1]
        fail(f"Duplicate SIDs: {dupes}")
    else:
        ok("All SIDs are unique")

    # ── PGY levels ──
    pgy_values = {r.get("pgyLevel") for r in residents}
    unknown_pgy = pgy_values - REQUIRED_PGY
    if unknown_pgy:
        warn(f"Unexpected PGY levels in data: {unknown_pgy}")
    missing_pgy = REQUIRED_PGY - pgy_values
    if missing_pgy:
        warn(f"No residents found for PGY levels: {missing_pgy}")
    if not unknown_pgy and not missing_pgy:
        ok(f"PGY levels present: {sorted(pgy_values)}")

    # ── Session counts match header ──
    n_sessions = len(sessions)
    wrong_count = [r["sid"] for r in residents if len(r.get("sessions", [])) != n_sessions]
    if wrong_count:
        fail(f"{len(wrong_count)} residents have wrong session count (expected {n_sessions}): {wrong_count[:5]}")
    else:
        ok(f"All residents have {n_sessions} session entries")

    # ── Session values are valid types ──
    invalid_vals = []
    for r in residents:
        for i, v in enumerate(r.get("sessions", [])):
            if not isinstance(v, VALID_SESSION_TYPES):
                invalid_vals.append((r["sid"], i, v))
    if invalid_vals:
        fail(f"Invalid session values (expected number or string): {invalid_vals[:5]}")
    else:
        ok("All session values are valid types (number or string)")

    # ── Hours sanity check ──
    for r in residents:
        total = r.get("totalHours", 0)
        conf = r.get("conferenceHours", 0)
        asyn = r.get("asyncHours", 0)
        if total < 0 or conf < 0 or asyn < 0:
            fail(f"{r['sid']}: negative hours (total={total}, conf={conf}, async={asyn})")
        if conf > total + 1:  # +1 for float rounding
            warn(f"{r['sid']}: conferenceHours ({conf}) > totalHours ({total})")
    ok("Hours sanity checks passed")

    # ── lastUpdated is a parseable date ──
    lu = data.get("lastUpdated", "")
    try:
        # Handle both M/D/YYYY and YYYY-MM-DD formats
        if "/" in lu:
            parts = lu.split("/")
            date(int(parts[2]), int(parts[0]), int(parts[1]))
        else:
            date.fromisoformat(lu)
        ok(f"lastUpdated is a valid date: {lu}")
    except Exception:
        warn(f"lastUpdated '{lu}' could not be parsed as a date")


def main():
    print(f"\nValidating {DATA_PATH}\n")
    validate()

    print()
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(w)
        print()

    if errors:
        print("Errors:")
        for e in errors:
            print(e)
        print(f"\n❌  Validation failed with {len(errors)} error(s).\n")
        sys.exit(1)
    else:
        print(f"✅  All checks passed ({len(warnings)} warning(s)).\n")


if __name__ == "__main__":
    main()
