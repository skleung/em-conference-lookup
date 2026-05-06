#!/usr/bin/env python3
"""
Scrapes the published Smartsheet URL using a headless Chromium browser,
extracts all cell data, and writes data/attendance.json.

Requires: pip install playwright && python -m playwright install chromium

Usage: python scripts/scrape_smartsheet.py [--url URL]
"""

import argparse
import asyncio
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).parent.parent
OUT_PATH = ROOT / "data" / "attendance.json"

PUBLISHED_URL = "https://app.smartsheet.com/b/publish?EQBCT=1c8b6e2ebff7412699dcc511609ecc0b"

MAX_ASYNC_HOURS = 18.5
ACADEMIC_YEAR = "AY25-26"
REQUIRED_HOURS = {"PGY1": 190, "PGY2": 155, "PGY3": 185, "PGY4": 195}

# Column names we expect in order (used for validation / fallback mapping)
EXPECTED_FIXED_COLS = ["Primary", "SID", "PGY LEVEL",
                        "TOTAL DIDACTIC HOURS (CONFERENCE + ASYN)",
                        "CONFERENCE HOURS TO DATE", "ASYN. HOURS"]

NAME_COL_NAME   = "Primary"
SID_COL_NAME    = "SID"
PGY_COL_NAME    = "PGY LEVEL"
TOTAL_COL_NAME  = "TOTAL DIDACTIC HOURS (CONFERENCE + ASYN)"
CONF_COL_NAME   = "CONFERENCE HOURS TO DATE"
ASYNC_COL_NAME  = "ASYN. HOURS"


# ── DOM extraction JS ────────────────────────────────────────────────────────

EXTRACT_CELLS_JS = """() => {
    const cells = document.querySelectorAll('[data-client-index]');
    const result = {};
    for (const c of cells) {
        const idx = c.getAttribute('data-client-index');
        if (!idx || !idx.startsWith('rk:')) continue;
        if (result[idx]) continue;  // first occurrence wins (outer gridCell)
        result[idx] = c.innerText?.trim() ?? '';
    }
    return result;
}"""

EXTRACT_HEADERS_JS = """() => {
    const hdrs = document.querySelectorAll('.columnHeader:not(.iconColumn)');
    return Array.from(hdrs).map(h => h.innerText?.trim() ?? '');
}"""

SCROLL_GRID_JS = """(pos) => {
    const el = document.querySelector('.clsMC');
    if (el) { el.scrollTop = pos; return el.scrollHeight; }
    return 0;
}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_session_date(raw: str) -> Optional[date]:
    """Parse '6/25/25', '7/16/25 JC', '10/22/25* DISASTER DAY' → date."""
    token = str(raw).split()[0].replace("*", "").strip()
    try:
        m, d, y = token.split("/")
        y = int(y); y += 2000 if y < 100 else 0
        return date(y, int(m), int(d))
    except Exception:
        return None


def parse_session_tag(raw: str) -> str:
    parts = str(raw).strip().replace("*", "").split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def normalize_cell(val: str):
    """Return float for numeric strings, original string otherwise."""
    if val == "":
        return 0.0
    try:
        return float(val)
    except ValueError:
        return val


# ── Core scraper ──────────────────────────────────────────────────────────────

async def scrape(url: str) -> dict:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        sys.exit("playwright is required: pip install playwright && "
                 "python -m playwright install chromium")

    print(f"Launching browser → {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # Wide viewport so all 57 columns render without horizontal scrolling
        page = await browser.new_page(viewport={"width": 6000, "height": 1200})

        print("Loading page…")
        await page.goto(url, timeout=90_000, wait_until="domcontentloaded")
        await asyncio.sleep(10)
        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        await asyncio.sleep(3)

        # ── Extract column headers (left→right DOM order) ──
        headers: list[str] = await page.evaluate(EXTRACT_HEADERS_JS)
        print(f"Column headers found: {len(headers)}  first few: {headers[:6]}")

        # ── Scroll grid vertically to expose all rows ──
        cells: dict[str, str] = {}
        scroll_top = 0
        scroll_height = await page.evaluate(SCROLL_GRID_JS, 0)
        step = 300

        while scroll_top <= scroll_height:
            batch = await page.evaluate(EXTRACT_CELLS_JS)
            new = {k: v for k, v in batch.items() if k not in cells}
            cells.update(new)
            scroll_top += step
            scroll_height = await page.evaluate(SCROLL_GRID_JS, scroll_top)
            await asyncio.sleep(0.4)

        await browser.close()

    rk_pattern = re.compile(r'rk:(\d+):')
    unique_rows = len(set(m.group(1) for k in cells if (m := rk_pattern.match(k))))
    print(f"Captured {len(cells)} cells, {unique_rows} rows")

    # ── Parse cell index → (row_key, col_idx) ──
    parsed: dict[str, dict[int, str]] = {}  # row_key → {col_idx: value}
    for idx_str, val in cells.items():
        m = re.match(r'rk:(\d+):(\d+)', idx_str)
        if not m:
            continue
        rk, ci = m.group(1), int(m.group(2))
        parsed.setdefault(rk, {})[ci] = val

    # ── Map column names to indices ──
    # Find min column index across all cells to establish offset
    all_col_idxs = sorted({ci for row in parsed.values() for ci in row})
    print(f"Column indices range: {all_col_idxs[0]}–{all_col_idxs[-1]}  ({len(all_col_idxs)} cols)")

    if len(headers) != len(all_col_idxs):
        print(f"Warning: {len(headers)} headers but {len(all_col_idxs)} column indices — using positional mapping")

    col_name_to_idx = {name: all_col_idxs[i] for i, name in enumerate(headers) if i < len(all_col_idxs)}
    col_idx_to_name = {v: k for k, v in col_name_to_idx.items()}

    # ── Identify special rows ──
    sid_col_idx   = col_name_to_idx.get(SID_COL_NAME)
    name_col_idx  = col_name_to_idx.get(NAME_COL_NAME)
    pgy_col_idx   = col_name_to_idx.get(PGY_COL_NAME)
    total_col_idx = col_name_to_idx.get(TOTAL_COL_NAME)
    conf_col_idx  = col_name_to_idx.get(CONF_COL_NAME)
    async_col_idx = col_name_to_idx.get(ASYNC_COL_NAME)

    # Session columns (after the 6 fixed columns)
    session_col_idxs = [ci for ci in all_col_idxs if ci not in col_name_to_idx.values()
                        or col_idx_to_name.get(ci) not in EXPECTED_FIXED_COLS]
    # More precisely: all cols past ASYNC_COL
    if async_col_idx:
        session_col_idxs = [ci for ci in all_col_idxs if ci > async_col_idx]

    # Session headers
    sessions = []
    for ci in session_col_idxs:
        raw_header = col_idx_to_name.get(ci, "")
        dt = parse_session_date(raw_header)
        if dt is None:
            continue
        sessions.append({
            "colIdx": ci,
            "date": dt.isoformat(),
            "label": raw_header.replace("*", "").strip(),
            "tag": parse_session_tag(raw_header),
            "maxHours": 0.0,  # filled from max-hours row below
        })

    # ── Find max-hours row ("Last Updated:" in SID column) ──
    last_updated = date.today().isoformat()
    total_possible = 250.0

    for rk, row in parsed.items():
        sid_val = row.get(sid_col_idx, "")
        if "Last Updated" in sid_val:
            pgy_val = row.get(pgy_col_idx, "")
            # Parse last-updated date
            try:
                if "/" in pgy_val:
                    m2, d2, y2 = pgy_val.split("/")
                    y2 = int(y2); y2 += 2000 if y2 < 100 else 0
                    last_updated = date(y2, int(m2), int(d2)).isoformat()
                else:
                    last_updated = date.fromisoformat(pgy_val).isoformat()
            except Exception:
                pass
            tot = row.get(total_col_idx, "250.0")
            try:
                total_possible = float(tot)
            except Exception:
                pass
            # Fill maxHours per session
            for s in sessions:
                raw = row.get(s["colIdx"], "0")
                try:
                    s["maxHours"] = float(raw)
                except Exception:
                    s["maxHours"] = 0.0
            break

    print(f"Last updated: {last_updated}, total possible: {total_possible}")

    # ── Build residents list ──
    residents = []
    for rk, row in parsed.items():
        sid = row.get(sid_col_idx, "").strip()
        if not sid.startswith("S") or len(sid) != 8:
            continue  # skip non-resident rows

        def get_float(ci):
            try:
                return float(row.get(ci, 0) or 0)
            except Exception:
                return 0.0

        session_vals = []
        for s in sessions:
            raw = row.get(s["colIdx"], "")
            session_vals.append(normalize_cell(raw))

        residents.append({
            "sid": sid,
            "pgyLevel": row.get(pgy_col_idx, "").strip(),
            "totalHours": get_float(total_col_idx),
            "conferenceHours": get_float(conf_col_idx),
            "asyncHours": get_float(async_col_idx),
            "sessions": session_vals,
        })

    # Sort residents alphabetically by name
    residents.sort(key=lambda r: r["sid"])

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


def main():
    parser = argparse.ArgumentParser(description="Scrape Smartsheet published URL → attendance.json")
    parser.add_argument("--url", default=PUBLISHED_URL, help="Published Smartsheet URL")
    args = parser.parse_args()

    data = asyncio.run(scrape(args.url))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(data, indent=2))
    print(f"Written {len(data['residents'])} residents, {len(data['sessions'])} sessions → {OUT_PATH}")


if __name__ == "__main__":
    main()
