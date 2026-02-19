import os
import io
import csv
import re
import json
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Header
from fastapi.responses import Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from telem_engine import (
    pad_row,
    find_crew_info_table,
    build_output_filename,
    apply_updates,
)

APP_PASSWORD = os.environ.get("C150_PASSWORD", "")
ALLOWED_ORIGINS = os.environ.get("CORS_ORIGINS", "*").split(",")

UNI_RE = re.compile(r"^[a-z]{2,8}\d{3,8}$", re.IGNORECASE)

app = FastAPI(title="C150 Telemetry Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS] if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


def require_password(x_c150_password: str | None):
    if not APP_PASSWORD:
        raise HTTPException(status_code=500, detail="Server misconfigured: missing C150_PASSWORD.")
    if (x_c150_password or "") != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


def read_csv_bytes(data: bytes) -> List[List[str]]:
    text = data.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    return [row for row in reader]


def _header_signature(row: List[str]) -> set:
    return {str(c).strip() for c in row if c is not None and str(c).strip() != ""}


def trim_to_first_export(rows: List[List[str]]) -> List[List[str]]:
    """
    If clipboard paste contains multiple exports concatenated, keep only the first.
    We cut at the 2nd occurrence of the File Info header signature row.
    """
    hits = []
    for i, row in enumerate(rows):
        sig = _header_signature(row)
        if {"Serial #", "Session", "Filename", "Start Time"}.issubset(sig):
            hits.append(i)
    if len(hits) <= 1:
        return rows
    return rows[:hits[1] - 1]


def sanitize_for_naive_split_parser(rows: List[List[str]]) -> List[List[str]]:
    """
    Your parser uses line.split(',') (not a CSV parser).
    So commas/quotes inside values will break it. Remove them.
    """
    out: List[List[str]] = []
    for row in rows:
        new_row: List[str] = []
        for cell in row:
            s = "" if cell is None else str(cell)
            s = s.replace(",", " ")
            s = s.replace('"', "")
            new_row.append(s)
        out.append(new_row)
    return out


def normalize_section_markers(rows: List[List[str]]) -> List[List[str]]:
    """
    CRITICAL FIX:
    Your outputs contain '=====,Section Name' but the parser only recognizes '#ERROR!,Section Name'.
    Convert any '=====' style markers into '#ERROR!' markers.
    """
    out = [list(r) for r in rows]

    for i, row in enumerate(out):
        if not row:
            continue

        first = (row[0] or "").strip()

        # Case A: first cell is exactly =====
        if first == "=====":
            row[0] = "#ERROR!"
            out[i] = row
            continue

        # Case B: first cell contains "===== File Info" (single-cell marker)
        if first.startswith("=====") and "," not in first:
            # Convert "===== File Info" -> ["#ERROR!", "File Info"]
            section = first.replace("=====", "").strip()
            out[i] = ["#ERROR!", section]
            continue

        # Case C: Already "#ERROR!" is fine
        # (Leave it alone.)

    return out


def label_bare_error_markers(rows: List[List[str]]) -> List[List[str]]:
    """
    If there is a bare '#ERROR!' line with no section name, infer it from the next header row.
    (This is a bonus safety net; your main issue is '=====' markers.)
    """
    out = [list(r) for r in rows]

    def next_nonempty_row(start_idx: int) -> Optional[int]:
        j = start_idx
        while j < len(out):
            if out[j] and any(str(c).strip() for c in out[j]):
                return j
            j += 1
        return None

    i = 0
    while i < len(out):
        row = out[i]
        if not row:
            i += 1
            continue

        first = (row[0] or "").strip()

        if first == "#ERROR!" and (len(row) < 2 or (row[1] or "").strip() == ""):
            j = next_nonempty_row(i + 1)
            if j is None:
                i += 1
                continue

            sig = _header_signature(out[j])

            if {"Serial #", "Session", "Filename", "Start Time"}.issubset(sig):
                out[i] = ["#ERROR!", "File Info"]
            elif {"Lat", "Lon", "UTC", "PeachTime"}.issubset(sig):
                out[i] = ["#ERROR!", "GPS Info"]
            elif {"Position", "Name", "Abbr", "Weight"}.issubset(sig):
                out[i] = ["#ERROR!", "Crew Info"]
            elif {"Start", "End", "#", "Duration", "Distance", "Rating", "Pace", "comment", "Wind", "Stream", "Validated"}.issubset(sig):
                out[i] = ["#ERROR!", "Piece"]
            elif any("Aperiodic" in str(c) for c in out[j]) and any("0x800A" in str(c) for c in out[j]):
                out[i] = ["#ERROR!", "Aperiodic", "0x800A"]
            elif any("Periodic" in str(c) for c in out[j]):
                out[i] = ["#ERROR!", "Periodic"]

        i += 1

    return out


def parse_first_crew(rows: List[List[str]]) -> List[Dict[str, str]]:
    crew_meta = find_crew_info_table(rows)
    if not crew_meta:
        raise HTTPException(status_code=400, detail="Crew Info table not found (Position/Name/Abbr/Weight).")

    _header_r, start, end, cols = crew_meta
    pos_c = cols.get("Position", 0)
    name_c = cols.get("Name")
    abbr_c = cols.get("Abbr")
    weight_c = cols.get("Weight")

    if name_c is None or abbr_c is None or weight_c is None:
        raise HTTPException(status_code=400, detail="Crew Info table missing Name/Abbr/Weight columns.")

    out: List[Dict[str, str]] = []
    seen = set()

    for r in range(start, end):
        if not rows[r]:
            continue
        pos_raw = (rows[r][pos_c] or "").strip()
        if not pos_raw.isdigit():
            continue
        pos = int(pos_raw)
        if pos < 1 or pos > 8 or pos in seen:
            continue

        pad_row(rows[r], max(name_c, abbr_c, weight_c) + 1)
        name = (rows[r][name_c] or "").strip()
        abbr = (rows[r][abbr_c] or "").strip()
        w = (rows[r][weight_c] or "").strip()

        if not abbr or not UNI_RE.match(abbr):
            continue

        out.append({"pos": pos, "name": name, "abbr": abbr, "existing_weight": w})
        seen.add(pos)
        if len(out) == 8:
            break

    out.sort(key=lambda x: x["pos"])
    return out


@app.get("/")
def home():
    return {"status": "ok", "service": "C150 Telemetry Processor"}


@app.post("/preview-crew")
async def preview_crew(
    file: UploadFile = File(...),
    x_c150_password: str | None = Header(default=None),
):
    require_password(x_c150_password)

    data = await file.read()
    rows = read_csv_bytes(data)

    rows = trim_to_first_export(rows)
    rows = normalize_section_markers(rows)
    rows = label_bare_error_markers(rows)
    rows = sanitize_for_naive_split_parser(rows)

    crew = parse_first_crew(rows)
    payload = {"crew": crew}
    if len(crew) != 8:
        payload["warning"] = f"Expected 8 athletes (seats 1–8), found {len(crew)}."
    return JSONResponse(payload)


@app.post("/process")
async def process_file(
    file: UploadFile = File(...),

    season: str = Form("FY26"),
    shell: str = Form(...),
    zone: str = Form(...),
    piece: str = Form(...),
    piece_number: str = Form(...),

    cox_uni: str = Form(...),
    rig_info: str = Form(...),

    wind: str = Form(...),
    stream: str = Form(...),
    temperature: str = Form(...),

    weights_json: str = Form(...),

    x_c150_password: str | None = Header(default=None),
):
    require_password(x_c150_password)

    shell_clean = " ".join((shell or "").split()).upper()
    if not shell_clean:
        raise HTTPException(status_code=400, detail="Shell is required.")

    zone_clean = (zone or "").strip().upper()
    if zone_clean not in {"T1", "T2", "T3", "T4", "T5", "T6"}:
        raise HTTPException(status_code=400, detail="Zone must be one of T1..T6.")

    piece_clean = (piece or "").strip()
    if not piece_clean:
        raise HTTPException(status_code=400, detail="Piece is required.")

    try:
        piece_number_clean = str(int(str(piece_number).strip()))
    except Exception:
        raise HTTPException(status_code=400, detail="Piece number must be an integer.")

    try:
        wind_i = str(int(wind))
        stream_i = str(int(stream))
        temp_i = str(int(temperature))
    except Exception:
        raise HTTPException(status_code=400, detail="Wind/Stream/Temperature must be integers (m/s, m/s, °C).")

    try:
        weights_obj = json.loads(weights_json)
        if not isinstance(weights_obj, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(status_code=400, detail="weights_json must be a JSON object mapping pos keys to kg values.")

    for seat in range(1, 9):
        k = f"pos_{seat}"
        raw = str(weights_obj.get(k, "")).strip()
        if raw == "":
            raise HTTPException(status_code=400, detail=f"Missing weight for seat {seat} (kg).")
        try:
            val = float(raw)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid weight for seat {seat}: must be numeric kg.")
        if val <= 0:
            raise HTTPException(status_code=400, detail=f"Invalid weight for seat {seat}: must be > 0 kg.")

    weights_by_key: Dict[str, str] = {}
    for k, v in weights_obj.items():
        kk = str(k).strip().lower()
        vv = str(v).strip()
        if kk:
            weights_by_key[kk] = vv

    data = await file.read()
    rows = read_csv_bytes(data)

    rows = trim_to_first_export(rows)
    rows = normalize_section_markers(rows)
    rows = label_bare_error_markers(rows)

    out_name = build_output_filename(
        season=season,
        rows=rows,
        shell=shell_clean,
        zone=zone_clean,
        piece=piece_clean,
        piece_num=piece_number_clean,
    )

    updated = apply_updates(
        rows=rows,
        cox_uni=cox_uni.strip(),
        rig_info=rig_info.strip(),
        wind=wind_i,
        stream=stream_i,
        temperature=temp_i,
        zone=zone_clean,
        weights_by_abbr=weights_by_key,
    )

    # Ensure markers are what your parser expects
    updated = normalize_section_markers(updated)
    updated = label_bare_error_markers(updated)

    # Make safe for naive line.split(',')
    updated = sanitize_for_naive_split_parser(updated)

    # Rectangular output (nice for Sheets/Excel)
    max_len = max((len(r) for r in updated), default=0)
    for r in updated:
        pad_row(r, max_len)

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(updated)
    out_bytes = buf.getvalue().encode("utf-8")

    return Response(
        content=out_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )