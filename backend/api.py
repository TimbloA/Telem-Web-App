import os
import io
import csv
import re
import json
from typing import Dict, List

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

# UNI-like abbreviation: ppf2107, wg2445, dl3847, etc.
UNI_RE = re.compile(r"^[a-z]{2,8}\d{3,8}$", re.IGNORECASE)

SECTION_FILE_INFO = "===== File Info"

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


def _row_first_cell(rows: List[List[str]], i: int) -> str:
    if i < 0 or i >= len(rows) or not rows[i]:
        return ""
    return (rows[i][0] or "").strip()


def trim_to_first_export(rows: List[List[str]]) -> List[List[str]]:
    """
    Clipboard pastes sometimes contain multiple full exports back-to-back.
    Keep only the FIRST export block.

    Heuristic:
    - Find all indices where first cell equals '===== File Info'
    - If there is more than one, truncate at the second occurrence.
    """
    indices = [i for i in range(len(rows)) if _row_first_cell(rows, i) == SECTION_FILE_INFO]
    if len(indices) <= 1:
        return rows
    return rows[:indices[1]]


def normalize_sections_to_error_markers(rows: List[List[str]]) -> List[List[str]]:
    """
    Your downstream parser expects '#ERROR!,<Section Name>' markers.
    Peach clipboard exports sometimes use '===== <Section Name>' instead.

    Convert '===== Something' rows into '#ERROR!,Something' so the parser can find sections.
    """
    out: List[List[str]] = []

    for row in rows:
        if not row:
            out.append(row)
            continue

        first = (row[0] or "").strip()

        # Convert "===== Section Name" rows to "#ERROR!,Section Name"
        if first.startswith("====="):
            title = first.lstrip("=").strip()

            # Special cases: the parser searches for these exact strings
            # '#ERROR!,Aperiodic,0x800A' and '#ERROR!,Periodic'
            lower = title.lower()

            if lower.startswith("aperiodic"):
                rest = title[len("Aperiodic"):].strip()
                rest = rest.lstrip(",").strip()
                if rest:
                    out.append(["#ERROR!", "Aperiodic", rest])
                else:
                    out.append(["#ERROR!", "Aperiodic"])
            elif lower.startswith("periodic"):
                out.append(["#ERROR!", "Periodic"])
            else:
                out.append(["#ERROR!", title])

            continue

        # Keep existing #ERROR! rows as-is
        out.append(row)

    return out


def parse_first_crew(rows: List[List[str]]) -> List[Dict[str, str]]:
    """
    Extract exactly seats 1..8 from the first (trimmed) export.
    Filters by Abbr being UNI-like to prevent leakage from other tables.
    """
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
    seen_seats = set()

    for r in range(start, end):
        if not rows[r]:
            continue

        pos_raw = (rows[r][pos_c] or "").strip()
        if not pos_raw.isdigit():
            continue

        pos_i = int(pos_raw)
        if pos_i < 1 or pos_i > 8:
            continue

        if pos_i in seen_seats:
            continue

        pad_row(rows[r], max(name_c, abbr_c, weight_c) + 1)

        name = (rows[r][name_c] or "").strip()
        abbr = (rows[r][abbr_c] or "").strip()
        existing_weight = (rows[r][weight_c] or "").strip()

        if not abbr or not UNI_RE.match(abbr):
            continue

        out.append({
            "pos": pos_i,
            "name": name,
            "abbr": abbr,
            "existing_weight": existing_weight,
        })
        seen_seats.add(pos_i)

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
    rows = normalize_sections_to_error_markers(rows)

    crew = parse_first_crew(rows)

    warning = None
    if len(crew) != 8:
        warning = f"Expected 8 athletes (seats 1–8), found {len(crew)}. Input may be malformed."

    payload = {"crew": crew}
    if warning:
        payload["warning"] = warning

    return JSONResponse(payload)


@app.post("/process")
async def process_file(
    file: UploadFile = File(...),

    # Metadata
    season: str = Form("FY26"),
    shell: str = Form(...),
    zone: str = Form(...),
    piece: str = Form(...),
    piece_number: str = Form(...),

    cox_uni: str = Form(...),
    rig_info: str = Form(...),

    wind: str = Form(...),         # integer string (m/s)
    stream: str = Form(...),       # integer string (m/s)
    temperature: str = Form(...),  # integer string (°C)

    # weights passed as JSON string: {"pos_1":"70.1", ...}
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

    # Enforce integers (backend guarantee)
    try:
        wind_i = str(int(wind))
        stream_i = str(int(stream))
        temp_i = str(int(temperature))
    except Exception:
        raise HTTPException(status_code=400, detail="Wind/Stream/Temperature must be integers (m/s, m/s, °C).")

    # Parse weights JSON
    try:
        weights_obj = json.loads(weights_json)
        if not isinstance(weights_obj, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="weights_json must be a JSON object mapping pos keys to kg values.",
        )

    # Validate seats 1-8 not blank and numeric > 0
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

    # Normalize keys (lowercase keys)
    weights_by_key: Dict[str, str] = {}
    for k, v in weights_obj.items():
        kk = str(k).strip().lower()
        vv = str(v).strip()
        if kk:
            weights_by_key[kk] = vv

    data = await file.read()
    rows = read_csv_bytes(data)

    # Keep only the first export if multiple were pasted
    rows = trim_to_first_export(rows)

    # Normalize section markers so downstream parsers work
    rows = normalize_sections_to_error_markers(rows)

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

    # Ensure section markers are still normalized after updates
    updated = normalize_sections_to_error_markers(updated)

    # Make output rectangular so Excel/Numbers don't visually "shift" columns
    max_len = max((len(r) for r in updated), default=0)
    for r in updated:
        pad_row(r, max_len)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(updated)
    out_bytes = buf.getvalue().encode("utf-8")

    return Response(
        content=out_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )