import os
import io
import csv
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
        # If you forget to set it, fail closed.
        raise HTTPException(status_code=500, detail="Server misconfigured: missing C150_PASSWORD.")
    if (x_c150_password or "") != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


def read_csv_bytes(data: bytes) -> List[List[str]]:
    text = data.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    return [row for row in reader]


@app.get("/")
def home():
    # Helps avoid {"detail":"Not Found"} confusion when you hit the base URL.
    return {"status": "ok", "service": "C150 Telemetry Processor"}


@app.post("/preview-crew")
async def preview_crew(
    file: UploadFile = File(...),
    x_c150_password: str | None = Header(default=None),
):
    require_password(x_c150_password)

    data = await file.read()
    rows = read_csv_bytes(data)

    crew = find_crew_info_table(rows)
    if not crew:
        raise HTTPException(status_code=400, detail="Crew Info table not found (Position/Name/Abbr/Weight).")

    _header_r, start, end, cols = crew
    pos_c = cols.get("Position", 0)
    name_c = cols.get("Name")
    abbr_c = cols.get("Abbr")
    weight_c = cols.get("Weight")
    if name_c is None or abbr_c is None or weight_c is None:
        raise HTTPException(status_code=400, detail="Crew Info table missing Name/Abbr/Weight columns.")

    out = []
    for r in range(start, end):
        if len(rows[r]) == 0:
            continue
        pos = (rows[r][pos_c] or "").strip()
        if not pos.isdigit():
            continue
        if int(pos) < 1 or int(pos) > 8:
            continue
        pad_row(rows[r], max(name_c, abbr_c, weight_c) + 1)
        out.append({
            "pos": int(pos),
            "name": (rows[r][name_c] or "").strip(),
            "abbr": (rows[r][abbr_c] or "").strip(),
            "existing_weight": (rows[r][weight_c] or "").strip(),
        })

    out.sort(key=lambda x: x["pos"])
    return JSONResponse({"crew": out})


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

    # weights passed as JSON string: {"at4117":"68.2", "pos_1":"70.1", ...}
    weights_json: str = Form(...),

    x_c150_password: str | None = Header(default=None),
):
    require_password(x_c150_password)

    shell_clean = " ".join((shell or "").split()).upper()
    if not shell_clean:
        raise HTTPException(status_code=400, detail="Shell is required.")

    zone_clean = (zone or "").strip().upper()
    if zone_clean not in {"T1", "T2", "T3", "T4", "T5"}:
        raise HTTPException(status_code=400, detail="Zone must be one of T1..T5.")

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
    import json
    try:
        weights_obj = json.loads(weights_json)
        if not isinstance(weights_obj, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="weights_json must be a JSON object mapping abbr/pos keys to kg values.",
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

    # Normalize keys
    weights_by_abbr: Dict[str, str] = {}
    for k, v in weights_obj.items():
        kk = str(k).strip().lower()
        vv = str(v).strip()
        if kk:
            weights_by_abbr[kk] = vv

    data = await file.read()
    rows = read_csv_bytes(data)

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
        weights_by_abbr=weights_by_abbr,
    )

    # Write CSV to bytes
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(updated)
    out_bytes = buf.getvalue().encode("utf-8")

    return Response(
        content=out_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )
