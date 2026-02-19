import re
from datetime import datetime, date
from typing import List, Tuple, Optional, Dict


# ----------------------------
# Helpers
# ----------------------------

def pad_row(row: List[str], min_len: int) -> List[str]:
    if len(row) < min_len:
        row.extend([""] * (min_len - len(row)))
    return row


def is_section_header_row(row: List[str]) -> bool:
    # Peach section headers look like: "===== Crew Info", "===== Piece", etc.
    if not row:
        return False
    return ((row[0] or "").strip().startswith("====="))


def find_first_cell(rows: List[List[str]], target: str) -> Optional[Tuple[int, int]]:
    for r_idx, row in enumerate(rows):
        for c_idx, cell in enumerate(row):
            if (cell or "").strip() == target:
                return r_idx, c_idx
    return None


def header_col_map(header_row: List[str]) -> dict:
    return {cell.strip(): idx for idx, cell in enumerate(header_row) if cell is not None and cell.strip() != ""}


def find_piece_header_row(rows: List[List[str]]) -> Optional[int]:
    """
    Finds the header row of the Piece table (the one containing columns like:
    Pace, comment, Wind, Stream, Validated)
    """
    required = {"Pace", "comment", "Wind", "Stream", "Validated"}
    for r_idx, row in enumerate(rows):
        s = {cell.strip() for cell in row if cell is not None and str(cell).strip() != ""}
        if required.issubset(s):
            return r_idx
    return None


def find_table_bounds_from_header(rows: List[List[str]], header_r: int) -> Tuple[int, int]:
    """
    Given a header row index, returns (start, end) where:
      start = first data row after header
      end = first row index where next section header begins OR EOF
    """
    start = header_r + 1
    end = start
    while end < len(rows):
        if is_section_header_row(rows[end]):
            break
        end += 1
    return start, end


def find_crew_info_table(rows: List[List[str]]) -> Optional[Tuple[int, int, int, dict]]:
    """
    Locate the Crew Info table.

    Returns (header_row_idx, start_row_idx, end_row_idx_exclusive, col_map)

    Long-term fix:
    - End the table at the next '=====' section header (NOT '#ERROR!')
    - Stop if we hit Cox/Coach lines
    """
    required = {"Position", "Name", "Abbr", "Weight"}
    for r_idx, row in enumerate(rows):
        s = {cell.strip() for cell in row if cell is not None and str(cell).strip() != ""}
        if required.issubset(s):
            col_map = header_col_map(row)
            start = r_idx + 1
            end = start

            while end < len(rows):
                if is_section_header_row(rows[end]):
                    break

                first = ((rows[end][0] if rows[end] else "") or "").strip().lower()
                if first in {"cox", "coach"}:
                    break

                end += 1

            return r_idx, start, end, col_map
    return None


def sanitize_token(s: str) -> str:
    """Make a string safe for filenames."""
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def extract_yyyymmdd(rows: List[List[str]]) -> str:
    header_pos = find_first_cell(rows, "Start Time")
    if header_pos:
        r, c = header_pos
        if r + 1 < len(rows) and c < len(rows[r + 1]):
            raw = (rows[r + 1][c] or "").strip()
            m = re.search(r"(\d{1,2} [A-Za-z]{3} \d{4})", raw)
            if m:
                try:
                    dt = datetime.strptime(m.group(1), "%d %b %Y")
                    return dt.strftime("%Y%m%d")
                except Exception:
                    pass
    return date.today().strftime("%Y%m%d")


def build_output_filename(season: str, rows: List[List[str]], shell: str, zone: str, piece: str, piece_num: str) -> str:
    """
    Season_YYYYMMDD_BoatName_Zone_Piece_PieceNumber.csv
    """
    yyyymmdd = extract_yyyymmdd(rows)

    season_clean = sanitize_token((season or "FY26").strip()) or "FY26"
    shell_clean = " ".join((shell or "").split()).upper()
    zone_clean = (zone or "").strip().upper()
    piece_clean = (piece or "").strip()

    try:
        piece_num_clean = str(int(str(piece_num).strip()))
    except Exception:
        piece_num_clean = sanitize_token(str(piece_num))

    safe_season = sanitize_token(season_clean)
    safe_shell = sanitize_token(shell_clean)
    safe_zone = sanitize_token(zone_clean)
    safe_piece = sanitize_token(piece_clean)
    safe_piece_num = sanitize_token(piece_num_clean)

    if not safe_shell:
        raise ValueError("BoatName (Shell) is required for filename.")
    if not safe_zone:
        raise ValueError("Zone is required for filename.")
    if not safe_piece:
        raise ValueError("Piece is required for filename.")
    if not safe_piece_num:
        raise ValueError("Piece number is required for filename.")

    return f"{safe_season}_{yyyymmdd}_{safe_shell}_{safe_zone}_{safe_piece}_{safe_piece_num}.csv"


# ----------------------------
# Core update logic
# ----------------------------

PACE_LIKE = re.compile(r".*:\d{2}.*")  # loose match to catch 1:31.1 etc


def _ensure_column_between(
    rows: List[List[str]],
    header_r: int,
    table_start: int,
    table_end: int,
    left_col_name: str,
    new_col_name: str,
    right_col_name: str,
) -> int:
    """
    Ensure `new_col_name` exists between `left_col_name` and `right_col_name`
    in the header row. If missing, insert it into ALL rows within the table bounds.
    Returns the column index of new_col_name (after insertion if done).
    """
    header = rows[header_r]
    cols = header_col_map(header)

    # If already exists, return its index.
    if new_col_name in cols:
        return cols[new_col_name]

    if left_col_name not in cols or right_col_name not in cols:
        raise ValueError(f"Cannot insert {new_col_name}: missing {left_col_name} or {right_col_name} in Piece header.")

    left_idx = cols[left_col_name]
    right_idx = cols[right_col_name]

    # Insert immediately to the right of left_col
    insert_at = left_idx + 1

    # Insert column into header + all table rows
    for r in range(header_r, table_end):
        pad_row(rows[r], insert_at)
        rows[r].insert(insert_at, "")

    # Set header cell
    rows[header_r][insert_at] = new_col_name
    return insert_at


def _ensure_column_after(
    rows: List[List[str]],
    header_r: int,
    table_end: int,
    anchor_col_name: str,
    new_col_name: str,
) -> int:
    """
    Ensure `new_col_name` exists immediately after `anchor_col_name`.
    If missing, insert it into ALL rows within the table bounds.
    Returns the column index of new_col_name.
    """
    header = rows[header_r]
    cols = header_col_map(header)

    if new_col_name in cols:
        return cols[new_col_name]

    if anchor_col_name not in cols:
        raise ValueError(f"Cannot insert {new_col_name}: missing {anchor_col_name} in Piece header.")

    anchor_idx = cols[anchor_col_name]
    insert_at = anchor_idx + 1

    for r in range(header_r, table_end):
        pad_row(rows[r], insert_at)
        rows[r].insert(insert_at, "")

    rows[header_r][insert_at] = new_col_name
    return insert_at


def apply_updates(
    rows: List[List[str]],
    cox_uni: str,
    rig_info: str,
    wind: str,
    stream: str,
    temperature: str,
    zone: str,
    weights_by_abbr: Dict[str, str],
) -> List[List[str]]:
    # 1) Cox UNI: two cells right of "Cox"
    cox_pos = find_first_cell(rows, "Cox")
    if not cox_pos:
        raise ValueError('Could not find a cell named exactly "Cox".')
    cox_r, cox_c = cox_pos
    pad_row(rows[cox_r], cox_c + 3)
    rows[cox_r][cox_c + 2] = cox_uni

    # 2) Crew weights (bounded correctly)
    crew = find_crew_info_table(rows)
    if not crew:
        raise ValueError("Could not locate Crew Info table (Position/Name/Abbr/Weight).")
    _crew_header_r, crew_start, crew_end, crew_cols = crew

    abbr_c = crew_cols.get("Abbr")
    weight_c = crew_cols.get("Weight")
    if abbr_c is None or weight_c is None:
        raise ValueError("Crew Info table missing Abbr or Weight column.")

    # Write weights for seats 1–8 only
    for r in range(crew_start, crew_end):
        if not rows[r]:
            continue

        pos = (rows[r][0] or "").strip()
        if not pos.isdigit():
            continue
        if int(pos) < 1 or int(pos) > 8:
            continue

        pad_row(rows[r], weight_c + 1)
        abbr = (rows[r][abbr_c] or "").strip().lower()

        # Prefer abbr-based, fall back to seat-based (pos_1..pos_8)
        w = weights_by_abbr.get(abbr)
        if w is None or str(w).strip() == "":
            w = weights_by_abbr.get(f"pos_{pos}")

        if w is not None and str(w).strip() != "":
            rows[r][weight_c] = str(w).strip()

    # Ensure weights not blank for seats 1–8
    for r in range(crew_start, crew_end):
        if not rows[r]:
            continue
        pos = (rows[r][0] or "").strip()
        if not pos.isdigit():
            continue
        if int(pos) < 1 or int(pos) > 8:
            continue
        pad_row(rows[r], weight_c + 1)
        if (rows[r][weight_c] or "").strip() == "":
            raise ValueError(f"Missing weight for seat {pos} in Crew Info. Fill seats 1–8.")

    # 3) Piece table (bounded to section, not #ERROR!)
    piece_header_r = find_piece_header_row(rows)
    if piece_header_r is None:
        raise ValueError('Could not find Piece header row containing Pace/comment/Wind/Stream/Validated.')

    table_start, table_end = find_table_bounds_from_header(rows, piece_header_r)

    # Ensure Zone between comment and Wind
    zone_c = _ensure_column_between(
        rows=rows,
        header_r=piece_header_r,
        table_start=table_start,
        table_end=table_end,
        left_col_name="comment",
        new_col_name="Zone",
        right_col_name="Wind",
    )

    # Ensure Temperature after Validated
    temp_c = _ensure_column_after(
        rows=rows,
        header_r=piece_header_r,
        table_end=table_end,
        anchor_col_name="Validated",
        new_col_name="Temperature",
    )

    # Rebuild col map after insertions
    cols = header_col_map(rows[piece_header_r])
    comment_c = cols["comment"]
    wind_c = cols["Wind"]
    stream_c = cols["Stream"]
    # validated_c = cols["Validated"]  # not needed for writing

    # Fill ONLY actual piece rows:
    # A "piece row" is one where Pace looks like a pace (contains ":")
    for r in range(table_start, table_end):
        if not rows[r]:
            continue
        # skip totally empty lines
        if not any((cell or "").strip() for cell in rows[r]):
            continue

        pad_row(rows[r], max(comment_c, zone_c, wind_c, stream_c, temp_c) + 1)

        pace_val = (rows[r][cols["Pace"]] or "").strip() if "Pace" in cols else ""

        # Only write metadata to real piece summary rows (pace-like)
        if not pace_val or not PACE_LIKE.match(pace_val):
            continue

        rows[r][comment_c] = rig_info
        rows[r][zone_c] = zone
        rows[r][wind_c] = wind
        rows[r][stream_c] = stream
        rows[r][temp_c] = temperature

    return rows