#!/usr/bin/env python3
"""
Split a structured pipe-delimited CSV file into multiple smaller files.

- Reads from ./csvinput/INPUT_FILENAME
- Writes to ./csvoutput/{OUTPUT_PREFIX}{n}.csv

Structure:
  JOB|...
  META_* lines
  Repeating data-items, each:
      CLNT_CORR|...                       <-- 1 per item
      (CLNT_ROLE|... | CLNT_ROL|... | PLAN|...)*  <-- multiple, any order; must include ≥1 ROLE and ≥1 PLAN
  FOOTER|<job_code>|<count>
"""

from pathlib import Path
from typing import List, Tuple

# ===== CONFIGURABLE =====
INPUT_FILENAME = "NTF_AER_SAVING_2024.csv"   # file inside ./csvinput/
ITEMS_PER_FILE = 45000                  # data-items per output file (e.g., 1 or 10)
OUTPUT_PREFIX  = "NTF_AER_SAVING_2024_45ksplit"      # output file prefix
# ========================

INPUT_DIR  = Path("csvinput")
OUTPUT_DIR = Path("csvoutput")


def read_lines(path: Path) -> List[str]:
    """
    Read text while preserving field padding and trying common Windows encodings.
    Strips only newline characters; keeps all other spaces/padding intact.
    """
    tried = []
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                # strip only \r and \n; keep spaces in fields
                return [line.rstrip("\r\n") for line in f]
        except UnicodeDecodeError as e:
            tried.append(f"{enc}: {e}")

    # Last-resort fallback: read bytes and decode with latin-1 replacing bad bytes
    with path.open("rb") as f:
        raw = f.read()
    text = raw.decode("latin-1", errors="replace")
    # You can log/troubleshoot with the encodings we tried:
    print("Warning: fell back to latin-1 with replacement. Tried:\n  " + "\n  ".join(tried))
    return text.splitlines()

def parse_header_and_meta(lines: List[str]) -> Tuple[str, List[str], int]:
    if not lines:
        raise ValueError("Input file is empty.")
    job_line = lines[0]
    if not job_line.startswith("JOB|"):
        raise ValueError("First line must start with 'JOB|'.")
    meta_lines, i = [], 1
    while i < len(lines) and lines[i].startswith("META_"):
        meta_lines.append(lines[i]); i += 1
    if not meta_lines:
        raise ValueError("Expected one or more META_* lines after JOB.")
    return job_line, meta_lines, i

def parse_groups_and_footer(lines: List[str], start_idx: int):
    """
    Parse item as:
      CLNT_CORR, then one or more of (CLNT_ROLE/CLNT_ROL | PLAN) in any order,
      until the next CLNT_CORR or FOOTER.
    Returns (groups, footer_line) where each group is a list of lines preserving original order.
    """
    groups: List[List[str]] = []
    i, n = start_idx, len(lines)

    def at_footer(pos: int) -> bool:
        return pos < n and lines[pos].startswith("FOOTER|")

    while i < n and not at_footer(i):
        if not lines[i].startswith("CLNT_CORR|"):
            raise ValueError(f"Expected 'CLNT_CORR|' at line {i+1}, found: {lines[i]}")
        item_lines = [lines[i]]; i += 1
        role_count = 0; plan_count = 0

        while i < n and not at_footer(i) and not lines[i].startswith("CLNT_CORR|"):
            if lines[i].startswith("CLNT_ROLE|"):
                role_count += 1; item_lines.append(lines[i])
            elif lines[i].startswith("PLAN|"):
                plan_count += 1; item_lines.append(lines[i])
            else:
                raise ValueError(
                    f"Unexpected line at {i+1}: {lines[i]} "
                    "(expected CLNT_ROLE/CLNT_ROL or PLAN, or start of next CLNT_CORR/FOOTER)."
                )
            i += 1

        if role_count == 0 or plan_count == 0:
            raise ValueError(
                f"Incomplete data-item starting with '{item_lines[0]}' "
                f"(roles={role_count}, plans={plan_count})."
            )
        groups.append(item_lines)

    if i >= n:
        raise ValueError("Missing FOOTER line.")
    footer_line = lines[i]
    if not footer_line.startswith("FOOTER|"):
        raise ValueError("Last block must be a FOOTER line.")
    return groups, footer_line

def extract_job_code(job_line: str) -> str:
    parts = job_line.split("|")
    if len(parts) < 2:
        raise ValueError("JOB line malformed (need JOB|<job_code>|...).")
    return parts[1]

def validate_footer(footer_line: str, expected_job_code: str, expected_total: int) -> None:
    parts = footer_line.split("|")
    if len(parts) != 3:
        raise ValueError("FOOTER must be 'FOOTER|<job_code>|<count>'.")
    _, job_code, count_str = parts
    if job_code != expected_job_code:
        raise ValueError(f"FOOTER job_code '{job_code}' ≠ JOB '{expected_job_code}'.")
    try:
        orig_count = int(count_str)
    except ValueError:
        raise ValueError("FOOTER count is not an integer.")
    if orig_count != expected_total:
        print(f"Warning: FOOTER count {orig_count} != actual items {expected_total}. Using actual counts for outputs.")

def write_batches(job_line: str, meta_lines: List[str], groups: List[List[str]],
                  items_per_file: int, job_code: str):
    if items_per_file <= 0:
        raise ValueError("ITEMS_PER_FILE must be a positive integer.")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total = len(groups)
    file_num = 1
    for start in range(0, total, items_per_file):
        chunk = groups[start:start + items_per_file]
        count = len(chunk)
        footer = f"FOOTER|{job_code}|{count}"
        out_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}{file_num}.csv"
        with out_path.open("w", encoding="utf-8", newline="\n") as f:
            f.write(job_line + "\n")
            for m in meta_lines:
                f.write(m + "\n")
            for item in chunk:
                for line in item:
                    f.write(line + "\n")  # preserve original order
            f.write(footer + "\n")
        print(f"Written: {out_path}")
        file_num += 1

def main():
    input_path = INPUT_DIR / INPUT_FILENAME
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    lines = read_lines(input_path)
    job_line, meta_lines, idx = parse_header_and_meta(lines)
    groups, footer_line = parse_groups_and_footer(lines, idx)
    job_code = extract_job_code(job_line)
    validate_footer(footer_line, job_code, expected_total=len(groups))
    print(f"Total data-items found: {len(groups)}")
    write_batches(job_line, meta_lines, groups, ITEMS_PER_FILE, job_code)

if __name__ == "__main__":
    main()
