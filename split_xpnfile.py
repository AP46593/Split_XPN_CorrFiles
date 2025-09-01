#!/usr/bin/env python3
"""
Split a structured pipe-delimited CSV file into multiple smaller files.

- Input file is picked from ./csvinput/NTF_SAMPLE.csv (configurable).
- Output files are written to ./csvoutput/output_file1.csv, output_file2.csv, ...

Structure expected:
JOB|...
META_* lines
[CLNT_CORR, CLNT_ROLE/CLNT_ROL, PLAN] repeated for each data-item
FOOTER|<job_code>|<count>
"""

from pathlib import Path
from typing import List, Tuple
from pathlib import Path
import sys, configparser

# Detect app directory whether running as .py or frozen .exe
if getattr(sys, "frozen", False):
    APP_DIR = Path(sys.executable).parent
else:
    APP_DIR = Path(__file__).resolve().parent

INPUT_DIR = APP_DIR / "csvinput"
OUTPUT_DIR = APP_DIR / "csvoutput"
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ===== CONFIGURABLE VARIABLES =====
INPUT_FILENAME = "NTF_SAMPLE.csv"   # Name of file to pick from ./csvinput/
ITEMS_PER_FILE = 2                  # How many data-items per output file
OUTPUT_PREFIX = "output_file"       # Prefix for output files
# ==================================

cfg = configparser.ConfigParser()
cfg_path = APP_DIR / "config.ini"
if cfg_path.exists():
    cfg.read(cfg_path)
    INPUT_FILENAME = cfg.get("app", "input_filename", fallback=INPUT_FILENAME)
    ITEMS_PER_FILE = cfg.getint("app", "items_per_file", fallback=ITEMS_PER_FILE)
    OUTPUT_PREFIX = cfg.get("app", "output_prefix", fallback=OUTPUT_PREFIX)


def read_lines(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as f:
        return [line.rstrip("\n") for line in f]

def parse_header_and_meta(lines: List[str]) -> Tuple[str, List[str], int]:
    job_line = lines[0]
    if not job_line.startswith("JOB|"):
        raise ValueError("First line must start with 'JOB|'")

    meta_lines = []
    idx = 1
    while idx < len(lines) and lines[idx].startswith("META_"):
        meta_lines.append(lines[idx])
        idx += 1

    return job_line, meta_lines, idx

def parse_groups_and_footer(lines: List[str], start_idx: int):
    groups = []
    i = start_idx
    n = len(lines)

    def at_footer(pos): return pos < n and lines[pos].startswith("FOOTER|")

    while i < n and not at_footer(i):
        if not lines[i].startswith("CLNT_CORR|"):
            raise ValueError(f"Expected CLNT_CORR at line {i+1}")
        corr = lines[i]; i += 1

        if not lines[i].startswith("CLNT_ROLE|"):
            raise ValueError(f"Expected CLNT_ROLE/CLNT_ROL at line {i+1}")
        role = lines[i]; i += 1

        if not lines[i].startswith("PLAN|"):
            raise ValueError(f"Expected PLAN at line {i+1}")
        plan = lines[i]; i += 1

        groups.append([corr, role, plan])

    if i >= n:
        raise ValueError("Missing FOOTER line")
    footer_line = lines[i]
    return groups, footer_line

def extract_job_code(job_line: str) -> str:
    return job_line.split("|")[1]

def write_batches(job_line, meta_lines, groups, items_per_file, job_code):
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
            for m in meta_lines: f.write(m + "\n")
            for g in chunk:
                for line in g:
                    f.write(line + "\n")
            f.write(footer + "\n")
        print(f"Written: {out_path}")
        file_num += 1

def main():
    input_path = INPUT_DIR / INPUT_FILENAME
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    lines = read_lines(input_path)
    job_line, meta_lines, idx = parse_header_and_meta(lines)
    groups, footer = parse_groups_and_footer(lines, idx)
    job_code = extract_job_code(job_line)

    print(f"Total data-items found: {len(groups)}")
    write_batches(job_line, meta_lines, groups, ITEMS_PER_FILE, job_code)

if __name__ == "__main__":
    main()
