# app/utils.py
import re
import base64
import json
from typing import Optional, Tuple, Dict, Any
import pandas as pd
import io

number_re = re.compile(r"-?\d+(?:\.\d+)?")

def extract_numbers(text: str):
    return [float(m.group()) for m in number_re.finditer(text)]

def try_parse_json_from_text(text: str):
    # attempt to find a JSON snippet in text
    start = text.find("{")
    if start == -1:
        return None
    # crude attempt: try progressively larger slices
    for end in range(len(text), start, -1):
        candidate = text[start:end]
        try:
            obj = json.loads(candidate)
            return obj
        except Exception:
            continue
    return None

def decode_base64_block(text: str) -> Optional[str]:
    # find large base64-looking block
    m = re.search(r"([A-Za-z0-9+/=\s]{100,})", text)
    if not m:
        return None
    block = "".join(m.group(1).split())
    try:
        return base64.b64decode(block).decode("utf-8", errors="ignore")
    except Exception:
        return None

def sum_dataframe_column(df: pd.DataFrame, col_name: str):
    if col_name in df.columns:
        try:
            return float(df[col_name].astype(float).sum())
        except Exception:
            # try to coerce non-numeric
            s = pd.to_numeric(df[col_name].str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")
            return float(s.sum(skipna=True))
    # try fuzzy match
    lower = {c.lower(): c for c in df.columns}
    if col_name.lower() in lower:
        return sum_dataframe_column(df, lower[col_name.lower()])
    return None

def parse_table_html_to_df(html: str) -> Optional[pd.DataFrame]:
    try:
        dfs = pd.read_html(html)
        if dfs:
            return dfs[0]
    except Exception:
        return None
    return None

def parse_csv_bytes(content: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(content))

def parse_excel_bytes(content: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(content))
