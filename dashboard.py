import streamlit as st
import pandas as pd
import io
import requests

# -----------------------------
# 1. Load Google Sheet CSV with multi-row header
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"

# Robust header parsing: download CSV reliably, then locate header rows dynamically
# Ignore the first two rows as requested

@st.cache_data(ttl=300)
def download_csv_text(url: str) -> str:
    user_agent = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    response = requests.get(url, headers={"User-Agent": user_agent}, allow_redirects=True, timeout=30)
    response.raise_for_status()
    return response.text

try:
    csv_text = download_csv_text(sheet_url)
    raw = pd.read_csv(io.StringIO(csv_text), header=None, skiprows=2)
except Exception:
    st.error("Failed to fetch the Google Sheet CSV. Please check the link/share settings and try again.")
    st.stop()

def _norm_cell(x):
    return str(x).strip()

def _norm_lower(x):
    return _norm_cell(x).lower()

# Find the row containing 'Strike Price' (case-insensitive)
r_sp_candidates = [
    idx for idx in range(len(raw))
    if any(_norm_lower(v) == "strike price" for v in raw.iloc[idx].tolist())
]

if not r_sp_candidates:
    st.error("Could not find a 'Strike Price' header row in the sheet.")
    st.stop()

r_sp = r_sp_candidates[0]
r_top1 = max(0, r_sp - 2)
r_top2 = max(0, r_sp - 1)

top1 = raw.iloc[r_top1].fillna("").astype(str).tolist()
top2 = raw.iloc[r_top2].fillna("").astype(str).tolist()
lvl1 = raw.iloc[r_sp].fillna("").astype(str).tolist()

# Forward-fill horizontally for top rows
def forward_fill_row(values):
    filled = []
    last = ""
    for v in values:
        v = _norm_cell(v)
        if v:
            last = v
        filled.append(last)
    return filled

top1_ff = forward_fill_row(top1)
top2_ff = forward_fill_row(top2)

def is_dateish(text: str) -> bool:
    t = _norm_cell(text)
    return any(ch.isdigit() for ch in t) and ("-" in t or "/" in t)

def is_overall(text: str) -> bool:
    return _norm_lower(text) == "overall"

def derive_index_and_expiry(a: str, b: str) -> tuple[str, str]:
    """Heuristic: from two header rows, pick an index-like label and an expiry-like label."""
    a = _norm_cell(a)
    b = _norm_cell(b)
    index_name = ""
    expiry_name = ""
    # Prefer dateish/overall as expiry
    if is_dateish(a) or is_overall(a):
        expiry_name = a
    elif a:
        index_name = a
    if is_dateish(b) or is_overall(b):
        if not expiry_name:
            expiry_name = b
    elif b and not index_name:
        index_name = b
    if not index_name and ("nifty" in _norm_lower(a) or "nifty" in _norm_lower(b)):
        index_name = a if "nifty" in _norm_lower(a) else b
    return (index_name or "Unknown"), expiry_name

# Build level-0 as "<Index> <Expiry>" (expiry optional)
level0 = []
for a, b in zip(top1_ff, top2_ff):
    idx_name, exp_name = derive_index_and_expiry(a, b)
    combined = f"{idx_name} {exp_name}".strip()
    level0.append(combined)

# Level-1 from the 'Strike Price/COI/...' row
level1 = [ _norm_cell(x) for x in lvl1 ]

# Ensure same length
max_len = max(len(level0), len(level1))
if len(level0) < max_len:
    level0 += ["Unknown"] * (max_len - len(level0))
if len(level1) < max_len:
    level1 += [""] * (max_len - len(level1))

multi_cols = pd.MultiIndex.from_tuples(list(zip(level0[:max_len], level1[:max_len])))

# Data starts after 'Strike Price' row
data_start = r_sp + 1
df = raw.iloc[data_start:].reset_index(drop=True)
df.columns = multi_cols

def normalize_label(label: str) -> str:
    return str(label).strip().lower()

strike_aliases = {"strike price", "strike", "strikeprice"}

def split_index_expiry(label: str) -> tuple[str, str]:
    """Split a combined label into (index, expiry) using the last token if it looks like a date or 'Overall'."""
    text = str(label)
    if " " not in text:
        return text, ""
    left, right = text.rsplit(" ", 1)
    if is_dateish(right) or is_overall(right):
        return left, right
    return text, ""

# -----------------------------
# 2. Sidebar Filters (defensive)
# -----------------------------
# Find 'Strike Price' columns using aliases (case-insensitive)
strike_cols = [
    c for c in df.columns
    if isinstance(c[1], str) and normalize_label(c[1]) in strike_aliases
]

# Derive indices preferably from strike columns; otherwise from all level-0 labels
if strike_cols:
    indices = sorted({
        split_index_expiry(c[0])[0]
        for c in strike_cols
        if isinstance(c[0], str) and str(c[0]).strip()
    })
else:
    # Fallback: infer candidates from any level-0 label by taking the first token
    indices = sorted({
        split_index_expiry(c[0])[0]
        for c in df.columns
        if isinstance(c[0], str) and str(c[0]).strip() and not normalize_label(str(c[0])).startswith("unnamed")
    })

if not indices:
    st.error("No indices could be inferred from the sheet headers.")
    st.stop()

selected_index = st.sidebar.selectbox("Select Index", indices, index=0)

# Detect available expiries for selected index (exclude the 'Strike Price' columns)
expiries = sorted({
    split_index_expiry(c[0])[1]
    for c in df.columns
    if isinstance(c[0], str)
    and isinstance(c[1], str)
    and normalize_label(c[1]) not in strike_aliases
    and split_index_expiry(c[0])[0] == selected_index
    and split_index_expiry(c[0])[1]
})

if not expiries:
    st.error(f"No expiries found for index '{selected_index}'.")
    st.stop()

selected_expiry = st.sidebar.selectbox("Select Expiry", expiries, index=0)

# Detect the Strike Price column for selected index using aliases
strike_col = next(
    (
        c for c in df.columns
        if isinstance(c[0], str)
        and isinstance(c[1], str)
        and normalize_label(c[1]) in strike_aliases
        and c[0].startswith(selected_index)
    ),
    None,
)

if strike_col is None:
    st.error(f"No 'Strike Price' column found for index '{selected_index}'.")
    st.stop()

# -----------------------------
# 3. Strike selection
# -----------------------------
strike_prices = pd.Series(df[strike_col]).dropna().unique().tolist()
selected_strikes = st.sidebar.multiselect("Select Strike Prices", strike_prices, default=strike_prices)

# Filtered data
filtered_df = df[df[strike_col].isin(selected_strikes)]

# -----------------------------
# 4. Helpers for other columns
# -----------------------------
def find_col(possible_labels):
    """
    Look up a column for the selected index+expiry by matching the level-1 header
    against any of the provided labels (case-insensitive).
    """
    targets = [normalize_label(lbl) for lbl in (possible_labels if isinstance(possible_labels, (list, tuple)) else [possible_labels])]
    for c in df.columns:
        if not (isinstance(c[0], str) and isinstance(c[1], str)):
            continue
        if c[0].startswith(selected_index) and selected_expiry in c[0] and normalize_label(c[1]) in targets:
            return c
    return None

# Try to be flexible with common aliases
call_col   = find_col(["coi", "call oi", "call open interest", "call_oi"]) 
put_col    = find_col(["poi", "put oi", "put open interest", "put_oi", "diff oi"])  
diff_col   = find_col(["diff amount", "diff", "net diff", "diff_amt"]) 
amount_col = find_col(["margin", "amount", "amt"]) 

# -----------------------------
# 5. Display Table
# -----------------------------
st.title(f"{selected_index} Option Chain - {selected_expiry}")

table_cols = [col for col in [strike_col, call_col, put_col, diff_col, amount_col] if col is not None]
if table_cols:
    st.dataframe(filtered_df[table_cols])
else:
    st.info("No matching data columns found to display.")

# -----------------------------
# 6. Charts
# -----------------------------
if not filtered_df.empty:
    base = filtered_df.set_index(strike_col)

    series_for_call_put = [c for c in [call_col, put_col] if c is not None and c in base.columns]
    if series_for_call_put:
        st.subheader("Call vs Put")
        st.line_chart(base[series_for_call_put])

    if diff_col is not None and diff_col in base.columns:
        st.subheader("Diff OI")
        st.bar_chart(base[[diff_col]])

    if amount_col is not None and amount_col in base.columns:
        st.subheader("Amount / Margin")
        st.bar_chart(base[[amount_col]])
else:
    st.info("No rows match the selected strike prices.")
