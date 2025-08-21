import streamlit as st
import pandas as pd

# -----------------------------
# 1. Load Google Sheet CSV with multi-row header
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"

# Robust header parsing: read raw then locate header rows dynamically
# Ignore the first two rows as requested
raw = pd.read_csv(sheet_url, header=None, skiprows=2)

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

# Build level-0 by choosing first non-empty among top1_ff then top2_ff
level0 = []
for a, b in zip(top1_ff, top2_ff):
    val = _norm_cell(a) or _norm_cell(b) or "Unknown"
    level0.append(val)

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
        str(c[0]).split()[0] for c in strike_cols
        if isinstance(c[0], str) and str(c[0]).strip()
    })
else:
    # Fallback: infer candidates from any level-0 label by taking the first token
    indices = sorted({
        str(c[0]).split()[0] for c in df.columns
        if isinstance(c[0], str)
        and str(c[0]).strip()
        and not normalize_label(str(c[0])).startswith("unnamed")
        and str(c[0]).split()[0].isalpha()
    })

if not indices:
    st.error("No indices could be inferred from the sheet headers.")
    st.stop()

selected_index = st.sidebar.selectbox("Select Index", indices, index=0)

# Detect available expiries for selected index (exclude the 'Strike Price' columns)
expiry_level0_labels = [
    c[0] for c in df.columns
    if isinstance(c[0], str)
    and c[0].startswith(selected_index + " ")
    and isinstance(c[1], str)
    and normalize_label(c[1]) not in strike_aliases
]

# Extract the part after the first space: "Nifty 21-Aug-2025" -> "21-Aug-2025"
expiries = sorted({label.split(" ", 1)[1] for label in expiry_level0_labels if " " in label})

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
