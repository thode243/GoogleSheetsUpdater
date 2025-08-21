# ---------------------------
import streamlit as st
import pandas as pd

# -----------------------------
# 1) Load Google Sheet CSV with multi-row header
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"

# Read first 2 rows as header (multi-index)
df = pd.read_csv(sheet_url, header=[0, 1])

# Normalize multi-index column labels (strip + ensure str)
norm_cols = [(str(c[0]).strip(), str(c[1]).strip()) for c in df.columns]
df.columns = pd.MultiIndex.from_tuples(norm_cols)

# -----------------------------
# 2) Sidebar Filters (defensive)
# -----------------------------
# Find 'Strike Price' columns
strike_cols = [
    c for c in df.columns
    if isinstance(c[1], str) and c[1].strip().lower() == 'strike price'
]

# Derive indices from the first token of the level-0 header (e.g., "Nifty 21-Aug-2025" -> "Nifty")
indices = sorted({
    c[0].split()[0] for c in strike_cols
    if isinstance(c[0], str) and c[0].strip()
})

if not indices:
    st.error("No indices found (no 'Strike Price' headers). Check the sheet header names.")
    st.stop()

selected_index = st.sidebar.selectbox("Select Index", indices, index=0)

# Find available expiries for selected index (exclude 'Strike Price')
expiry_level0_labels = [
    c[0] for c in df.columns
    if isinstance(c[0], str)
    and c[0].startswith(selected_index + ' ')
    and isinstance(c[1], str)
    and c[1].strip().lower() != 'strike price'
]

# Extract the part after the first space: "Nifty 21-Aug-2025" -> "21-Aug-2025"
expiries = sorted({label.split(' ', 1)[1] for label in expiry_level0_labels if ' ' in label})

if not expiries:
    st.error(f"No expiries found for index '{selected_index}'.")
    st.stop()

selected_expiry = st.sidebar.selectbox("Select Expiry", expiries, index=0)

# Find the 'Strike Price' column for selected index
strike_col = next(
    (
        c for c in df.columns
        if isinstance(c[0], str)
        and isinstance(c[1], str)
        and c[1].strip().lower() == 'strike price'
        and c[0].startswith(selected_index)
    ),
    None
)

if strike_col is None:
    st.error(f"No 'Strike Price' column found for index '{selected_index}'.")
    st.stop()

# -----------------------------
# 3) Strike selection
# -----------------------------
strike_prices = df[strike_col].dropna().unique().tolist()
selected_strikes = st.sidebar.multiselect("Select Strike Prices", strike_prices, default=strike_prices)

# Filtered data
filtered_df = df[df[strike_col].isin(selected_strikes)]

# -----------------------------
# 4) Helpers for other columns
# -----------------------------
def find_col(possible_labels):
    """
    Look up a column for the selected index+expiry by matching the level-1 header
    against any of the provided labels (case-insensitive).
    """
    targets = [lbl.strip().lower() for lbl in (possible_labels if isinstance(possible_labels, (list, tuple)) else [possible_labels])]
    for c in df.columns:
        if not (isinstance(c[0], str) and isinstance(c[1], str)):
            continue
        if c[0].startswith(selected_index) and selected_expiry in c[0] and c[1].strip().lower() in targets:
            return c
    return None

# Try to be flexible with common aliases
call_col   = find_col(['coi', 'call oi', 'call open interest', 'call_oi'])
put_col    = find_col(['poi', 'put oi', 'put open interest', 'put_oi', 'diff oi'])  # include 'diff oi' if your sheet uses that
diff_col   = find_col(['diff amount', 'diff', 'net diff', 'diff_amt'])
amount_col = find_col(['margin', 'amount', 'amt'])

# -----------------------------
# 5) Display Table
# -----------------------------
st.title(f"{selected_index} Option Chain - {selected_expiry}")

table_cols = [col for col in [strike_col, call_col, put_col, diff_col, amount_col] if col is not None]
if table_cols:
    st.dataframe(filtered_df[table_cols])
else:
    st.info("No matching data columns found to display.")

# -----------------------------
# 6) Charts
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
