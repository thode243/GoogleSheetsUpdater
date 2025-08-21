import streamlit as st
import pandas as pd

# -----------------------------
# 1. Load Google Sheet CSV with multi-row header
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"

# Read first 2 rows as header
df = pd.read_csv(sheet_url, header=[0,1])

# Strip spaces from multi-index columns
df.columns = pd.MultiIndex.from_tuples([(str(i[0]).strip(), str(i[1]).strip()) for i in df.columns])

# -----------------------------
# 2. Sidebar Filters
# -----------------------------
# Detect available indices
indices = sorted(set([c[0].split()[0] for c in df.columns if c[1] == 'Strike Price']))
selected_index = st.sidebar.selectbox("Select Index", indices)

# Detect available expiries for selected index
expiry_cols = [c[0] for c in df.columns if c[0].startswith(selected_index) and c[1] != 'Strike Price']
expiries = sorted(list(set([c.split()[1] for c in expiry_cols])))  # Extract date from "Nifty 21-Aug-2025"
selected_expiry = st.sidebar.selectbox("Select Expiry", expiries)

# Detect the Strike Price column for selected index
strike_col = None
for c in df.columns:
    if c[1] == 'Strike Price' and c[0].startswith(selected_index):
        strike_col = c
        break

if strike_col is None:
    st.error("No Strike Price column found for selected index")
    st.stop()

# Filter strike prices
strike_prices = df[strike_col].unique()
selected_strikes = st.sidebar.multiselect("Select Strike Prices", strike_prices, default=strike_prices)

# -----------------------------
# 3. Filter Data
# -----------------------------
filtered_df = df[df[strike_col].isin(selected_strikes)]

# Helper to find column by type and expiry
def find_col(col_type):
    for c in df.columns:
        if c[0].startswith(selected_index) and selected_expiry in c[0] and c[1] == col_type:
            return c
    return None

call_col = find_col('COI')           # Adjust if Call OI naming is different
put_col = find_col('Diff OI')        # Adjust if Put OI naming is different
diff_col = find_col('Diff Amount')
amount_col = find_col('Margin')

# -----------------------------
# 4. Display Table
# -----------------------------
table_cols = [strike_col, call_col, put_col, diff_col]
if amount_col:
    table_cols.append(amount_col)

st.title(f"{selected_index} Option Chain - {selected_expiry}")
st.dataframe(filtered_df[table_cols])

# -----------------------------
# 5. Charts
# -----------------------------
st.subheader("Call vs Put")
st.line_chart(filtered_df.set_index(strike_col)[[call_col, put_col]])

st.subheader("Diff OI")
st.bar_chart(filtered_df.set_index(strike_col)[[diff_col]])

if amount_col:
    st.subheader("Amount / Margin")
    st.bar_chart(filtered_df.set_index(strike_col)[[amount_col]])
