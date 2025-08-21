import streamlit as st
import pandas as pd

# -----------------------------
# 1. Load Google Sheet CSV with multi-row header
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"

# Use first 2 rows as header
df = pd.read_csv(sheet_url, header=[0,1])

# Strip spaces from multi-index columns
df.columns = pd.MultiIndex.from_tuples([(str(i[0]).strip(), str(i[1]).strip()) for i in df.columns])

# -----------------------------
# 2. Sidebar Filters
# -----------------------------
# Detect available indices dynamically
indices = sorted(set([i[0] for i in df.columns if i[0].strip() != '']))
selected_index = st.sidebar.selectbox("Select Index", indices)

# Detect available expiry dates for the selected index
expiry_cols = [i for i in df.columns if i[0] == selected_index and i[1] != 'Strike Price']
expiries = sorted(list(set([i[0].split()[-1] for i in expiry_cols])))  # assuming expiry at the end of index name
selected_expiry = st.sidebar.selectbox("Select Expiry", expiries)

# Detect Strike Price column for selected index
strike_col = (selected_index, 'Strike Price')
strike_prices = df[strike_col].unique()
selected_strikes = st.sidebar.multiselect("Select Strike Prices", strike_prices, default=strike_prices)

# -----------------------------
# 3. Filter Data
# -----------------------------
filtered_df = df[df[strike_col].isin(selected_strikes)]

# Dynamically select columns based on expiry
def get_col(col_type):
    # Find column where expiry matches selected_expiry and type matches col_type
    for c in df.columns:
        if c[0].startswith(selected_index) and selected_expiry in c[0] and c[1] == col_type:
            return c
    return None

call_col = get_col('COI')        # Change to 'Call OI' if your CSV uses different naming
put_col = get_col('Diff OI')     # Adjust as needed
diff_col = get_col('Diff Amount')
call_amt_col = get_col('Margin')  # Optional
put_amt_col = get_col('Margin')   # Optional, adjust if separate

# -----------------------------
# 4. Display Table
# -----------------------------
table_cols = [strike_col, call_col, put_col, diff_col]
st.title(f"{selected_index} Option Chain - {selected_expiry}")
st.dataframe(filtered_df[table_cols])

# -----------------------------
# 5. Charts
# -----------------------------
st.subheader("Call vs Put")
st.line_chart(filtered_df.set_index(strike_col)[[call_col, put_col]])

st.subheader("Diff OI")
st.bar_chart(filtered_df.set_index(strike_col)[[diff_col]])

# Optional Amount/Margin chart
if call_amt_col and put_amt_col:
    st.subheader("Amount / Margin")
    st.bar_chart(filtered_df.set_index(strike_col)[[call_amt_col, put_amt_col]])
