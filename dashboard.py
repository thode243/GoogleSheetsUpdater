import streamlit as st
import pandas as pd

# -----------------------------
# 1. Load Google Sheet CSV
# -----------------------------
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=1970058116&single=true&output=csv"
df = pd.read_csv(sheet_url)

# -----------------------------
# 2. Sidebar Filters
# -----------------------------
# Index selection (can be extended dynamically if available in sheet)
index_list = ['Nifty', 'Bank Nifty', 'Mid CAP Nifty', 'Fin CAP Nifty']
selected_index = st.sidebar.selectbox("Select Index", index_list)

# Dynamically detect expiry dates from column headers
expiry_dates = sorted({col.replace('Call OI ', '') for col in df.columns if 'Call OI' in col})
selected_expiry = st.sidebar.selectbox("Select Expiry", expiry_dates)

# Strike price filter
strike_prices = sorted(df['Strike Price'].unique())
selected_strikes = st.sidebar.multiselect("Select Strike Prices", strike_prices, default=strike_prices)

# -----------------------------
# 3. Filter Data
# -----------------------------
filtered_df = df[df['Strike Price'].isin(selected_strikes)]

# Column names based on selected expiry
call_col = f"Call OI {selected_expiry}"
put_col = f"Put OI {selected_expiry}"
diff_col = f"Diff OI {selected_expiry}"

# Optional Amount columns
call_amt_col = f"Call Amount {selected_expiry}"
put_amt_col = f"Put Amount {selected_expiry}"
has_amount = all(col in df.columns for col in [call_amt_col, put_amt_col])

# -----------------------------
# 4. Display Table
# -----------------------------
st.title(f"{selected_index} Option Chain - {selected_expiry}")
table_cols = ['Strike Price', call_col, put_col, diff_col]
if has_amount:
    table_cols += [call_amt_col, put_amt_col]

st.dataframe(filtered_df[table_cols])

# -----------------------------
# 5. Charts
# -----------------------------
st.subheader("Call OI vs Put OI")
st.line_chart(filtered_df.set_index('Strike Price')[[call_col, put_col]])

st.subheader("Diff OI vs Strike Price")
st.bar_chart(filtered_df.set_index('Strike Price')[[diff_col]])

if has_amount:
    st.subheader("Call & Put Amount")
    st.bar_chart(filtered_df.set_index('Strike Price')[[call_amt_col, put_amt_col]])
