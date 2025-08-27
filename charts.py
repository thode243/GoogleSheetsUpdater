import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ===== Compute derived metrics =====
def compute_metrics(df):
    df["CallDiff"] = df["Call VWAP"] - df["Call LTP"]
    df["PutDiff"] = df["Put VWAP"] - df["Put LTP"]
    df["OIDiff"] = df["CE OI"] - df["PE OI"]
    df["AmtDiff"] = df["Call Amount"] - df["Put Amount"]
    return df

# ===== Load from Google Sheet (latest data pushed by updater) =====
sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5Lvrxvflj_qRKt-eVIUlr3yltRJQgISwea-qRRDoI5tXMT3TFXiwy0pukbs6wjOfS1K_C9zNxtUra/pub?gid=42275942&single=true&output=csv"
df = pd.read_csv(sheet_url)
df = compute_metrics(df)

# ===== UI =====
strike = st.selectbox("Choose Strike Price", df["Strike Price"].unique())

metrics = {
    "Call OI": "CE OI",
    "Put OI": "PE OI",
    "Call LTP": "CE LTP",
    "Put LTP": "PE LTP",
    "Call VWAP-LTP diff": "CallDiff",
    "Put VWAP-LTP diff": "PutDiff",
    "Change OI (Call)": "CE Chng OI",
    "Change OI (Put)": "PE Chng OI",
    "OI Diff": "OIDiff",
    "Call Amount": "Call Amount",
    "Put Amount": "Put Amount",
    "Amount Diff": "AmtDiff"
}

left1 = st.selectbox("Left Y1 Metric", [""] + list(metrics.keys()), index=0)
left2 = st.selectbox("Left Y2 Metric", [""] + list(metrics.keys()), index=0)
right1 = st.selectbox("Right Y1 Metric", [""] + list(metrics.keys()), index=0)
right2 = st.selectbox("Right Y2 Metric", [""] + list(metrics.keys()), index=0)

d = df[df["Strike Price"] == strike]

# ===== Plotting =====
fig, ax1 = plt.subplots(figsize=(12, 6))
ax2 = ax1.twinx()

for metric, color in zip([left1, left2], ["red", "green"]):
    if metric:
        ax1.plot(d["Time"], d[metrics[metric]], label=metric, color=color)
        ax1.set_ylabel("Left Y-axis")

for metric, color in zip([right1, right2], ["blue", "orange"]):
    if metric:
        ax2.plot(d["Time"], d[metrics[metric]], label=metric, color=color)
        ax2.set_ylabel("Right Y-axis")

fig.legend(loc="upper left")
st.pyplot(fig)
