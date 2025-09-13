# proxy_api.py
from flask import Flask, jsonify
import requests

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/"
}

@app.route("/nifty", methods=["GET"])
def get_option_chain():
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    session = requests.Session()
    res = session.get(url, headers=HEADERS, timeout=30)
    return jsonify(res.json())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
