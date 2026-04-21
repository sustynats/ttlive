import json, math, queue, re, sqlite3, threading, hashlib, secrets, os, requests
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import altair as alt
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent

# Konfiguration
APP_TITLE = "TikTok Live Monitor"
TZ = ZoneInfo("Europe/Berlin")
DATA_DIR = Path("shared_data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "board_store.sqlite3"
APP_BASE_URL = "https://ttlivechat.streamlit.app/"

# Mapping für UI-Optimierung
COLUMN_MAPPING = {
    "username": "Nutzer", "messages": "Nachrichten", "trigger_ratio": "Trigger-Anteil",
    "toxic_ratio": "Toxik-Anteil", "question_ratio": "Frage-Anteil", "shift_score": "Einfluss-Score",
    "role": "Rolle", "repeat_ratio": "Wiederholungen", "archetype": "Archetyp", "why": "Grund"
}

def display_table(df, **kwargs):
    if df.empty: return
    display_df = df.copy()
    display_df.rename(columns=COLUMN_MAPPING, inplace=True)
    st.dataframe(display_df, use_container_width=True, hide_index=True, **kwargs)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS boards (board_id TEXT PRIMARY KEY, host_username TEXT, created_at TEXT NOT NULL, started_at TEXT, status TEXT NOT NULL DEFAULT 'idle', report_text TEXT DEFAULT '')")
    cur.execute("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, board_id TEXT NOT NULL, timestamp TEXT NOT NULL, type TEXT NOT NULL, username TEXT NOT NULL, text TEXT NOT NULL, avatar_url TEXT, FOREIGN KEY (board_id) REFERENCES boards(board_id))")
    conn.commit()
    conn.close()
    def create_board() -> str:
    board_id = secrets.token_urlsafe(5).replace("-", "").replace("_", "")[:8].lower()
    conn = get_conn()
    conn.execute("INSERT INTO boards(board_id, created_at, status, report_text) VALUES (?, ?, 'idle', '')", (board_id, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()
    return board_id

def get_board(board_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM boards WHERE board_id = ?", (board_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def update_board(board_id: str, **kwargs):
    cols = ", ".join([f"{k} = ?" for k in kwargs.keys()])
    vals = list(kwargs.values()) + [board_id]
    conn = get_conn()
    conn.execute(f"UPDATE boards SET {cols} WHERE board_id = ?", vals)
    conn.commit()
    conn.close()

def insert_message(board_id: str, payload: dict):
    conn = get_conn()
    conn.execute("INSERT INTO messages(board_id, timestamp, type, username, text, avatar_url) VALUES (?, ?, ?, ?, ?, ?)",
                 (board_id, payload["timestamp"], payload["type"], payload["username"], payload["text"], payload.get("avatar_url")))
    conn.commit()
    conn.close()

def load_messages(board_id: str):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM messages WHERE board_id = ? ORDER BY id ASC", (board_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def build_dataframe(messages) -> pd.DataFrame:
    # Hier fügst du deine Funktion 'build_dataframe' aus dem Original ein
    pass # (Ersetze 'pass' mit deinem originalen Code)
# Füge hier alle deine Analyse-Funktionen ein:
# user_scores, build_clusters, impact_scores, influencer_map, user_archetypes, 
# attention_vs_substance, generate_rule_based_report, start_client, queue_message

def init_state():
    defaults = {
        "chat_queue": queue.Queue(), "listener_thread": None, "board_id": None,
        "ai_enabled": False, "ai_mode": "Manuell"
    }
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value
            # --- UI START ---
init_db()
init_state()
st.set_page_config(page_title=APP_TITLE, layout="wide")

# (Dein CSS Block hier einfügen)

board_id = st.session_state.board_id
# ... (Hier Sidebar und Daten laden)

tab1, tab2, tab3, tab4 = st.tabs(["📊 Live-Monitor", "👥 Community", "🔍 Analyse", "⚙️ Export & KI"])

with tab1:
    st.subheader("Live-Monitor")
    # Feed Code

with tab2:
    st.subheader("Community & Rollen")
    c1, c2 = st.columns(2)
    with c1: display_table(influencer_map(comment_df))
    with c2: display_table(user_archetypes(comment_df, user_scores(comment_df)))

with tab3:
    st.subheader("Diskurs & Themen")
    # ...

with tab4:
    st.subheader("KI & Export")
    # ...
