import json
import math
import queue
import re
import sqlite3
import threading
import hashlib
import secrets
import os
import requests
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

# --- 1. KONFIGURATION & MAPPINGS ---
APP_TITLE = "TikTok Live Monitor"
COLUMN_MAPPING = {
    "username": "Nutzer", "messages": "Nachrichten", "trigger_ratio": "Trigger-Anteil",
    "toxic_ratio": "Toxik-Anteil", "question_ratio": "Frage-Anteil", "shift_score": "Einfluss-Score",
    "role": "Rolle", "repeat_ratio": "Wiederholungen", "archetype": "Archetyp", "why": "Grund"
}

# --- 2. HILFSFUNKTIONEN (Logik) ---
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

# ... (HIER MÜSSEN ALLE DEINE WEITEREN FUNKTIONEN STEHEN: create_board, get_board, insert_message, etc.)
# Da der Platz hier begrenzt ist, stelle sicher, dass alle 'def' Funktionen, die du vorher hattest, 
# hier vollständig eingefügt sind.

# --- 3. UI & HAUPTPROGRAMM (Wird erst ganz am Ende ausgeführt) ---

# Datenbank initialisieren, BEVOR die UI startet
init_db()
init_state()

st.set_page_config(page_title=APP_TITLE, page_icon="💬", layout="wide")

# Sidebar
with st.sidebar:
    st.header("Navigation")
    # ... (Dein Navigation-Code)

# Hauptbereich mit Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Live-Monitor", "👥 Community", "🔍 Diskurs-Analyse", "⚙️ Export & KI"])

with tab1:
    st.subheader("Live-Monitor")
    # ... (Dein Live-Feed Code)

with tab2:
    st.subheader("Community-Struktur")
    # Nutze hier display_table(influencer_df) statt st.dataframe
    
with tab3:
    st.subheader("Themen & Narrative")
    # ...

with tab4:
    st.subheader("Export & KI")
    # ...
