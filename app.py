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

# --- KONFIGURATION ---
APP_TITLE = "TikTok Live Monitor"
TZ = ZoneInfo("Europe/Berlin")
DISPLAY_LIMIT = 2000
AUTO_REFRESH_MS = 2000
DATA_DIR = Path("shared_data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "board_store.sqlite3"
APP_BASE_URL = "https://ttlivechat.streamlit.app/"

AI_DEFAULT_MODEL = "gemini-2.0-flash"
AI_MIN_NEW_MESSAGES = 100
AI_CONTEXT_LIMIT = 220

# UI-Mapping für lesbare Tabellenspalten
COLUMN_MAPPING = {
    "username": "Nutzer", "messages": "Nachrichten", "trigger_ratio": "Trigger-Anteil",
    "toxic_ratio": "Toxik-Anteil", "question_ratio": "Frage-Anteil", "shift_score": "Einfluss-Score",
    "role": "Rolle", "repeat_ratio": "Wiederholungen", "archetype": "Archetyp", "why": "Grund",
    "attention_share": "Aufmerksamkeit", "substance_score": "Substanz", "attention_minus_substance": "Gap",
    "keyword": "Wort", "count": "Anzahl", "share": "Anteil", "toxic_rate": "Toxik-Rate", "avg_length": "Ø-Länge"
}

# --- HILFSFUNKTIONEN ---
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

# ... [Alle deine ursprünglichen Logik-Funktionen (create_board, classify_message, etc.) bleiben hier] ...
# [Ich habe sie hier für den Überblick gekürzt, kopiere deine originalen Funktionen exakt ab hier ein]
# ... 

# (Kopiere hier deine Logik-Funktionen hinein: create_board, get_board, update_board, insert_message, load_messages, normalize_username, extract_words, extract_emojis, user_color, initials, elapsed_label, safe_avatar_url, classify_message, clean_message_store, build_dataframe, render_message_text, messages_to_txt, messages_to_csv_bytes, messages_to_json_bytes, get_comment_messages, summarize_heuristics, top_words, top_emojis, top_users, activity_per_minute, repeated_messages, user_scores, build_clusters, filtered_comment_df, impact_scores, narrative_candidates, role_summary, salience_warning, metric_snapshot, explain_impact_scores, event_overview, recent_window_metrics, compute_live_ampel, compute_alerts, narrative_drift, mention_edges, influencer_map, greeting_edges, user_detail_snapshot, phase_of_live, critical_moments, fairness_metrics, trigger_effect_analysis, user_archetypes, attention_vs_substance, generate_rule_based_report, basic_alerts_for_ai, ai_enabled, get_google_api_key, build_ai_payload, build_ai_prompt, call_google_ai, maybe_run_auto_ai, queue_message, start_client, init_state)

# --- UI START ---
init_db()
init_state()

st.set_page_config(page_title=APP_TITLE, page_icon="💬", layout="wide")

# ... (Dein CSS Block bleibt gleich) ...

with st.sidebar:
    st.header("Einstellungen")
    # ... (Dein Sidebar Code)

# Daten laden
board_id = st.session_state.board_id
board = get_board(board_id) if board_id else None
messages = load_messages(board_id) if board_id else []
comment_df = build_dataframe(get_comment_messages(messages))

# TABS DEFINITION
tab1, tab2, tab3, tab4 = st.tabs(["📊 Live-Monitor", "👥 Community", "🔍 Analyse", "⚙️ Export & KI"])

with tab1:
    col1, col2 = st.columns([1.5, 1])
    with col1:
        st.subheader("Live-Chat Feed")
        # ... (Dein Feed Code)
    with col2:
        st.subheader("Aktuelle Lage")
        # ... (Deine Ampel und Alerts)

with tab2:
    st.subheader("Community-Struktur & Rollen")
    c_a, c_b = st.columns(2)
    with c_a:
        st.write("Influencer-Übersicht")
        display_table(influencer_map(comment_df))
    with c_b:
        st.write("User-Archetypen")
        display_table(user_archetypes(comment_df, user_scores(comment_df)))

with tab3:
    st.subheader("Diskurs-Analyse")
    # ... (Cluster, Narrative, etc.)

with tab4:
    st.subheader("KI & Export")
    # ... (Report Buttons und Download)
