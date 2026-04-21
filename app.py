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

# --- CONFIG & HELPERS ---
APP_TITLE = "TikTok Live Monitor"
COLUMN_MAPPING = {
    "username": "Nutzer", "messages": "Nachrichten", "trigger_ratio": "Trigger-Anteil",
    "toxic_ratio": "Toxik-Anteil", "question_ratio": "Frage-Anteil", "shift_score": "Einfluss-Score",
    "role": "Rolle", "repeat_ratio": "Wiederholungen", "archetype": "Archetyp", "why": "Grund",
    "attention_share": "Aufmerksamkeit", "substance_score": "Substanz", "attention_minus_substance": "Gap",
    "keyword": "Wort", "count": "Anzahl", "share": "Anteil", "toxic_rate": "Toxik-Rate", "avg_length": "Ø-Länge",
    "bucket": "Zeitfenster", "escalation_score": "Eskalations-Score", "signal": "Signal"
}

def display_table(df, **kwargs):
    """Zeigt ein DataFrame mit bereinigten Spaltennamen an."""
    if df.empty: return
    display_df = df.copy()
    display_df.rename(columns=COLUMN_MAPPING, inplace=True)
    st.dataframe(display_df, use_container_width=True, hide_index=True, **kwargs)

# --- REST DER LOGIK (Bleibt unverändert) ---
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

try:
    from TikTokLive.events import LikeEvent, GiftEvent, JoinEvent, ShareEvent
    OPTIONAL_LIVE_EVENTS = True
except Exception:
    LikeEvent = GiftEvent = JoinEvent = ShareEvent = None
    OPTIONAL_LIVE_EVENTS = False

SKLEARN_AVAILABLE = True
try:
    from sklearn.cluster import KMeans
    from sklearn.feature_extraction.text import TfidfVectorizer
except Exception:
    SKLEARN_AVAILABLE = False

# [Die restlichen Funktionsdefinitionen (now_dt, init_db, etc.) bleiben 1:1 gleich wie in deiner alten Datei...]
# (Ich habe sie hier aus Platzgründen nur angedeutet, in deiner Datei müssen sie vollständig stehen bleiben)
def now_dt() -> datetime: return datetime.now(TZ)
def now_ts() -> str: return now_dt().strftime("%Y-%m-%d %H:%M:%S")
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ... (HIER MÜSSEN DEINE FUNKTIONEN: init_db, create_board, get_board, update_board, insert_message, 
# load_messages, normalize_username, extract_words, extract_emojis, user_color, initials, 
# elapsed_label, safe_avatar_url, classify_message, clean_message_store, build_dataframe, 
# render_message_text, messages_to_txt, messages_to_csv_bytes, messages_to_json_bytes, 
# get_comment_messages, summarize_heuristics, top_words, top_emojis, top_users, activity_per_minute, 
# repeated_messages, user_scores, build_clusters, filtered_comment_df, calibrate_band, 
# score_label, score_color, score_arrow, impact_scores, narrative_candidates, role_summary, 
# salience_warning, metric_snapshot, explain_impact_scores, event_overview, recent_window_metrics, 
# compute_live_ampel, compute_alerts, narrative_drift, mention_edges, influencer_map, 
# greeting_edges, user_detail_snapshot, phase_of_live, critical_moments, fairness_metrics, 
# trigger_effect_analysis, user_archetypes, attention_vs_substance, generate_rule_based_report, 
# basic_alerts_for_ai, ai_enabled, get_google_api_key, build_ai_payload, build_ai_prompt, 
# call_google_ai, maybe_run_auto_ai, queue_message, start_client, init_state stehen bleiben!)

# --- UI START ---
init_db()
init_state()

st.set_page_config(page_title=APP_TITLE, page_icon="💬", layout="wide")

# CSS Styling
st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Dein CSS hier

st.sidebar.header("Navigation")
board_id = st.text_input("Board-ID", value=st.session_state.get("board_id", ""))
if st.sidebar.button("Board laden"):
    st.session_state.board_id = board_id.strip().lower()
    st.rerun()

if st.sidebar.button("Neues Board erstellen"):
    st.session_state.board_id = create_board()
    st.rerun()

# [Hier folgt der Rest der UI Logik mit Tabs]
# Initialisiere Daten
board = get_board(board_id) if board_id else None
messages = load_messages(board_id) if board_id else []
comment_df = build_dataframe(get_comment_messages(messages))

# Tabs definieren
tab1, tab2, tab3, tab4 = st.tabs(["📊 Live-Monitor", "👥 Community", "🔍 Diskurs-Analyse", "⚙️ Export & KI"])

with tab1:
    # Live-Feed und Ampel hier rein
    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.subheader("Live-Feed")
        # Dein Feed-Code hier
    with col_b:
        st.subheader("Lagebild")
        # Deine Ampel/Warnungen hier

with tab2:
    st.subheader("Wer ist im Chat aktiv?")
    # Influencer-Map und User-Tabellen hier
    display_table(influencer_df)
    display_table(archetype_df)

with tab3:
    st.subheader("Themen & Narrative")
    # Cluster, Narrative, Wortwolken hier

with tab4:
    st.subheader("Report & KI")
    # Report anzeigen und KI-Buttons hier
