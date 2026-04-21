
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


APP_TITLE = "TikTok Live Impact Monitor V2"
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

SCORE_TOOLTIPS = {
    "Diskurskultur": "Misst, wie respektvoll und konstruktiv die Kommunikation verläuft. -3 = stark toxisch, dominant oder abwertend. +3 = respektvoll, vielfältig und dialogorientiert.",
    "Salienz-Bewusstsein": "Zeigt, ob Aufmerksamkeit verzerrt wird. -3 = wenige laute Stimmen oder Trigger dominieren. +3 = ausgewogene Aufmerksamkeit und Themenvielfalt.",
    "Verantwortung & Macht": "Misst, wie verantwortungsvoll Einfluss und Bühne wirken. -3 = Eskalation, Manipulation oder problematische Verstärkung. +3 = verantwortungsvolle Kommunikation und klare Begrenzung destruktiver Dynamiken.",
    "Systemischer Impact": "Beschreibt die Wirkung auf den Gesamtdiskurs. -3 = Spaltung, Eskalation oder Verzerrung. +3 = Stabilisierung, Einordnung und gesellschaftlich konstruktive Wirkung.",
    "Emotionale Resonanz": "Erfasst die Qualität der emotionalen Dynamik. -3 = Hass, Aggression oder Unsicherheit. +3 = Verbindung, Sicherheit, Empathie und konstruktive Resonanz.",
}

GLOBAL_TOOLTIPS = {
    "shift_score": "Kombinierter Auffälligkeitswert pro User. Berücksichtigt Aktivität, Triggerbegriffe, Wiederholungen, Frage-Druck, Capslock und abwertende Marker. Hohe Werte bedeuten überproportionalen Einfluss auf den Diskurs - nicht automatisch Manipulation.",
    "rollen": "Heuristische Einordnung auf Basis von Kommunikationsmustern. Dient zur Orientierung, nicht zur Bewertung von Personen.",
    "trigger": "Begriffe oder Formulierungen, die typischerweise emotionale Reaktionen, Polarisierung oder Aufmerksamkeitsverschiebung auslösen.",
    "toxisch": "Erkannt über sprachliche Marker wie Beleidigungen oder aggressive Formulierungen. Kontextabhängig und deshalb nicht fehlerfrei.",
    "wiederholungen": "Identische oder sehr ähnliche Nachrichten eines Users. Kann auf hohe Aktivität, Agenda-Setting oder Spam hindeuten.",
    "cluster": "Automatisch erkannte Themenmuster auf Basis gemeinsam auftretender Begriffe. Keine perfekte Themenklassifikation, sondern heuristische Mustererkennung.",
    "narrative": "Verdichtete Deutungsmuster, die sich aus wiederkehrenden Begriffen und Themen ableiten. Zeigen, welche Geschichten den Diskurs prägen.",
    "salienz": "Beschreibt, worauf Aufmerksamkeit fällt - nicht unbedingt, was objektiv am wichtigsten ist. Hohe Salienz kann durch wenige aktive Stimmen entstehen.",
    "report": "Automatisch generierte Zusammenfassung auf Basis von Chatmustern. Liefert Hinweise auf Dynamiken, keine endgültigen Bewertungen."
}


GERMAN_STOPWORDS = {
    "aber", "alle", "allem", "allen", "aller", "alles", "als", "also", "am", "an",
    "ander", "andere", "anderem", "anderen", "anderer", "anderes", "auch", "auf",
    "aus", "bei", "bin", "bis", "bist", "da", "damit", "dann", "das", "dass",
    "dein", "deine", "dem", "den", "der", "des", "dessen", "deshalb", "die", "dies",
    "diese", "diesem", "diesen", "dieser", "dieses", "doch", "dort", "du", "durch",
    "ein", "eine", "einem", "einen", "einer", "eines", "er", "es", "euer", "eure",
    "für", "hat", "hatte", "hattest", "hattet", "hier", "hin", "hinter", "ich", "ihr",
    "ihre", "im", "in", "ist", "ja", "jede", "jedem", "jeden", "jeder", "jedes",
    "jener", "jenes", "jetzt", "kann", "kannst", "können", "könnt", "machen", "mein",
    "meine", "mit", "muss", "musst", "müssen", "müsst", "nach", "nicht", "noch", "nun",
    "nur", "oder", "sehr", "sein", "seine", "sich", "sie", "sind", "so", "solche",
    "solchem", "solchen", "solcher", "solches", "soll", "sollen", "sollst", "sollt",
    "sondern", "sonst", "über", "um", "und", "uns", "unser", "unter", "viel", "vom",
    "von", "vor", "wann", "warum", "was", "weiter", "weil", "wenn", "wer", "werden",
    "wie", "wieder", "wir", "wird", "wirst", "wo", "wollen", "wollt", "würde", "würden",
    "zu", "zum", "zur", "zwar"
}

TRIGGER_KEYWORDS = {
    "afd", "lügenpresse", "mainstream", "woke", "messer", "grüne", "gruenen", "merz",
    "migration", "migranten", "ausländer", "auslaender", "klimahysterie", "klimalüge",
    "klimaluege", "krieg", "putin", "nato", "gender", "linksgrün", "linksgruen",
    "systemmedien", "verräter", "verraeter", "schlafschafe", "propaganda", "wahrheit",
    "elite", "korrupt", "schande", "heimat", "remigration", "fakenews", "fake news"
}

TOXIC_MARKERS = {
    "idiot", "dumm", "lächerlich", "laecherlich", "peinlich", "krank", "hirnlos",
    "verrückt", "verrueckt", "lüge", "luege", "lügner", "luegner", "abschaum",
    "hasse", "hass", "fresse", "halt die", "geh sterben", "ekelhaft", "widerlich"
}

QUESTION_BAIT_MARKERS = {
    "warum", "wieso", "weshalb", "echt jetzt", "ehrliche frage", "ernsthaft",
    "nur mal so", "mal ne frage", "frage", "wirklich"
}

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U0001F1E6-\U0001F1FF"
    "\U000025A0-\U00002BEF"
    "]+",
    flags=re.UNICODE,
)

WORD_PATTERN = re.compile(r"[A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß0-9_-]{2,}")
UPPER_PATTERN = re.compile(r"[A-ZÄÖÜ]{4,}")


def now_dt() -> datetime:
    return datetime.now(TZ)


def now_ts() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS boards (
            board_id TEXT PRIMARY KEY,
            host_username TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            status TEXT NOT NULL DEFAULT 'idle',
            report_text TEXT DEFAULT ''
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            board_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            type TEXT NOT NULL,
            username TEXT NOT NULL,
            text TEXT NOT NULL,
            avatar_url TEXT,
            FOREIGN KEY (board_id) REFERENCES boards(board_id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_board_id_id ON messages(board_id, id)")
    conn.commit()
    conn.close()


def create_board() -> str:
    board_id = secrets.token_urlsafe(5).replace("-", "").replace("_", "")[:8].lower()
    conn = get_conn()
    conn.execute(
        "INSERT INTO boards(board_id, created_at, status, report_text) VALUES (?, ?, 'idle', '')",
        (board_id, now_ts())
    )
    conn.commit()
    conn.close()
    return board_id


def get_board(board_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM boards WHERE board_id = ?", (board_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_board(board_id: str, **kwargs):
    if not kwargs:
        return
    cols = ", ".join([f"{k} = ?" for k in kwargs.keys()])
    vals = list(kwargs.values()) + [board_id]
    conn = get_conn()
    conn.execute(f"UPDATE boards SET {cols} WHERE board_id = ?", vals)
    conn.commit()
    conn.close()


def insert_message(board_id: str, payload: dict):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO messages(board_id, timestamp, type, username, text, avatar_url)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            board_id,
            payload["timestamp"],
            payload["type"],
            payload["username"],
            payload["text"],
            payload.get("avatar_url")
        )
    )
    conn.commit()
    conn.close()


def load_messages(board_id: str):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT timestamp, type, username, text, avatar_url
        FROM messages
        WHERE board_id = ?
        ORDER BY id ASC
        """,
        (board_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def normalize_username(username: str) -> str:
    username = username.strip()
    if not username:
        raise ValueError("Bitte einen TikTok-Usernamen eingeben.")
    if not username.startswith("@"):
        username = "@" + username
    return username


def extract_words(text: str) -> list[str]:
    words = [w.lower() for w in WORD_PATTERN.findall(text)]
    return [w for w in words if w not in GERMAN_STOPWORDS and len(w) > 2]


def extract_emojis(text: str) -> list[str]:
    return EMOJI_PATTERN.findall(text)


def user_color(username: str) -> str:
    palette = [
        "#60a5fa", "#34d399", "#f59e0b", "#f472b6", "#a78bfa",
        "#fb7185", "#22d3ee", "#4ade80", "#f87171", "#c084fc"
    ]
    idx = int(hashlib.md5(username.encode("utf-8")).hexdigest(), 16) % len(palette)
    return palette[idx]


def initials(name: str) -> str:
    parts = [p for p in re.split(r"\s+", str(name).strip()) if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][:1] + parts[-1][:1]).upper()


def elapsed_label(start_iso: str | None) -> str:
    if not start_iso:
        return "-"
    try:
        start_dt = datetime.fromisoformat(start_iso)
        delta = now_dt() - start_dt
        seconds = max(int(delta.total_seconds()), 0)
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"
    except Exception:
        return "-"


def safe_avatar_url(user_obj) -> str | None:
    if user_obj is None:
        return None
    candidates = ["avatar_thumb", "avatar_medium", "avatar_large", "profilePicture"]
    for attr in candidates:
        try:
            media = getattr(user_obj, attr, None)
            if media is None:
                continue
            if isinstance(media, str) and media.startswith("http"):
                return media
            url_list = getattr(media, "url_list", None) or getattr(media, "urlList", None)
            if url_list and len(url_list) > 0:
                return url_list[0]
            url = getattr(media, "url", None)
            if isinstance(url, str) and url.startswith("http"):
                return url
        except Exception:
            pass
    return None


def classify_message(text: str) -> dict:
    lowered = text.lower()
    features = {
        "is_question": "?" in text or any(k in lowered for k in QUESTION_BAIT_MARKERS),
        "has_trigger": any(k in lowered for k in TRIGGER_KEYWORDS),
        "has_toxic_marker": any(k in lowered for k in TOXIC_MARKERS),
        "has_caps": bool(UPPER_PATTERN.search(text)),
        "has_link": "http://" in lowered or "https://" in lowered or "www." in lowered,
        "emoji_count": len(extract_emojis(text)),
        "word_count": len(text.split()),
    }
    if features["has_toxic_marker"]:
        tone = "abwertend"
    elif features["has_trigger"]:
        tone = "polarisierend"
    elif features["is_question"]:
        tone = "fragend"
    else:
        tone = "neutral"
    features["tone"] = tone
    return features


def clean_message_store(messages):
    cleaned = []
    for m in messages:
        if isinstance(m, dict) and {"timestamp", "type", "username", "text"}.issubset(set(m.keys())):
            cleaned.append(m)
    return cleaned


def build_dataframe(messages) -> pd.DataFrame:
    messages = clean_message_store(messages)
    if not messages:
        return pd.DataFrame(columns=[
            "timestamp", "username", "text", "type", "avatar_url", "is_question",
            "has_trigger", "has_toxic_marker", "has_caps", "has_link", "emoji_count",
            "word_count", "tone", "dt", "minute"
        ])
    rows = []
    for row in messages:
        base = {
            "timestamp": row["timestamp"],
            "username": row["username"],
            "text": row["text"],
            "type": row["type"],
            "avatar_url": row.get("avatar_url"),
        }
        base.update(classify_message(row["text"]))
        rows.append(base)
    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["minute"] = df["dt"].dt.floor("min")
    return df


def render_message_text(row: dict) -> str:
    return f"{row['username']}: {row['text']} [{row['timestamp'][11:19]}]"


def info_title(title: str, tooltip: str) -> str:
    return f"{title}  ℹ️"


def messages_to_txt(messages) -> str:
    messages = clean_message_store(messages)
    return "\n".join(render_message_text(m) for m in messages)


def messages_to_csv_bytes(messages) -> bytes:
    return build_dataframe(messages).to_csv(index=False).encode("utf-8")


def messages_to_json_bytes(messages) -> bytes:
    messages = clean_message_store(messages)
    return json.dumps(messages, ensure_ascii=False, indent=2).encode("utf-8")


def get_comment_messages(messages):
    return [m for m in messages if isinstance(m, dict) and m.get("type") == "comment"]


def summarize_heuristics(comment_df: pd.DataFrame) -> dict:
    if comment_df.empty:
        return {
            "messages": 0, "users": 0, "questions": 0, "trigger_msgs": 0,
            "toxic_msgs": 0, "caps_msgs": 0, "links": 0, "avg_length": 0
        }
    return {
        "messages": int(len(comment_df)),
        "users": int(comment_df["username"].nunique()),
        "questions": int(comment_df["is_question"].sum()),
        "trigger_msgs": int(comment_df["has_trigger"].sum()),
        "toxic_msgs": int(comment_df["has_toxic_marker"].sum()),
        "caps_msgs": int(comment_df["has_caps"].sum()),
        "links": int(comment_df["has_link"].sum()),
        "avg_length": round(comment_df["text"].str.len().mean(), 1),
    }


def top_words(comment_df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    counter = Counter()
    for text in comment_df["text"].tolist():
        counter.update(extract_words(text))
    return pd.DataFrame([{"word": k, "count": v} for k, v in counter.most_common(n)])


def top_emojis(comment_df: pd.DataFrame, n: int = 12) -> pd.DataFrame:
    counter = Counter()
    for text in comment_df["text"].tolist():
        counter.update(extract_emojis(text))
    return pd.DataFrame([{"emoji": k, "count": v} for k, v in counter.most_common(n)])


def top_users(comment_df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["username", "messages"])
    return (
        comment_df.groupby("username")
        .size()
        .reset_index(name="messages")
        .sort_values("messages", ascending=False)
        .head(n)
    )


def activity_per_minute(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["minute", "messages"])
    return (
        comment_df.groupby("minute")
        .size()
        .reset_index(name="messages")
        .sort_values("minute")
    )


def repeated_messages(comment_df: pd.DataFrame, min_count: int = 2) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["username", "text", "count"])
    df = comment_df.copy()
    df["text_norm"] = df["text"].str.strip().str.lower()
    rep = (
        df.groupby(["username", "text_norm"])
        .size()
        .reset_index(name="count")
    )
    rep = rep[rep["count"] >= min_count].sort_values("count", ascending=False)
    rep = rep.rename(columns={"text_norm": "text"})
    return rep.head(50)


def user_scores(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=[
            "username", "messages", "trigger_ratio", "toxic_ratio", "question_ratio",
            "repeat_ratio", "caps_ratio", "shift_score", "role"
        ])
    df = comment_df.copy()
    df["text_norm"] = df["text"].str.strip().str.lower()
    rows = []
    for username, group in df.groupby("username"):
        total = len(group)
        repeated = group["text_norm"].value_counts()
        repeated_msgs = int(repeated[repeated > 1].sum()) if not repeated.empty else 0
        trigger_ratio = float(group["has_trigger"].mean()) if total else 0.0
        toxic_ratio = float(group["has_toxic_marker"].mean()) if total else 0.0
        question_ratio = float(group["is_question"].mean()) if total else 0.0
        caps_ratio = float(group["has_caps"].mean()) if total else 0.0
        repeat_ratio = repeated_msgs / total if total else 0.0
        volume_factor = min(total / 12.0, 1.0)
        shift_score = round(
            100 * (
                0.28 * volume_factor +
                0.24 * trigger_ratio +
                0.18 * repeat_ratio +
                0.16 * question_ratio +
                0.08 * caps_ratio +
                0.06 * toxic_ratio
            ),
            1
        )
        if shift_score >= 65:
            role = "stark auffällig"
        elif shift_score >= 45:
            role = "auffällig"
        elif trigger_ratio >= 0.4:
            role = "Narrativ-Verstärker"
        elif question_ratio >= 0.5 and total >= 4:
            role = "Frage-Treiber"
        elif total >= 8 and toxic_ratio < 0.15:
            role = "sehr aktiv"
        else:
            role = "normal"
        rows.append({
            "username": username,
            "messages": total,
            "trigger_ratio": round(trigger_ratio, 2),
            "toxic_ratio": round(toxic_ratio, 2),
            "question_ratio": round(question_ratio, 2),
            "repeat_ratio": round(repeat_ratio, 2),
            "caps_ratio": round(caps_ratio, 2),
            "shift_score": shift_score,
            "role": role,
        })
    return pd.DataFrame(rows).sort_values(["shift_score", "messages"], ascending=[False, False]).reset_index(drop=True)


def build_clusters(comment_df: pd.DataFrame, max_clusters: int = 8) -> pd.DataFrame:
    if not SKLEARN_AVAILABLE or comment_df.empty or len(comment_df) < 8:
        return pd.DataFrame(columns=["cluster", "label", "messages"])
    try:
        texts = comment_df["text"].astype(str).tolist()
        vectorizer = TfidfVectorizer(
            max_features=1200,
            ngram_range=(1, 2),
            min_df=2,
            stop_words=list(GERMAN_STOPWORDS),
        )
        X = vectorizer.fit_transform(texts)
        if X.shape[1] == 0:
            return pd.DataFrame(columns=["cluster", "label", "messages"])
        k = max(2, min(max_clusters, int(math.sqrt(len(texts)))))
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = model.fit_predict(X)
        terms = vectorizer.get_feature_names_out()
        order_centroids = model.cluster_centers_.argsort()[:, ::-1]
        counts = Counter(labels)
        rows = []
        for i in range(k):
            top_terms = [terms[ind] for ind in order_centroids[i, :4] if ind < len(terms)]
            label = ", ".join(top_terms[:3]) if top_terms else f"Cluster {i + 1}"
            rows.append({"cluster": i + 1, "label": label, "messages": counts.get(i, 0)})
        return pd.DataFrame(rows).sort_values("messages", ascending=False)
    except Exception:
        return pd.DataFrame(columns=["cluster", "label", "messages"])


def filtered_comment_df(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    q = filters.get("search", "").strip().lower()
    if q:
        out = out[out["text"].str.lower().str.contains(re.escape(q), regex=True)]
    user = filters.get("user")
    if user and user != "Alle":
        out = out[out["username"] == user]
    tone = filters.get("tone")
    if tone and tone != "Alle":
        out = out[out["tone"] == tone]
    if filters.get("only_questions"):
        out = out[out["is_question"]]
    if filters.get("only_triggers"):
        out = out[out["has_trigger"]]
    if filters.get("only_toxic"):
        out = out[out["has_toxic_marker"]]
    return out


def calibrate_band(raw_score: float) -> int:
    # conservative thresholds so +3 is rare and 0 is a true middle zone
    if raw_score >= 0.88:
        return 3
    if raw_score >= 0.75:
        return 2
    if raw_score >= 0.62:
        return 1
    if raw_score >= 0.46:
        return 0
    if raw_score >= 0.34:
        return -1
    if raw_score >= 0.22:
        return -2
    return -3


def score_label(score: int) -> str:
    mapping = {
        3: "sehr stark",
        2: "stark",
        1: "eher gut",
        0: "neutral",
        -1: "leicht kritisch",
        -2: "kritisch",
        -3: "stark kritisch",
    }
    return mapping.get(score, "neutral")


def score_color(score: int) -> str:
    mapping = {
        3: "#16a34a",
        2: "#22c55e",
        1: "#84cc16",
        0: "#f59e0b",
        -1: "#f97316",
        -2: "#ef4444",
        -3: "#b91c1c",
    }
    return mapping.get(score, "#94a3b8")


def score_arrow(score: int) -> str:
    if score >= 2:
        return "▲"
    if score == 1:
        return "↗"
    if score == 0:
        return "→"
    if score == -1:
        return "↘"
    return "▼"


def impact_scores(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame) -> dict:
    if comment_df.empty:
        return {
            "Diskurskultur": 0,
            "Salienz-Bewusstsein": 0,
            "Verantwortung & Macht": 0,
            "Systemischer Impact": 0,
            "Emotionale Resonanz": 0,
        }

    toxic = float(comment_df["has_toxic_marker"].mean())
    trigger = float(comment_df["has_trigger"].mean())
    caps = float(comment_df["has_caps"].mean())

    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0

    cluster_top = 0.0
    if not clusters_df.empty and clusters_df["messages"].sum() > 0:
        cluster_top = float(clusters_df["messages"].max() / clusters_df["messages"].sum())

    flagged_ratio = float((scores_df["shift_score"] >= 45).mean()) if not scores_df.empty else 0.0

    emoji_level = min(float(comment_df["emoji_count"].mean()) / 3.0, 1.0)
    avg_len = min(float(comment_df["text"].str.len().mean()) / 120.0, 1.0)

    raw = {
        "Diskurskultur": 0.45 * (1 - toxic) + 0.25 * (1 - caps) + 0.30 * (1 - concentration),
        "Salienz-Bewusstsein": 0.45 * (1 - trigger) + 0.25 * (1 - concentration) + 0.30 * (1 - cluster_top),
        "Verantwortung & Macht": 0.40 * (1 - toxic) + 0.30 * (1 - flagged_ratio) + 0.30 * (1 - trigger),
        "Systemischer Impact": 0.45 * (1 - trigger) + 0.20 * (1 - flagged_ratio) + 0.35 * (1 - cluster_top),
        "Emotionale Resonanz": 0.35 * (1 - toxic) + 0.20 * (1 - caps) + 0.20 * emoji_level + 0.25 * avg_len,
    }
    return {k: calibrate_band(max(0.0, min(v, 1.0))) for k, v in raw.items()}


def narrative_candidates(comment_df: pd.DataFrame) -> list[str]:
    if comment_df.empty:
        return []
    words = top_words(comment_df, n=20)
    if words.empty:
        return []
    out = []
    top = words["word"].tolist()
    if any(w in top for w in ["migration", "migranten", "ausländer", "auslaender"]):
        out.append("Migration als dominantes Problemfeld")
    if any(w in top for w in ["merz", "afd", "grüne", "gruenen"]):
        out.append("Parteipolitische Konfliktlinien dominieren")
    if any(w in top for w in ["propaganda", "lügenpresse", "fakenews", "systemmedien"]):
        out.append("Misstrauen gegenüber Medien und Öffentlichkeit")
    if any(w in top for w in ["krieg", "putin", "nato"]):
        out.append("Geopolitische Konfliktnarrative")
    if any(w in top for w in ["gender", "woke"]):
        out.append("Kulturkampf- und Identitätsnarrative")
    return out[:5]


def role_summary(scores_df: pd.DataFrame) -> dict:
    return scores_df["role"].value_counts().to_dict() if not scores_df.empty else {}


def salience_warning(comment_df: pd.DataFrame, scores_df: pd.DataFrame) -> str:
    if comment_df.empty:
        return "Noch keine Daten."
    trigger = float(comment_df["has_trigger"].mean())
    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
    if trigger > 0.25 and concentration > 0.18:
        return "Auffällig: Aufmerksamkeit scheint stark durch wenige polarisierende Stimmen gebunden zu werden."
    if trigger > 0.18:
        return "Hinweis: Ein überdurchschnittlicher Teil der Aufmerksamkeit liegt auf Triggerbegriffen und Konfliktmarkern."
    if concentration > 0.20:
        return "Hinweis: Einzelne sehr aktive Accounts prägen die Wahrnehmung überproportional."
    return "Keine starke Salienz-Drift erkennbar."


def metric_snapshot(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame) -> dict:
    if comment_df.empty:
        return {
            "toxic": 0.0,
            "trigger": 0.0,
            "questions": 0.0,
            "caps": 0.0,
            "avg_len": 0.0,
            "emoji_balance": 0.0,
            "concentration": 0.0,
            "cluster_top": 0.0,
            "flagged_ratio": 0.0,
            "severe_ratio": 0.0,
        }
    toxic = float(comment_df["has_toxic_marker"].mean())
    trigger = float(comment_df["has_trigger"].mean())
    questions = float(comment_df["is_question"].mean())
    caps = float(comment_df["has_caps"].mean())
    avg_len = min(float(comment_df["text"].str.len().mean()) / 110.0, 1.0)
    emoji_balance = min(float(comment_df["emoji_count"].mean()) / 2.5, 1.0)
    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
    cluster_top = 0.0
    if not clusters_df.empty and clusters_df["messages"].sum() > 0:
        cluster_top = float(clusters_df["messages"].max() / clusters_df["messages"].sum())
    flagged_ratio = float((scores_df["shift_score"] >= 45).mean()) if not scores_df.empty else 0.0
    severe_ratio = float((scores_df["shift_score"] >= 65).mean()) if not scores_df.empty else 0.0
    return {
        "toxic": toxic,
        "trigger": trigger,
        "questions": questions,
        "caps": caps,
        "avg_len": avg_len,
        "emoji_balance": emoji_balance,
        "concentration": concentration,
        "cluster_top": cluster_top,
        "flagged_ratio": flagged_ratio,
        "severe_ratio": severe_ratio,
    }


def explain_impact_scores(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame, impact: dict) -> dict:
    m = metric_snapshot(comment_df, scores_df, clusters_df)

    def pct(v: float) -> str:
        return f"{v * 100:.1f}%"

    explanations = {
        "Diskurskultur": (
            f"Score {impact['Diskurskultur']}. "
            f"Wichtig waren hier vor allem Abwertungsquote ({pct(m['toxic'])}), Capslock-Anteil ({pct(m['caps'])}) "
            f"und die Konzentration auf wenige User ({pct(m['concentration'])}). "
            f"Der Wert steigt, wenn der Chat respektvoller, weniger dominant und etwas dialogischer wirkt. "
            f"Fragequote aktuell: {pct(m['questions'])}."
        ),
        "Salienz-Bewusstsein": (
            f"Score {impact['Salienz-Bewusstsein']}. "
            f"Dieser Wert reagiert besonders auf Triggerquote ({pct(m['trigger'])}), Konzentration auf wenige User ({pct(m['concentration'])}) "
            f"und Themenverengung über dominante Cluster ({pct(m['cluster_top'])}). "
            f"Je stärker Aufmerksamkeit durch wenige laute Impulse gebunden wird, desto niedriger fällt der Score aus."
        ),
        "Verantwortung & Macht": (
            f"Score {impact['Verantwortung & Macht']}. "
            f"Entscheidend waren der Anteil auffälliger Accounts ({pct(m['flagged_ratio'])}), stark auffälliger Accounts ({pct(m['severe_ratio'])}), "
            f"die Abwertungsquote ({pct(m['toxic'])}) und die Konzentration ({pct(m['concentration'])}). "
            f"Je dominanter einzelne Muster den Raum prägen, desto kritischer wird der Wert."
        ),
        "Systemischer Impact": (
            f"Score {impact['Systemischer Impact']}. "
            f"Hier fließen vor allem Triggerquote ({pct(m['trigger'])}), Themenverengung ({pct(m['cluster_top'])}), "
            f"auffällige Accounts ({pct(m['flagged_ratio'])}) und die durchschnittliche Textsubstanz ein. "
            f"Je breiter und weniger polarisierend der Diskurs, desto höher der Score."
        ),
        "Emotionale Resonanz": (
            f"Score {impact['Emotionale Resonanz']}. "
            f"Relevant sind Abwertungsquote ({pct(m['toxic'])}), Capslock-Anteil ({pct(m['caps'])}), "
            f"Emojis bzw. emotionale Beteiligung ({m['emoji_balance'] * 100:.1f}% normiert), durchschnittliche Textlänge "
            f"und Konzentration auf wenige Stimmen ({pct(m['concentration'])}). "
            f"Der Wert steigt bei konstruktiver Beteiligung und sinkt bei Aggression oder Überhitzung."
        ),
    }
    return explanations




def event_overview(messages) -> pd.DataFrame:
    msgs = clean_message_store(messages)
    if not msgs:
        return pd.DataFrame(columns=["event", "count"])

    counts = Counter(
        m.get("type", "unknown")
        for m in msgs
        if m.get("type") not in {"comment", "system", "error"}
    )

    if not counts:
        return pd.DataFrame(columns=["event", "count"])

    return (
        pd.DataFrame([{"event": k, "count": v} for k, v in counts.items()])
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )


def recent_window_metrics(comment_df: pd.DataFrame, minutes: int = 5) -> dict:
    if comment_df.empty or comment_df["dt"].isna().all():
        return {"messages": 0, "trigger_rate": 0.0, "toxic_rate": 0.0, "question_rate": 0.0}
    end = comment_df["dt"].max()
    start = end - pd.Timedelta(minutes=minutes)
    recent = comment_df[comment_df["dt"] >= start]
    if recent.empty:
        return {"messages": 0, "trigger_rate": 0.0, "toxic_rate": 0.0, "question_rate": 0.0}
    return {
        "messages": int(len(recent)),
        "trigger_rate": float(recent["has_trigger"].mean()),
        "toxic_rate": float(recent["has_toxic_marker"].mean()),
        "question_rate": float(recent["is_question"].mean()),
    }


def compute_live_ampel(comment_df: pd.DataFrame, scores_df: pd.DataFrame, impact: dict) -> dict:
    if comment_df.empty:
        return {"score": 50, "label": "neutral", "color": "#f59e0b", "ampel": "gelb", "trend": "→"}
    repeated_df = repeated_messages(comment_df, min_count=2)
    repeat_pressure = min((repeated_df["count"].sum() if not repeated_df.empty else 0) / max(len(comment_df), 1), 1.0)
    toxic = float(comment_df["has_toxic_marker"].mean())
    trigger = float(comment_df["has_trigger"].mean())
    caps = float(comment_df["has_caps"].mean())
    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
    flagged = float((scores_df["shift_score"] >= 45).mean()) if not scores_df.empty else 0.0
    positive = (
        0.22 * ((impact.get("Diskurskultur", 0) + 3) / 6) +
        0.20 * ((impact.get("Salienz-Bewusstsein", 0) + 3) / 6) +
        0.20 * ((impact.get("Verantwortung & Macht", 0) + 3) / 6) +
        0.18 * ((impact.get("Systemischer Impact", 0) + 3) / 6) +
        0.20 * ((impact.get("Emotionale Resonanz", 0) + 3) / 6)
    )
    pressure = 0.28 * toxic + 0.24 * trigger + 0.14 * concentration + 0.14 * repeat_pressure + 0.10 * caps + 0.10 * flagged
    score = max(0, min(100, round(100 * (0.62 * positive + 0.38 * (1 - pressure)))))
    if score >= 78:
        label, color, ampel = "stabil", "#16a34a", "grün"
    elif score >= 60:
        label, color, ampel = "beobachten", "#84cc16", "gelb-grün"
    elif score >= 42:
        label, color, ampel = "angespannt", "#f59e0b", "gelb"
    elif score >= 26:
        label, color, ampel = "kritisch", "#f97316", "orange"
    else:
        label, color, ampel = "eskaliert", "#dc2626", "rot"

    if comment_df["dt"].notna().any():
        end = comment_df["dt"].max()
        recent = comment_df[comment_df["dt"] >= end - pd.Timedelta(minutes=5)]
        prev = comment_df[(comment_df["dt"] < end - pd.Timedelta(minutes=5)) & (comment_df["dt"] >= end - pd.Timedelta(minutes=10))]
        recent_pressure = (float(recent["has_trigger"].mean()) if not recent.empty else 0) + (float(recent["has_toxic_marker"].mean()) if not recent.empty else 0)
        prev_pressure = (float(prev["has_trigger"].mean()) if not prev.empty else 0) + (float(prev["has_toxic_marker"].mean()) if not prev.empty else 0)
        if recent_pressure > prev_pressure + 0.08:
            trend = "↘"
        elif recent_pressure + 0.08 < prev_pressure:
            trend = "↗"
        else:
            trend = "→"
    else:
        trend = "→"
    return {"score": score, "label": label, "color": color, "ampel": ampel, "trend": trend}


def compute_alerts(comment_df: pd.DataFrame, scores_df: pd.DataFrame, impact: dict) -> list[dict]:
    alerts = []
    if comment_df.empty:
        return alerts

    recent = recent_window_metrics(comment_df, minutes=5)
    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
    repeat_df = repeated_messages(comment_df, min_count=3)
    top_user = user_counts.idxmax() if not user_counts.empty else "-"
    top_share = concentration * 100

    if impact.get("Diskurskultur", 0) <= -1:
        alerts.append({
            "level": "red",
            "title": "Diskurskultur kritisch",
            "detail": "Der Gesamtwert der Diskurskultur ist unter den neutralen Bereich gefallen. Das deutet auf mehr Reibung, Dominanz oder Abwertung hin."
        })
    if recent["trigger_rate"] >= 0.22:
        alerts.append({
            "level": "orange",
            "title": "Triggerquote erhöht",
            "detail": f"In den letzten 5 Minuten waren {recent['trigger_rate']*100:.1f}% der Nachrichten triggerhaltig."
        })
    if recent["toxic_rate"] >= 0.08:
        alerts.append({
            "level": "red",
            "title": "Abwertende Sprache steigt",
            "detail": f"Im letzten Zeitfenster waren {recent['toxic_rate']*100:.1f}% der Nachrichten abwertend oder toxisch."
        })
    if concentration >= 0.18:
        alerts.append({
            "level": "yellow",
            "title": "Dominanter Account",
            "detail": f"{top_user} prägt aktuell etwa {top_share:.1f}% des Chats."
        })
    if not scores_df.empty and (scores_df["shift_score"] >= 65).any():
        strongest = scores_df.sort_values("shift_score", ascending=False).iloc[0]
        alerts.append({
            "level": "orange",
            "title": "Stark auffälliger Account",
            "detail": f"{strongest['username']} hat aktuell den höchsten Shift-Score ({strongest['shift_score']})."
        })
    if not repeat_df.empty:
        top_repeat = repeat_df.iloc[0]
        alerts.append({
            "level": "yellow",
            "title": "Wiederholungsmuster erkannt",
            "detail": f"{top_repeat['username']} wiederholt eine Nachricht auffällig oft ({int(top_repeat['count'])}x)."
        })
    if not alerts:
        alerts.append({
            "level": "green",
            "title": "Keine akuten Warnsignale",
            "detail": "Aktuell zeigen Trigger, Toxizität, Dominanz und Wiederholungen keine kritische Zuspitzung."
        })
    return alerts[:6]


def narrative_drift(comment_df: pd.DataFrame, bucket: str = "5min") -> pd.DataFrame:
    if comment_df.empty or comment_df["dt"].isna().all():
        return pd.DataFrame(columns=["bucket", "label", "messages"])
    df = comment_df.copy()
    df["bucket"] = df["dt"].dt.floor(bucket)
    rows = []
    for bucket_val, group in df.groupby("bucket"):
        words = Counter()
        for txt in group["text"].astype(str):
            words.update(extract_words(txt))
        top = ", ".join([w for w, _ in words.most_common(3)]) if words else "kein klares Thema"
        rows.append({"bucket": bucket_val, "label": top, "messages": int(len(group))})
    out = pd.DataFrame(rows).sort_values("bucket")
    return out


def mention_edges(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["source", "target", "count"])
    rows = []
    pattern = re.compile(r"@([A-Za-z0-9_.]+)")
    for _, row in comment_df.iterrows():
        targets = pattern.findall(str(row["text"]))
        for target in targets:
            rows.append({"source": row["username"], "target": target, "count": 1})
    if not rows:
        return pd.DataFrame(columns=["source", "target", "count"])
    df = pd.DataFrame(rows)
    return df.groupby(["source", "target"], as_index=False)["count"].sum().sort_values("count", ascending=False)




def influencer_map(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["user", "sent_mentions", "received_mentions", "out_degree", "in_degree", "messages", "role"])
    edges = mention_edges(comment_df)
    msg_counts = comment_df.groupby("username").size().to_dict()

    if edges.empty:
        rows = []
        for user, cnt in sorted(msg_counts.items(), key=lambda x: x[1], reverse=True):
            rows.append({
                "user": user,
                "sent_mentions": 0,
                "received_mentions": 0,
                "out_degree": 0,
                "in_degree": 0,
                "messages": int(cnt),
                "role": "isoliert/ohne Erwähnungen",
            })
        return pd.DataFrame(rows)

    sent_mentions = edges.groupby("source")["count"].sum().to_dict()
    received_mentions = edges.groupby("target")["count"].sum().to_dict()
    out_degree = edges.groupby("source")["target"].nunique().to_dict()
    in_degree = edges.groupby("target")["source"].nunique().to_dict()

    users = set(msg_counts.keys()) | set(sent_mentions.keys()) | set(received_mentions.keys())
    rows = []
    for user in users:
        sm = int(sent_mentions.get(user, 0))
        rm = int(received_mentions.get(user, 0))
        od = int(out_degree.get(user, 0))
        ind = int(in_degree.get(user, 0))
        mc = int(msg_counts.get(user, 0))

        if rm >= 4 and ind >= 2:
            role = "Hub / Bezugspunkt"
        elif sm >= 4 and od >= 2:
            role = "Aktiver Verstärker"
        elif sm >= 2 and rm == 0:
            role = "Initiator / Sender"
        elif rm >= 2 and sm == 0:
            role = "Wird adressiert"
        else:
            role = "peripher"

        rows.append({
            "user": user,
            "sent_mentions": sm,
            "received_mentions": rm,
            "out_degree": od,
            "in_degree": ind,
            "messages": mc,
            "role": role,
        })

    return pd.DataFrame(rows).sort_values(
        ["received_mentions", "sent_mentions", "messages"],
        ascending=[False, False, False]
    ).reset_index(drop=True)


def greeting_edges(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["source", "target", "count"])
    rows = []
    greet_words = ["hallo", "hey", "hi", "moin", "servus"]
    pattern = re.compile(r"@([A-Za-z0-9_.]+)")
    for _, row in comment_df.iterrows():
        txt = str(row["text"]).lower()
        if any(word in txt for word in greet_words):
            targets = pattern.findall(txt)
            for target in targets:
                rows.append({"source": row["username"], "target": target, "count": 1})
    if not rows:
        return pd.DataFrame(columns=["source", "target", "count"])
    df = pd.DataFrame(rows)
    return df.groupby(["source", "target"], as_index=False)["count"].sum().sort_values("count", ascending=False)


def user_detail_snapshot(comment_df: pd.DataFrame, username: str) -> dict:
    if comment_df.empty or not username:
        return {}
    user_df = comment_df[comment_df["username"] == username].copy()
    if user_df.empty:
        return {}
    return {
        "messages": int(len(user_df)),
        "trigger_rate": float(user_df["has_trigger"].mean()),
        "toxic_rate": float(user_df["has_toxic_marker"].mean()),
        "question_rate": float(user_df["is_question"].mean()),
        "repeat_count": int(repeated_messages(user_df, min_count=2)["count"].sum()) if not repeated_messages(user_df, min_count=2).empty else 0,
        "first_seen": user_df["dt"].min(),
        "last_seen": user_df["dt"].max(),
        "recent_messages": user_df.sort_values("dt", ascending=False).head(8)[["timestamp", "text"]].to_dict("records")
    }


def phase_of_live(comment_df: pd.DataFrame) -> str:
    if comment_df.empty or comment_df["dt"].isna().all():
        return "keine Daten"
    activity = activity_per_minute(comment_df)
    if activity.empty:
        return "keine Daten"
    recent = activity.tail(5)["messages"].mean()
    overall = activity["messages"].mean()
    early = activity.head(5)["messages"].mean()
    if recent >= overall * 1.25:
        return "Peak"
    if recent <= overall * 0.75 and activity.shape[0] > 10:
        return "Abklingen"
    if early >= overall * 0.9 and activity.shape[0] <= 10:
        return "Warmup"
    return "laufende Debatte"

def critical_moments(comment_df: pd.DataFrame, bucket: str = "1min") -> pd.DataFrame:
    if comment_df.empty or comment_df["dt"].isna().all():
        return pd.DataFrame(columns=["bucket", "messages", "trigger_rate", "toxic_rate", "caps_rate", "dominance", "escalation_score", "signal"])
    df = comment_df.copy()
    df["bucket"] = df["dt"].dt.floor(bucket)
    rows = []
    for bucket_val, group in df.groupby("bucket"):
        user_counts = group.groupby("username").size()
        dominance = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
        trigger_rate = float(group["has_trigger"].mean()) if len(group) else 0.0
        toxic_rate = float(group["has_toxic_marker"].mean()) if len(group) else 0.0
        caps_rate = float(group["has_caps"].mean()) if len(group) else 0.0
        escalation_score = round(100 * (0.34 * trigger_rate + 0.28 * toxic_rate + 0.18 * dominance + 0.20 * caps_rate), 1)
        signal = "stabil"
        if escalation_score >= 40:
            signal = "kritisch"
        elif escalation_score >= 24:
            signal = "angespannt"
        rows.append({
            "bucket": bucket_val,
            "messages": int(len(group)),
            "trigger_rate": trigger_rate,
            "toxic_rate": toxic_rate,
            "caps_rate": caps_rate,
            "dominance": dominance,
            "escalation_score": escalation_score,
            "signal": signal,
        })
    out = pd.DataFrame(rows).sort_values("bucket")
    return out


def fairness_metrics(comment_df: pd.DataFrame) -> dict:
    if comment_df.empty:
        return {"top1_share": 0.0, "top3_share": 0.0, "gini": 0.0, "dominant_user": "-", "users": 0}
    counts = comment_df.groupby("username").size().sort_values(ascending=False)
    total = float(counts.sum())
    top1_share = float(counts.iloc[0] / total) if len(counts) else 0.0
    top3_share = float(counts.head(3).sum() / total) if len(counts) else 0.0
    arr = counts.to_numpy(dtype=float)
    if arr.sum() == 0 or len(arr) == 0:
        gini = 0.0
    else:
        arr = arr[arr >= 0]
        arr.sort()
        n = len(arr)
        gini = float((2 * ((list(range(1, n + 1)) * arr).sum()) / (n * arr.sum())) - (n + 1) / n)
    return {
        "top1_share": top1_share,
        "top3_share": top3_share,
        "gini": gini,
        "dominant_user": str(counts.index[0]) if len(counts) else "-",
        "users": int(len(counts)),
    }


def trigger_effect_analysis(comment_df: pd.DataFrame, keywords: set[str] | None = None) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["keyword", "count", "share", "question_rate", "toxic_rate", "avg_length"])
    keys = keywords or TRIGGER_KEYWORDS
    rows = []
    lower_text = comment_df["text"].astype(str).str.lower()
    for kw in sorted(keys):
        mask = lower_text.str.contains(re.escape(kw), regex=True)
        sub = comment_df[mask]
        if sub.empty:
            continue
        rows.append({
            "keyword": kw,
            "count": int(len(sub)),
            "share": float(len(sub) / max(len(comment_df), 1)),
            "question_rate": float(sub["is_question"].mean()),
            "toxic_rate": float(sub["has_toxic_marker"].mean()),
            "avg_length": float(sub["text"].str.len().mean()),
        })
    if not rows:
        return pd.DataFrame(columns=["keyword", "count", "share", "question_rate", "toxic_rate", "avg_length"])
    return pd.DataFrame(rows).sort_values(["count", "toxic_rate"], ascending=[False, False]).reset_index(drop=True)


def user_archetypes(comment_df: pd.DataFrame, scores_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty or scores_df.empty:
        return pd.DataFrame(columns=["username", "archetype", "messages", "why"])
    rows = []
    for _, row in scores_df.iterrows():
        archetype = "Teilnehmer"
        why = []
        if row["repeat_ratio"] >= 0.35:
            archetype = "Echo / Repeater"
            why.append("viele Wiederholungen")
        if row["trigger_ratio"] >= 0.35 and row["question_ratio"] >= 0.25:
            archetype = "Provokateur"
            why = ["viele Trigger", "lenkende Fragen"]
        elif row["trigger_ratio"] >= 0.35:
            archetype = "Narrativ-Verstärker"
            why = ["viele Trigger"]
        elif row["question_ratio"] >= 0.45:
            archetype = "Frage-Treiber"
            why = ["hohe Fragequote"]
        elif row["messages"] >= 10 and row["toxic_ratio"] < 0.1:
            archetype = "Aktiver Stammgast"
            why = ["sehr aktiv", "wenig toxisch"]
        elif row["toxic_ratio"] >= 0.18:
            archetype = "Eskalierer"
            why = ["überdurchschnittlich toxisch"]
        rows.append({
            "username": row["username"],
            "archetype": archetype,
            "messages": int(row["messages"]),
            "why": ", ".join(why) if why else "unauffällig",
        })
    return pd.DataFrame(rows).sort_values(["messages", "archetype"], ascending=[False, True]).reset_index(drop=True)


def attention_vs_substance(comment_df: pd.DataFrame) -> pd.DataFrame:
    if comment_df.empty:
        return pd.DataFrame(columns=["username", "attention_share", "avg_length", "substance_score", "attention_minus_substance"])
    rows = []
    total = max(len(comment_df), 1)
    for username, group in comment_df.groupby("username"):
        attention_share = float(len(group) / total)
        avg_length = float(group["text"].str.len().mean()) if len(group) else 0.0
        meaningful_words = group["text"].astype(str).apply(lambda t: len(extract_words(t))).mean() if len(group) else 0.0
        substance_score = min((avg_length / 80.0) * 0.55 + (meaningful_words / 12.0) * 0.45, 1.0)
        rows.append({
            "username": username,
            "attention_share": attention_share,
            "avg_length": avg_length,
            "substance_score": substance_score,
            "attention_minus_substance": attention_share - substance_score,
        })
    return pd.DataFrame(rows).sort_values("attention_minus_substance", ascending=False).reset_index(drop=True)


def generate_rule_based_report(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame, impact: dict) -> str:
    if comment_df.empty:
        return "Es gibt noch keine Chatdaten für einen Report."

    summary = summarize_heuristics(comment_df)
    top_words_df = top_words(comment_df, n=12)
    top_users_df = top_users(comment_df, n=10)
    activity_df = activity_per_minute(comment_df)
    rep_df = repeated_messages(comment_df, min_count=2).head(8)
    narratives = narrative_candidates(comment_df)

    peak_text = "Kein Aktivitätspeak erkennbar."
    if not activity_df.empty:
        peak_row = activity_df.sort_values("messages", ascending=False).iloc[0]
        peak_time = pd.to_datetime(peak_row["minute"]).strftime("%H:%M")
        peak_text = f"Der stärkste Peak lag um {peak_time} Uhr mit {int(peak_row['messages'])} Nachrichten in einer Minute."

    cluster_text = "Es konnten noch keine stabilen Themencluster gebildet werden."
    if not clusters_df.empty:
        lines = [f"- {row['label']} ({int(row['messages'])} Nachrichten)" for _, row in clusters_df.head(5).iterrows()]
        cluster_text = "Dominante Cluster:\n" + "\n".join(lines)

    narrative_text = "Noch keine klaren wiederkehrenden Narrative erkennbar."
    if narratives:
        narrative_text = "Auffällige Narrative:\n" + "\n".join(f"- {x}" for x in narratives)

    suspicious_text = "Noch keine auffälligen User-Muster erkennbar."
    if not scores_df.empty:
        flagged = scores_df[scores_df["shift_score"] >= 45].head(8)
        if not flagged.empty:
            flagged_lines = [
                f"- {row['username']}: Shift-Score {row['shift_score']}, Rolle {row['role']}, {int(row['messages'])} Nachrichten"
                for _, row in flagged.iterrows()
            ]
            suspicious_text = (
                "Auffällige User-Verhaltensmuster:\n"
                + "\n".join(flagged_lines)
                + "\nDiese Hinweise deuten auf überdurchschnittliche Aktivität, Triggernutzung, Wiederholungen oder Frage-Druck hin, "
                  "sind aber kein Beweis für koordinierte oder absichtliche Manipulation."
            )

    repeated_text = "Keine auffälligen Wiederholungen erkannt."
    if not rep_df.empty:
        rep_lines = [f"- {row['username']}: \"{str(row['text'])[:100]}\" ({int(row['count'])}x)" for _, row in rep_df.iterrows()]
        repeated_text = "Wiederholungen / mögliche Spam-Muster:\n" + "\n".join(rep_lines)

    infl_df = influencer_map(comment_df)
    influencer_text = "Keine klare Influencer-Struktur über @-Erwähnungen erkennbar."
    if not infl_df.empty:
        influencer_lines = [
            f"- {row['user']}: empfangen {int(row['received_mentions'])}, gesendet {int(row['sent_mentions'])}, Rolle {row['role']}"
            for _, row in infl_df.head(5).iterrows()
        ]
        influencer_text = "Influencer-Map / Ansprache-Struktur:\n" + "\n".join(influencer_lines)

    impact_text = "\n".join([f"- {k}: {v}" for k, v in impact.items()])
    top_words_text = ", ".join(top_words_df["word"].head(10).tolist()) if not top_words_df.empty else "-"
    top_users_text = ", ".join([f"{r['username']} ({int(r['messages'])})" for _, r in top_users_df.head(8).iterrows()]) if not top_users_df.empty else "-"
    question_rate = (comment_df["is_question"].mean() * 100) if not comment_df.empty else 0
    trigger_rate = (comment_df["has_trigger"].mean() * 100) if not comment_df.empty else 0
    toxic_rate = (comment_df["has_toxic_marker"].mean() * 100) if not comment_df.empty else 0

    report = f"""1. Kurzfazit

Der Chat umfasste {summary['messages']} Nachrichten von {summary['users']} Usern.
Fragequote: {question_rate:.1f} Prozent.
Triggerquote: {trigger_rate:.1f} Prozent.
Abwertungsquote: {toxic_rate:.1f} Prozent.

2. Wirkungsfelder nach Live-Impact-Kompass

{impact_text}

3. Hauptthemen, Cluster und Narrative

Häufige Begriffe: {top_words_text}

{cluster_text}

{narrative_text}

4. Diskursdynamik und Aufmerksamkeit

{peak_text}

{salience_warning(comment_df, scores_df)}

5. Auffällige User-Muster

Die aktivsten User waren: {top_users_text}.

{suspicious_text}

6. Wiederholungen und mögliche Diskursverschiebung

{repeated_text}

7. Influencer-Map und soziale Adressierung

{influencer_text}

8. Grenzen der Auswertung

Die Einschätzungen beruhen auf Heuristiken, Häufigkeiten, Wiederholungsmustern, Triggerbegriffen und einfachen Clustern.
Sie zeigen Auffälligkeiten und Wahrscheinlichkeiten, aber keine sicheren Absichten, Identitäten oder Koordination.
"""
    return report



def basic_alerts_for_ai(comment_df: pd.DataFrame, scores_df: pd.DataFrame, impact: dict) -> list[str]:
    alerts = []
    if comment_df.empty:
        return alerts
    trigger_rate = float(comment_df["has_trigger"].mean()) if len(comment_df) else 0.0
    toxic_rate = float(comment_df["has_toxic_marker"].mean()) if len(comment_df) else 0.0
    q_rate = float(comment_df["is_question"].mean()) if len(comment_df) else 0.0
    user_counts = comment_df.groupby("username").size()
    concentration = float(user_counts.max() / user_counts.sum()) if not user_counts.empty else 0.0
    if trigger_rate >= 0.18:
        alerts.append(f"Triggerquote erhöht ({trigger_rate*100:.1f}%).")
    if toxic_rate >= 0.06:
        alerts.append(f"Abwertende Sprache erhöht ({toxic_rate*100:.1f}%).")
    if concentration >= 0.18 and not user_counts.empty:
        alerts.append(f"Dominanter User: {user_counts.idxmax()} mit etwa {concentration*100:.1f}% Anteil.")
    if not scores_df.empty and (scores_df["shift_score"] >= 45).any():
        top = scores_df.sort_values("shift_score", ascending=False).iloc[0]
        alerts.append(f"Auffälliger Account: {top['username']} (Shift-Score {top['shift_score']}).")
    if impact.get("Diskurskultur", 0) <= -1:
        alerts.append("Diskurskultur unter neutralem Bereich.")
    if q_rate >= 0.30:
        alerts.append(f"Hohe Fragequote ({q_rate*100:.1f}%).")
    return alerts[:6]


def ai_enabled() -> bool:
    return bool(st.session_state.get("ai_enabled", False))


def get_google_api_key() -> str | None:
    try:
        if "GOOGLE_API_KEY" in st.secrets:
            return st.secrets["GOOGLE_API_KEY"]
    except Exception:
        pass
    return os.getenv("GOOGLE_API_KEY")


def build_ai_payload(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame, impact: dict, report_text: str, mode: str = "snapshot") -> dict:
    recent_messages = []
    if not comment_df.empty:
        recent_df = comment_df.sort_values("dt", ascending=False).head(AI_CONTEXT_LIMIT).sort_values("dt")
        for _, row in recent_df.iterrows():
            recent_messages.append({
                "timestamp": str(row["timestamp"]),
                "username": str(row["username"]),
                "text": str(row["text"]),
                "tone": str(row["tone"]),
                "question": bool(row["is_question"]),
                "trigger": bool(row["has_trigger"]),
                "toxic": bool(row["has_toxic_marker"]),
            })

    fairness = {
        "top_user_share": 0.0,
        "top_user": "-",
    }
    if not comment_df.empty:
        counts = comment_df.groupby("username").size().sort_values(ascending=False)
        if len(counts):
            fairness["top_user"] = str(counts.index[0])
            fairness["top_user_share"] = float(counts.iloc[0] / counts.sum())

    payload = {
        "mode": mode,
        "summary": summarize_heuristics(comment_df),
        "impact": impact,
        "alerts": basic_alerts_for_ai(comment_df, scores_df, impact),
        "top_users": top_users(comment_df, n=10).to_dict("records") if not comment_df.empty else [],
        "top_words": top_words(comment_df, n=15).to_dict("records") if not comment_df.empty else [],
        "top_emojis": top_emojis(comment_df, n=12).to_dict("records") if not comment_df.empty else [],
        "clusters": clusters_df.head(8).to_dict("records") if not clusters_df.empty else [],
        "roles": scores_df.head(12).to_dict("records") if not scores_df.empty else [],
        "narratives": narrative_candidates(comment_df),
        "fairness": fairness,
        "report_text": report_text or "",
        "recent_messages": recent_messages,
    }
    return payload


def build_ai_prompt(payload: dict, mode: str = "snapshot") -> str:
    if mode == "endreport":
        goal = (
            "Erstelle einen präzisen Abschlussbericht zu einem TikTok-Live-Chat. "
            "Arbeite strukturiert mit den Abschnitten: Gesamtlage, dominante Narrative, kritische Momente, "
            "auffällige User-Muster, Diskursqualität, Wirkung nach den fünf Wirkungsfeldern, Grenzen der Interpretation, Kurzfazit."
        )
    else:
        goal = (
            "Erstelle einen kompakten KI-Snapshot zum bisherigen TikTok-Live-Chat. "
            "Arbeite mit den Abschnitten: Gesamtlage jetzt, dominante Narrative, auffällige User-Muster, "
            "kritische Momente bisher, Diskursqualität, Kurzfazit."
        )

    rules = (
        "Wichtig: Sei vorsichtig mit Zuschreibungen. "
        "Formuliere Hinweise auf mögliche Manipulation oder Koordination nur als Beobachtung oder Hypothese, nicht als Fakt. "
        "Nutze die gelieferten Heuristiken, Warnungen und Rohbeispiele zusammen. "
        "Antworte auf Deutsch. Keine Tabellen. Keine Markdown-Überschriften mit #. "
        "Lieber klar, präzise und nüchtern als dramatisch."
    )

    return f"{goal}\n\n{rules}\n\nDATENPAKET:\n{json.dumps(payload, ensure_ascii=False)}"


def call_google_ai(prompt: str, model: str | None = None) -> str:
    api_key = get_google_api_key()
    if not api_key:
        raise RuntimeError("Kein GOOGLE_API_KEY gefunden. Bitte als Streamlit Secret oder Umgebungsvariable setzen.")
    model_name = model or AI_DEFAULT_MODEL
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.3,
            "topP": 0.9,
            "maxOutputTokens": 1600,
        }
    }
    resp = requests.post(url, json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        raise RuntimeError(f"Unerwartete Antwort von Google AI Studio: {data}")


def maybe_run_auto_ai(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame, impact: dict, report_text: str):
    mode = st.session_state.get("ai_mode", "Manuell")
    if mode not in {"Nur bei Alarm", "Nur Endreport", "Bei Alarm + Endreport"}:
        return
    if not ai_enabled():
        return
    current_count = len(comment_df)
    last_count = st.session_state.get("ai_last_auto_count", 0)
    if current_count - last_count < AI_MIN_NEW_MESSAGES:
        return
    alerts = basic_alerts_for_ai(comment_df, scores_df, impact)
    if not alerts and mode != "Nur Endreport":
        return
    if mode == "Nur Endreport":
        return
    try:
        payload = build_ai_payload(comment_df, scores_df, clusters_df, impact, report_text, mode="snapshot")
        prompt = build_ai_prompt(payload, mode="snapshot")
        text = call_google_ai(prompt, st.session_state.get("ai_model", AI_DEFAULT_MODEL))
        st.session_state["ai_snapshot_text"] = text
        st.session_state["ai_last_auto_count"] = current_count
        st.session_state["ai_last_run_label"] = f"Auto-Alarm bei {current_count} Nachrichten"
    except Exception as e:
        st.session_state["ai_error"] = str(e)



def queue_message(queue_obj, msg_type: str, username: str, text: str, avatar_url: str | None = None) -> None:
    queue_obj.put({
        "timestamp": now_ts(),
        "type": msg_type,
        "username": username,
        "text": text,
        "avatar_url": avatar_url,
    })



def start_client(board_id: str, username: str, queue_obj):
    try:
        queue_message(queue_obj, "system", "SYSTEM", f"Verbinde zu {username} ...")
        client = TikTokLiveClient(unique_id=username)

        @client.on(ConnectEvent)
        async def on_connect(event):
            queue_message(queue_obj, "system", "SYSTEM", f"Verbunden mit {username}")

        @client.on(CommentEvent)
        async def on_comment(event):
            nickname = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Unbekannt")
            comment = getattr(event, "comment", "")
            avatar_url = safe_avatar_url(getattr(event, "user", None))
            queue_message(queue_obj, "comment", nickname, comment, avatar_url=avatar_url)

        @client.on(DisconnectEvent)
        async def on_disconnect(event):
            queue_message(queue_obj, "system", "SYSTEM", "Verbindung getrennt - Verlauf bleibt erhalten")

        if OPTIONAL_LIVE_EVENTS and LikeEvent is not None:
            @client.on(LikeEvent)
            async def on_like(event):
                count = getattr(event, "count", None) or getattr(event, "like_count", None) or getattr(event, "total", None)
                user = getattr(getattr(event, "user", None), "nickname", None) or getattr(getattr(event, "user", None), "unique_id", "Like")
                txt = f"{user} hat geliked"
                if count is not None:
                    txt += f" ({count})"
                queue_message(queue_obj, "like", user, txt)

        if OPTIONAL_LIVE_EVENTS and JoinEvent is not None:
            @client.on(JoinEvent)
            async def on_join(event):
                user = getattr(getattr(event, "user", None), "nickname", None) or getattr(getattr(event, "user", None), "unique_id", "Join")
                queue_message(queue_obj, "join", user, f"{user} ist dem Live beigetreten")

        if OPTIONAL_LIVE_EVENTS and ShareEvent is not None:
            @client.on(ShareEvent)
            async def on_share(event):
                user = getattr(getattr(event, "user", None), "nickname", None) or getattr(getattr(event, "user", None), "unique_id", "Share")
                queue_message(queue_obj, "share", user, f"{user} hat das Live geteilt")

        if OPTIONAL_LIVE_EVENTS and GiftEvent is not None:
            @client.on(GiftEvent)
            async def on_gift(event):
                user = getattr(getattr(event, "user", None), "nickname", None) or getattr(getattr(event, "user", None), "unique_id", "Gift")
                gift = getattr(event, "gift", None)
                gift_name = None
                if gift is not None:
                    gift_name = getattr(getattr(gift, "extended_gift", None), "name", None) or getattr(gift, "name", None)
                txt = f"{user} hat ein Geschenk gesendet"
                if gift_name:
                    txt += f": {gift_name}"
                queue_message(queue_obj, "gift", user, txt)

        client.run()
    except Exception as e:
        queue_message(queue_obj, "error", "FEHLER", f"{type(e).__name__}: {e}")


def init_state():
    defaults = {
        "chat_queue": queue.Queue(),
        "listener_thread": None,
        "board_id": None,
        "local_report": "",
        "ai_enabled": False,
        "ai_mode": "Manuell",
        "ai_model": AI_DEFAULT_MODEL,
        "ai_snapshot_text": "",
        "ai_endreport_text": "",
        "ai_last_auto_count": 0,
        "ai_last_run_label": "",
        "ai_error": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_db()
init_state()

qp = st.query_params
query_board = qp.get("board")
if isinstance(query_board, list):
    query_board = query_board[0] if query_board else None
if query_board:
    st.session_state.board_id = query_board

st.set_page_config(page_title=APP_TITLE, page_icon="💬", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1.15rem; padding-bottom: 1.15rem; max-width: 1520px; }
    .hero { padding: 1rem 1.15rem; border-radius: 20px; background: linear-gradient(135deg, rgba(59,130,246,.14), rgba(168,85,247,.14)); border: 1px solid rgba(148,163,184,.22); margin-bottom: 1rem; }
    .muted { color: #94a3b8; font-size: 0.9rem; }
    .card { border: 1px solid rgba(148,163,184,.18); border-radius: 18px; padding: 1rem; background: rgba(255,255,255,.02); margin-bottom: 1rem; }
    .chat-item { border: 1px solid rgba(148,163,184,.16); border-radius: 16px; padding: .65rem .8rem .5rem .8rem; margin-bottom: .45rem; background: rgba(255,255,255,.02); }
    .chat-main { line-height: 1.35; word-break: break-word; font-size: 0.96rem; }
    .chat-meta { text-align: right; color: #94a3b8; font-size: 0.75rem; margin-top: .25rem; }
    .pill { display: inline-block; border-radius: 999px; padding: .12rem .45rem; font-size: .72rem; margin-right: .3rem; border: 1px solid rgba(148,163,184,.22); }
    .pill-trigger { background: rgba(245,158,11,.13); border-color: rgba(245,158,11,.28); }
    .pill-toxic { background: rgba(244,63,94,.12); border-color: rgba(244,63,94,.28); }
    .pill-question { background: rgba(59,130,246,.12); border-color: rgba(59,130,246,.28); }
    .score-card {
        border-radius: 18px;
        padding: .85rem .95rem;
        border: 1px solid rgba(148,163,184,.18);
        background: rgba(255,255,255,.02);
        min-height: 172px;
    }
    .score-num { font-size: 2rem; font-weight: 800; line-height: 1; margin-top: .35rem; margin-bottom: .25rem; }
    .score-sub { color: #94a3b8; font-size: .85rem; }
    .score-arrow { font-size: 1.05rem; font-weight: 700; margin-left: .2rem; }
    .avatar-fallback { width: 44px; height: 44px; border-radius: 999px; display:flex; align-items:center; justify-content:center; font-size:.82rem; font-weight:700; color:white; }
    .report-box { white-space: pre-wrap; line-height: 1.55; }
    .sticky-panel { position: sticky; top: 0.75rem; max-height: calc(100vh - 1.5rem); overflow-y: auto; padding-right: 0.2rem; }
    .ampel-card { border-radius: 18px; padding: 0.9rem 1rem; border: 1px solid rgba(148,163,184,.18); background: rgba(255,255,255,.02); margin-bottom: 0.8rem; }
    .alert-card { border-radius: 14px; padding: 0.65rem 0.8rem; margin-bottom: 0.5rem; border: 1px solid rgba(148,163,184,.18); }
    .heat-neutral { border-left: 5px solid #cbd5e1; background: rgba(255,255,255,.02); }
    .heat-question { border-left: 5px solid #60a5fa; background: rgba(96,165,250,.05); }
    .heat-trigger { border-left: 5px solid #f59e0b; background: rgba(245,158,11,.06); }
    .heat-toxic { border-left: 5px solid #ef4444; background: rgba(239,68,68,.06); }
    .heat-repeat { border-left: 5px solid #a855f7; background: rgba(168,85,247,.06); }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="hero">
    <h1 style="margin:0 0 .35rem 0;">💬 {APP_TITLE}</h1>
    <div class="muted">
        Kostenloses Shared Dashboard mit Board-ID. Datenstand gemeinsam, Filter persönlich pro Session.
    </div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Dashboard teilen")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Neues Dashboard", use_container_width=True):
            new_board = create_board()
            st.session_state.board_id = new_board
            st.query_params["board"] = new_board
            st.rerun()
    with c2:
        join_board = st.text_input("Board-ID", value=st.session_state.board_id or "", label_visibility="collapsed", placeholder="Board-ID")
        if st.button("Beitreten", use_container_width=True) and join_board:
            st.session_state.board_id = join_board.strip().lower()
            st.query_params["board"] = st.session_state.board_id
            st.rerun()

    board_id = st.session_state.board_id
    share_url = f"{APP_BASE_URL}?board={board_id}" if board_id else APP_BASE_URL
    st.text_input("Share-URL", value=share_url, help="Diese URL teilen. Alle sehen denselben Datenstand.")
    st.caption("Filter, Suche und persönliche Ansichten bleiben lokal pro Nutzerin bzw. Nutzer.")

    st.divider()
    st.header("Live starten")
    username_input = st.text_input("TikTok Username", placeholder="@username")
    if st.button("▶ Mitschnitt für dieses Dashboard starten", use_container_width=True, disabled=not board_id):
        try:
            if not board_id:
                raise ValueError("Bitte zuerst ein Dashboard erstellen oder beitreten.")
            username = normalize_username(username_input)
            board = get_board(board_id)
            if not board:
                raise ValueError("Board nicht gefunden.")
            update_board(
                board_id,
                host_username=username,
                started_at=now_dt().isoformat(),
                status="running"
            )
            st.session_state.chat_queue = queue.Queue()
            insert_message(board_id, {
                "timestamp": now_ts(),
                "type": "system",
                "username": "SYSTEM",
                "text": f"Mitschnitt gestartet für {username}",
                "avatar_url": None
            })
            thread = threading.Thread(
                target=start_client,
                args=(board_id, username, st.session_state.chat_queue),
                daemon=True
            )
            thread.start()
            st.session_state.listener_thread = thread
            st.success(f"Listener gestartet für {username}")
        except Exception as e:
            st.error(str(e))

    if st.button("📝 Gemeinsamen Report erstellen", use_container_width=True, disabled=not board_id):
        board = get_board(board_id) if board_id else None
        messages = load_messages(board_id) if board_id else []
        comment_df = build_dataframe(get_comment_messages(messages))
        scores_df = user_scores(comment_df)
        clusters_df = build_clusters(comment_df, max_clusters=8)
        impact = impact_scores(comment_df, scores_df, clusters_df)
        report = generate_rule_based_report(comment_df, scores_df, clusters_df, impact)
        update_board(board_id, report_text=report)
        st.success("Report im Dashboard gespeichert.")

    st.divider()
    st.subheader("KI-Auswertung")
    st.toggle("KI aktiv", key="ai_enabled", help="Kosteneffiziente Strategie: Heuristik live, KI nur gezielt.")
    st.selectbox("KI-Modus", ["Manuell", "Nur bei Alarm", "Nur Endreport", "Bei Alarm + Endreport"], key="ai_mode", help="Empfehlung: Nur bei Alarm oder manuell.")
    st.text_input("KI-Modell", key="ai_model", help="Google AI Studio Modellname, z. B. gemini-2.0-flash.")
    st.caption("Am sinnvollsten: Snapshot auf Abruf, Endreport am Ende und Auto-KI nur bei Alarm.")
    if st.session_state.get("ai_enabled") and not get_google_api_key():
        st.warning("Kein GOOGLE_API_KEY gefunden. Bitte als Secret oder Umgebungsvariable setzen.")
    if st.session_state.get("ai_error"):
        st.error(st.session_state.get("ai_error"))

    st.divider()
    st.subheader("Persönliche Filter")
    search_text = st.text_input("Suche", placeholder="z. B. Merz")
    tone_filter = st.selectbox("Tonlage", ["Alle", "neutral", "fragend", "polarisierend", "abwertend"], help="Heuristische Einordnung pro Nachricht. 'Polarisierend' basiert vor allem auf Triggerbegriffen, 'abwertend' auf beleidigenden oder aggressiven Markern.")
    only_questions = st.checkbox("Nur Fragen")
    only_triggers = st.checkbox("Nur Trigger")
    only_toxic = st.checkbox("Nur abwertend/toxisch")

if board_id:
    st_autorefresh(interval=AUTO_REFRESH_MS, key="board_refresh")

while not st.session_state.chat_queue.empty():
    msg = st.session_state.chat_queue.get()
    if isinstance(msg, dict) and board_id:
        insert_message(board_id, msg)

board = get_board(board_id) if board_id else None
messages = load_messages(board_id) if board_id else []
all_messages = clean_message_store(messages)
comment_messages = get_comment_messages(all_messages)
comment_df = build_dataframe(comment_messages)

all_users = ["Alle"] + sorted(comment_df["username"].dropna().unique().tolist()) if not comment_df.empty else ["Alle"]
with st.sidebar:
    user_filter = st.selectbox("User", all_users)

filters = {
    "search": search_text,
    "user": user_filter,
    "tone": tone_filter,
    "only_questions": only_questions,
    "only_triggers": only_triggers,
    "only_toxic": only_toxic,
}
filtered_df = filtered_comment_df(comment_df, filters)
summary = summarize_heuristics(comment_df)
scores_df = user_scores(comment_df)
clusters_df = build_clusters(comment_df, max_clusters=8)
impact = impact_scores(comment_df, scores_df, clusters_df)
impact_explanations = explain_impact_scores(comment_df, scores_df, clusters_df, impact)
roles = role_summary(scores_df)


event_df = event_overview(all_messages)
repeat_df_global = repeated_messages(comment_df, min_count=2)
live_ampel = compute_live_ampel(comment_df, scores_df, impact)
alerts = compute_alerts(comment_df, scores_df, impact)
drift_df = narrative_drift(comment_df)
mention_df = mention_edges(comment_df)
influencer_df = influencer_map(comment_df)
greeting_df = greeting_edges(comment_df)
critical_df = critical_moments(comment_df)
fairness = fairness_metrics(comment_df)
trigger_df = trigger_effect_analysis(comment_df)
archetype_df = user_archetypes(comment_df, scores_df)
attention_df = attention_vs_substance(comment_df)
phase_label = phase_of_live(comment_df)

if not board_id:
    st.info("Erstelle links ein neues Dashboard oder tritt einem bestehenden Board bei.")
    st.stop()

host = board["host_username"] if board else None
started_at = board["started_at"] if board else None

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Nachrichten", summary["messages"])
k2.metric("User", summary["users"])
k3.metric("Fragen", summary["questions"], help="Anzahl erkannter Fragen im Chat. Kann echte Verständnisfragen, rhetorische Fragen oder themenlenkende Fragen enthalten.")
k4.metric("Trigger", summary["trigger_msgs"], help=GLOBAL_TOOLTIPS["trigger"])
k5.metric("Abwertend", summary["toxic_msgs"], help=GLOBAL_TOOLTIPS["toxisch"])
k6.metric("Laufzeit", elapsed_label(started_at))

meta1, meta2, meta3 = st.columns(3)
meta1.info(f"Board: {board_id}")
meta2.info(f"Host: {host or '-'}")
meta3.info(f"Status: {board['status'] if board else '-'}")

explain_mode = st.toggle(
    "Explain Mode für Wirkungsfelder",
    value=False,
    help="Zeigt unter jedem Wirkungsfeld eine kurze Begründung, warum der aktuelle Wert zustande kommt."
)


left, right = st.columns([1.45, 0.95], gap="large")

with left:
    st.subheader("Live-Feed")
    i1, i2, i3, i4 = st.columns(4)
    i1.info(f"Sichtbar: {min(len(filtered_df), DISPLAY_LIMIT)} - neueste oben")
    i2.info(f"Gesamt: {len(comment_df)}")
    i3.info(f"Dein Filter-User: {user_filter}")
    i4.info(f"Phase: {phase_label}")

    mention_repeat_users = set()
    if not repeat_df_global.empty:
        mention_repeat_users = set(repeat_df_global["username"].astype(str).tolist())

    if not filtered_df.empty:
        render_df = filtered_df.sort_values("dt", ascending=False).head(DISPLAY_LIMIT)
        for _, row in render_df.iterrows():
            badges = []
            if row["is_question"]:
                badges.append('<span class="pill pill-question">Frage</span>')
            if row["has_trigger"]:
                badges.append('<span class="pill pill-trigger">Trigger</span>')
            if row["has_toxic_marker"]:
                badges.append('<span class="pill pill-toxic">Abwertend</span>')

            heat_class = "heat-neutral"
            if row["username"] in mention_repeat_users:
                heat_class = "heat-repeat"
            if row["is_question"]:
                heat_class = "heat-question"
            if row["has_trigger"]:
                heat_class = "heat-trigger"
            if row["has_toxic_marker"]:
                heat_class = "heat-toxic"

            badge_html = "".join(badges)
            username_col = user_color(row["username"])
            ts = row["dt"].strftime("%H:%M:%S") if pd.notna(row["dt"]) else "--:--:--"

            avatar_col, content_col = st.columns([0.09, 0.91], gap="small")
            with avatar_col:
                if row.get("avatar_url"):
                    st.image(row["avatar_url"], width=42)
                else:
                    st.markdown(
                        f'<div class="avatar-fallback" style="background:{username_col};">{initials(row["username"])}</div>',
                        unsafe_allow_html=True
                    )
            with content_col:
                st.markdown(
                    f"""
                    <div class="chat-item {heat_class}">
                        <div class="chat-main">
                            <span style="color:{username_col}; font-weight:700;">{row['username']}</span>: {row['text']}
                        </div>
                        <div style="margin-top:.25rem;">{badge_html}</div>
                        <div class="chat-meta">{ts}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
    else:
        st.info("Noch keine passenden Chatnachrichten. Falls das Live aktiv ist, warte ein paar Sekunden.")

    system_rows = [m for m in all_messages if isinstance(m, dict) and m.get("type") in {"system", "error"}]
    if system_rows:
        with st.expander("Systemmeldungen", expanded=False):
            for row in system_rows[-50:]:
                st.markdown(f"**{row['username']}**: {row['text']}  \n`{row['timestamp'][11:19]}`")

with right:
    st.markdown('<div class="sticky-panel">', unsafe_allow_html=True)

    st.subheader("Live-Lage")
    st.markdown(
        f"""
        <div class="ampel-card" style="border-left: 8px solid {live_ampel['color']};">
            <div style="font-size:0.86rem; color:#94a3b8;">Live-Ampel</div>
            <div style="font-size:2rem; font-weight:800; color:{live_ampel['color']}; line-height:1.1;">{live_ampel['score']} {live_ampel['trend']}</div>
            <div style="font-size:1rem; font-weight:700; color:{live_ampel['color']};">{live_ampel['label'].capitalize()} - {live_ampel['ampel']}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.caption("Die Live-Ampel fasst Diskurskultur, Salienz, Dominanz einzelner User, Wiederholungen, Trigger und Toxizität zu einer Gesamtlage zusammen.")

    st.subheader("Warnungen")
    color_map = {"green": "#16a34a", "yellow": "#eab308", "orange": "#f97316", "red": "#ef4444"}
    for alert in alerts:
        c = color_map.get(alert["level"], "#94a3b8")
        st.markdown(
            f"""
            <div class="alert-card" style="border-left:5px solid {c};">
                <div style="font-weight:700; color:{c}; margin-bottom:.15rem;">{alert['title']}</div>
                <div style="font-size:.88rem; color:#475569;">{alert['detail']}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.subheader("Wirkungsfelder")
    for name, val in impact.items():
        color = score_color(val)
        arrow = score_arrow(val)
        ampel = "grün" if val >= 1 else "gelb" if val == 0 else "rot"

        st.metric(
            label=name,
            value=f"{val} {arrow}",
            help=SCORE_TOOLTIPS.get(name, "")
        )
        st.markdown(
            f"""
            <div class="ampel-card" style="border-left: 6px solid {color}; padding:.55rem .8rem; margin-top:-.35rem;">
                <div style="color:#94a3b8; font-size:.83rem;">{score_label(val)}</div>
                <div style="color:#94a3b8; font-size:.83rem;">Ampel: {ampel}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        if explain_mode:
            st.caption(impact_explanations.get(name, ""))

    st.subheader("Dynamik")
    activity_df = activity_per_minute(comment_df)
    if not activity_df.empty:
        st.altair_chart(
            alt.Chart(activity_df).mark_line(point=True).encode(
                x=alt.X("minute:T", title="Zeit"),
                y=alt.Y("messages:Q", title="Msgs/Min"),
                tooltip=["minute:T", "messages:Q"]
            ).properties(height=180),
            use_container_width=True
        )
    else:
        st.info("Noch keine Zeitreihe vorhanden.")

    st.info(salience_warning(comment_df, scores_df), icon="ℹ️")

    st.subheader("Top-Wörter und Emojis")
    c1, c2 = st.columns(2)
    words_df = top_words(filtered_df if not filtered_df.empty else comment_df, n=10)
    emojis_df = top_emojis(filtered_df if not filtered_df.empty else comment_df, n=10)
    with c1:
        st.dataframe(words_df if not words_df.empty else pd.DataFrame(columns=["word", "count"]), use_container_width=True, hide_index=True, height=250)
    with c2:
        st.dataframe(emojis_df if not emojis_df.empty else pd.DataFrame(columns=["emoji", "count"]), use_container_width=True, hide_index=True, height=250)

    st.subheader("Engagement / Events")
    if not event_df.empty:
        st.dataframe(event_df, use_container_width=True, hide_index=True, height=170)
    else:
        st.caption("Zusätzliche Live-Events wie Likes, Gifts, Joins oder Shares werden nur angezeigt, wenn die Bibliothek sie im Stream liefert.")

    st.subheader("Narrative Drift")
    if not drift_df.empty:
        drift_show = drift_df.copy()
        drift_show["bucket"] = pd.to_datetime(drift_show["bucket"]).dt.strftime("%H:%M")
        st.dataframe(drift_show.tail(12), use_container_width=True, hide_index=True, height=250)
    else:
        st.info("Noch keine Narrative-Drift verfügbar.")

    st.subheader("User-Detailpanel")
    detail_options = ["-"] + (top_users(comment_df, n=20)["username"].tolist() if not comment_df.empty else [])
    selected_detail_user = st.selectbox("User-Analyse", detail_options, key="detail_user_select")
    if selected_detail_user != "-":
        snap = user_detail_snapshot(comment_df, selected_detail_user)
        if snap:
            d1, d2 = st.columns(2)
            d1.metric("Msgs", snap["messages"])
            d2.metric("Repeats", snap["repeat_count"])
            d3, d4 = st.columns(2)
            d3.metric("Trigger", f"{snap['trigger_rate']*100:.0f}%")
            d4.metric("Fragen", f"{snap['question_rate']*100:.0f}%")
            st.caption(f"Erste Aktivität: {snap['first_seen']} | Letzte Aktivität: {snap['last_seen']}")
            st.write("Letzte Nachrichten")
            recent_df = pd.DataFrame(snap["recent_messages"])
            st.dataframe(recent_df if not recent_df.empty else pd.DataFrame(columns=["timestamp","text"]), use_container_width=True, hide_index=True, height=220)

    st.subheader("Aktivste User")
    top_users_df = top_users(comment_df)
    if not top_users_df.empty:
        st.altair_chart(
            alt.Chart(top_users_df).mark_bar().encode(
                x=alt.X("messages:Q", title="Nachrichten"),
                y=alt.Y("username:N", sort="-x", title="User"),
                tooltip=["username", "messages"]
            ).properties(height=280),
            use_container_width=True
        )
    else:
        st.info("Noch keine User-Daten.")

    st.subheader("Auffällige User / Diskursverschiebung", help=GLOBAL_TOOLTIPS["shift_score"])
    if not scores_df.empty:
        st.dataframe(scores_df.head(20), use_container_width=True, hide_index=True, height=280)
    else:
        st.info("Noch keine User-Scores verfügbar.")

    st.subheader("Wiederholungen / mögliche Spam-Muster", help=GLOBAL_TOOLTIPS["wiederholungen"])
    if not repeat_df_global.empty:
        st.dataframe(repeat_df_global, use_container_width=True, hide_index=True, height=240)
    else:
        st.info("Bisher keine auffälligen Wiederholungen erkannt.")

    st.subheader("Themencluster", help=GLOBAL_TOOLTIPS["cluster"])
    if not clusters_df.empty:
        st.dataframe(clusters_df, use_container_width=True, hide_index=True, height=240)
    else:
        st.info("Für Themencluster werden mehr Chatdaten benötigt.")

    st.subheader("Rollenbild")
    if roles:
        role_df = pd.DataFrame([{"Rolle": k, "Anzahl": v} for k, v in roles.items()])
        st.dataframe(role_df, use_container_width=True, hide_index=True, height=190)
    else:
        st.info("Noch keine Rollenverteilung verfügbar.")
    st.caption(GLOBAL_TOOLTIPS["rollen"])

    st.subheader("Narrative")
    narratives = narrative_candidates(comment_df)
    if narratives:
        for item in narratives:
            st.write(f"- {item}")
    else:
        st.info("Noch keine stabilen Narrative erkannt.")
    st.caption(GLOBAL_TOOLTIPS["narrative"])

    st.subheader("Influencer-Map")
    if not influencer_df.empty:
        st.dataframe(influencer_df.head(20), use_container_width=True, hide_index=True, height=260)
    else:
        st.info("Noch keine Influencer-Struktur erkennbar.")
    st.caption("Die Influencer-Map basiert auf @-Erwähnungen. Sie zeigt, wer eher adressiert wird, wer andere aktiv anspricht und wer als Hub, Verstärker oder Initiator wirkt.")

    st.subheader("Influence / Mention Map")
    if not mention_df.empty:
        st.dataframe(mention_df.head(20), use_container_width=True, hide_index=True, height=220)
    else:
        st.info("Noch keine Erwähnungsbeziehungen erkannt.")
    st.caption("Diese Tabelle zeigt die eigentlichen Kanten: Wer erwähnt wen wie oft.")

    st.subheader("Begrüßungen / direkte Ansprache")
    if not greeting_df.empty:
        st.dataframe(greeting_df.head(20), use_container_width=True, hide_index=True, height=180)
    else:
        st.caption("Noch keine klaren Begrüßungen mit @-Ansprache erkannt.")

    st.subheader("Phase 4 - Fairness & Dominanz")
    f1, f2 = st.columns(2)
    f1.metric("Top 1 Anteil", f"{fairness['top1_share']*100:.1f}%")
    f2.metric("Top 3 Anteil", f"{fairness['top3_share']*100:.1f}%")
    f3, f4 = st.columns(2)
    f3.metric("Gini", f"{fairness['gini']:.2f}")
    f4.metric("Dominant", fairness["dominant_user"])

    st.subheader("Phase 4 - Kritische Momente")
    if not critical_df.empty:
        critical_show = critical_df.copy()
        critical_show["bucket"] = pd.to_datetime(critical_show["bucket"]).dt.strftime("%H:%M")
        st.dataframe(critical_show.tail(12), use_container_width=True, hide_index=True, height=240)
    else:
        st.info("Noch keine kritischen Momente berechenbar.")

    st.subheader("Phase 4 - Trigger-Wirkung")
    if not trigger_df.empty:
        trigger_show = trigger_df.copy()
        trigger_show["share"] = (trigger_show["share"] * 100).round(1)
        trigger_show["question_rate"] = (trigger_show["question_rate"] * 100).round(1)
        trigger_show["toxic_rate"] = (trigger_show["toxic_rate"] * 100).round(1)
        st.dataframe(trigger_show.head(12), use_container_width=True, hide_index=True, height=260)
    else:
        st.info("Noch keine Trigger-Wirkung auswertbar.")

    st.subheader("Phase 4 - User-Archetypen")
    if not archetype_df.empty:
        st.dataframe(archetype_df.head(20), use_container_width=True, hide_index=True, height=240)
    else:
        st.info("Noch keine Archetypen bestimmbar.")

    st.subheader("Phase 4 - Aufmerksamkeit vs Substanz")
    if not attention_df.empty:
        attention_show = attention_df.copy()
        attention_show["attention_share"] = (attention_show["attention_share"] * 100).round(1)
        attention_show["substance_score"] = (attention_show["substance_score"] * 100).round(1)
        attention_show["attention_minus_substance"] = attention_show["attention_minus_substance"].round(2)
        st.dataframe(attention_show.head(15), use_container_width=True, hide_index=True, height=250)
    else:
        st.info("Noch keine Aufmerksamkeit-Substanz-Analyse verfügbar.")

    st.markdown('</div>', unsafe_allow_html=True)




st.subheader("Gemeinsamer Report", help=GLOBAL_TOOLTIPS["report"])
report_text = board.get("report_text", "") if board else ""
maybe_run_auto_ai(comment_df, scores_df, clusters_df, impact, report_text)

ai_col1, ai_col2 = st.columns(2)
with ai_col1:
    if st.button("🧠 KI-Snapshot jetzt", use_container_width=True, disabled=(not ai_enabled() or not bool(all_messages))):
        try:
            st.session_state["ai_error"] = ""
            payload = build_ai_payload(comment_df, scores_df, clusters_df, impact, report_text, mode="snapshot")
            prompt = build_ai_prompt(payload, mode="snapshot")
            st.session_state["ai_snapshot_text"] = call_google_ai(prompt, st.session_state.get("ai_model", AI_DEFAULT_MODEL))
            st.session_state["ai_last_run_label"] = f"Manueller Snapshot bei {len(comment_df)} Nachrichten"
            st.success("KI-Snapshot erstellt.")
        except Exception as e:
            st.session_state["ai_error"] = str(e)
            st.error(str(e))

with ai_col2:
    if st.button("🧾 KI-Endreport jetzt", use_container_width=True, disabled=(not ai_enabled() or not bool(all_messages))):
        try:
            st.session_state["ai_error"] = ""
            payload = build_ai_payload(comment_df, scores_df, clusters_df, impact, report_text, mode="endreport")
            prompt = build_ai_prompt(payload, mode="endreport")
            st.session_state["ai_endreport_text"] = call_google_ai(prompt, st.session_state.get("ai_model", AI_DEFAULT_MODEL))
            st.session_state["ai_last_run_label"] = f"Manueller Endreport bei {len(comment_df)} Nachrichten"
            st.success("KI-Endreport erstellt.")
        except Exception as e:
            st.session_state["ai_error"] = str(e)
            st.error(str(e))

if report_text:
    st.markdown(f'<div class="report-box">{report_text}</div>', unsafe_allow_html=True)
else:
    st.info("Noch kein gemeinsamer Report erstellt. Nutze links den Button 'Gemeinsamen Report erstellen'.")

if st.session_state.get("ai_last_run_label"):
    st.caption(f"Letzte KI-Auswertung: {st.session_state['ai_last_run_label']}")

if st.session_state.get("ai_snapshot_text"):
    st.subheader("KI-Snapshot")
    st.markdown(f'<div class="report-box">{st.session_state["ai_snapshot_text"]}</div>', unsafe_allow_html=True)

if st.session_state.get("ai_endreport_text"):
    st.subheader("KI-Endreport")
    st.markdown(f'<div class="report-box">{st.session_state["ai_endreport_text"]}</div>', unsafe_allow_html=True)

st.subheader("Phase 4 - Deep Dive")
dt1, dt2, dt3 = st.tabs(["Kritische Momente", "Dominanz & Fairness", "Trigger & Archetypen"])
with dt1:
    if not critical_df.empty:
        chart_df = critical_df.copy()
        st.altair_chart(
            alt.Chart(chart_df).mark_line(point=True).encode(
                x=alt.X("bucket:T", title="Zeit"),
                y=alt.Y("escalation_score:Q", title="Eskalations-Score"),
                tooltip=["bucket:T", "messages:Q", "trigger_rate:Q", "toxic_rate:Q", "dominance:Q", "escalation_score:Q", "signal:N"]
            ).properties(height=300),
            use_container_width=True,
        )
        st.dataframe(chart_df.tail(20), use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Zeitfenster-Daten für kritische Momente.")
with dt2:
    fcols = st.columns(4)
    fcols[0].metric("Top 1 Anteil", f"{fairness['top1_share']*100:.1f}%")
    fcols[1].metric("Top 3 Anteil", f"{fairness['top3_share']*100:.1f}%")
    fcols[2].metric("Gini", f"{fairness['gini']:.2f}")
    fcols[3].metric("Dominanter User", fairness['dominant_user'])
    if not attention_df.empty:
        st.dataframe(attention_df.head(25), use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Fairness- oder Aufmerksamkeitsanalyse verfügbar.")
with dt3:
    c_left, c_right = st.columns(2)
    with c_left:
        if not trigger_df.empty:
            st.dataframe(trigger_df.head(20), use_container_width=True, hide_index=True)
        else:
            st.info("Noch keine Trigger-Wirkungsanalyse verfügbar.")
    with c_right:
        if not archetype_df.empty:
            st.dataframe(archetype_df.head(25), use_container_width=True, hide_index=True)
        else:
            st.info("Noch keine User-Archetypen verfügbar.")

st.caption("Shared Dashboard: Datenstand gemeinsam, Filter persönlich. Nur die Basisdaten, Scores und Reports werden über das Board geteilt.")


