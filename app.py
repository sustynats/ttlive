
import json
import math
import queue
import re
import threading
import hashlib
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
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)

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

IMPACT_HELP = {
    "Diskurskultur": "Wie respektvoll und ausgewogen der Chat ist. Toxische Sprache, Capslock und Dominanz einzelner Stimmen drücken den Wert.",
    "Salienz-Bewusstsein": "Ob Aufmerksamkeit durch wenige laute Trigger-Kommentare verzerrt wird oder relativ ausgewogen verteilt bleibt.",
    "Verantwortung & Macht": "Wie stark einzelne Accounts oder aggressive Muster den Raum dominieren. Hohe Trigger- und Auffälligkeitswerte senken den Score.",
    "Systemischer Impact": "Ob sich Polarisierung, enge Narrative und Themenverengung zeigen oder eher Differenzierung und Breite.",
    "Emotionale Resonanz": "Wie sich der Chat emotional anfühlt. Zu viel Aggression senkt den Wert, konstruktive Beteiligung stabilisiert ihn."
}

def now_dt() -> datetime:
    return datetime.now(TZ)

def now_ts() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")

def normalize_username(username: str) -> str:
    username = username.strip()
    if not username:
        raise ValueError("Bitte einen TikTok-Usernamen eingeben.")
    if not username.startswith("@"):
        username = "@" + username
    return username

def safe_slug(text: str) -> str:
    text = text.replace("@", "").strip()
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", text) or "chat"

def make_export_base(username: str) -> str:
    return f"{safe_slug(username)}_{now_dt().strftime('%Y%m%d_%H%M%S')}"

def append_txt(filepath: Path, line: str) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def append_jsonl(filepath: Path, payload: dict) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

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
                0.30 * volume_factor +
                0.24 * trigger_ratio +
                0.18 * repeat_ratio +
                0.16 * question_ratio +
                0.07 * caps_ratio +
                0.05 * toxic_ratio
            ),
            1
        )
        if shift_score >= 65:
            role = "stark auffällig"
        elif shift_score >= 45:
            role = "auffällig"
        elif trigger_ratio >= 0.40:
            role = "Narrativ-Verstärker"
        elif question_ratio >= 0.50 and total >= 4:
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

def impact_scores(comment_df: pd.DataFrame, scores_df: pd.DataFrame, clusters_df: pd.DataFrame) -> dict:
    if comment_df.empty:
        return {name: 0 for name in IMPACT_HELP}
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
    raw = {
        "Diskurskultur": (
            0.38 * (1 - toxic) +
            0.18 * (1 - caps) +
            0.22 * (1 - concentration) +
            0.10 * min(questions + 0.15, 1.0) +
            0.12 * min(avg_len + 0.10, 1.0)
        ),
        "Salienz-Bewusstsein": (
            0.38 * (1 - trigger) +
            0.24 * (1 - concentration) +
            0.24 * (1 - cluster_top) +
            0.14 * (1 - severe_ratio)
        ),
        "Verantwortung & Macht": (
            0.34 * (1 - flagged_ratio) +
            0.26 * (1 - severe_ratio) +
            0.22 * (1 - toxic) +
            0.18 * (1 - concentration)
        ),
        "Systemischer Impact": (
            0.34 * (1 - trigger) +
            0.24 * (1 - cluster_top) +
            0.22 * (1 - flagged_ratio) +
            0.20 * min(avg_len + 0.15, 1.0)
        ),
        "Emotionale Resonanz": (
            0.35 * (1 - toxic) +
            0.18 * (1 - caps) +
            0.18 * min(emoji_balance + 0.15, 1.0) +
            0.14 * min(avg_len + 0.10, 1.0) +
            0.15 * (1 - concentration)
        ),
    }
    return {k: calibrate_band(max(0.0, min(v, 1.0))) for k, v in raw.items()}

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
    if scores_df.empty:
        return {}
    return scores_df["role"].value_counts().to_dict()

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
    impact_text = "\n".join([f"- {k}: {v} ({score_label(v)})" for k, v in impact.items()])
    top_words_text = ", ".join(top_words_df["word"].head(10).tolist()) if not top_words_df.empty else "-"
    top_users_text = ", ".join([f"{r['username']} ({int(r['messages'])})" for _, r in top_users_df.head(8).iterrows()]) if not top_users_df.empty else "-"
    question_rate = (comment_df["is_question"].mean() * 100) if not comment_df.empty else 0
    trigger_rate = (comment_df["has_trigger"].mean() * 100) if not comment_df.empty else 0
    toxic_rate = (comment_df["has_toxic_marker"].mean() * 100) if not comment_df.empty else 0
    return f"""1. Kurzfazit

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

7. Grenzen der Auswertung

Die Einschätzungen beruhen auf Heuristiken, Häufigkeiten, Wiederholungsmustern, Triggerbegriffen und einfachen Clustern.
Sie zeigen Auffälligkeiten und Wahrscheinlichkeiten, aber keine sicheren Absichten, Identitäten oder Koordination.
"""

def persist_message(payload: dict) -> None:
    st.session_state.messages.append(payload)
    append_txt(st.session_state.export_txt_path, render_message_text(payload))
    append_jsonl(st.session_state.export_jsonl_path, payload)

def queue_message(queue_obj, msg_type: str, username: str, text: str, avatar_url: str | None = None) -> None:
    queue_obj.put({
        "timestamp": now_ts(),
        "type": msg_type,
        "username": username,
        "text": text,
        "avatar_url": avatar_url,
    })

def start_client(username: str, queue_obj):
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

        client.run()
    except Exception as e:
        queue_message(queue_obj, "error", "FEHLER", f"{type(e).__name__}: {e}")

def init_state():
    defaults = {
        "chat_queue": queue.Queue(),
        "messages": [],
        "listener_thread": None,
        "started": False,
        "username": "",
        "export_base": "",
        "export_txt_path": EXPORT_DIR / "chat_placeholder.txt",
        "export_jsonl_path": EXPORT_DIR / "chat_placeholder.jsonl",
        "capture_started_at": None,
        "auto_report": "",
        "board_id": "gzh9anu",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_state()
st.session_state.messages = clean_message_store(st.session_state.messages)

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
    .score-big {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1;
        margin-top: .35rem;
        margin-bottom: .25rem;
    }
    .score-sub {
        color: #94a3b8;
        font-size: .85rem;
    }
    .score-arrow {
        font-size: 1.05rem;
        font-weight: 700;
        margin-left: .2rem;
    }
    .avatar-fallback { width: 44px; height: 44px; border-radius: 999px; display:flex; align-items:center; justify-content:center; font-size:.82rem; font-weight:700; color:white; }
    .report-box { white-space: pre-wrap; line-height: 1.55; }
    .status-box {
        background: rgba(96,165,250,.12);
        border: 1px solid rgba(96,165,250,.18);
        border-radius: 16px;
        padding: .75rem .9rem;
        font-size: 0.95rem;
        margin-bottom: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="hero">
    <h1 style="margin:0 0 .35rem 0;">💬 {APP_TITLE}</h1>
    <div class="muted">
        Kostenlose Version 2 mit Impact-Kompass, konservativer Scorecard,
        Salienz-Hinweisen, Rollenbildern, Sharing-/Status-Infos und Best-Effort-Profilbildern.
    </div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Steuerung")
    username_input = st.text_input("TikTok Username", value=st.session_state.username, placeholder="@username")
    c1, c2 = st.columns(2)
    with c1:
        start_clicked = st.button("▶ Starten", use_container_width=True)
    with c2:
        new_capture = st.button("🧹 Reset", use_container_width=True)

    if start_clicked:
        try:
            username = normalize_username(username_input)
            st.session_state.username = username
            if not st.session_state.export_base:
                st.session_state.export_base = make_export_base(username)
                st.session_state.export_txt_path = EXPORT_DIR / f"{st.session_state.export_base}.txt"
                st.session_state.export_jsonl_path = EXPORT_DIR / f"{st.session_state.export_base}.jsonl"
                st.session_state.capture_started_at = now_dt().isoformat()
            st.session_state.chat_queue = queue.Queue()
            persist_message({
                "timestamp": now_ts(),
                "type": "system",
                "username": "SYSTEM",
                "text": f"Mitschnitt gestartet für {username}",
                "avatar_url": None,
            })
            thread = threading.Thread(
                target=start_client,
                args=(username, st.session_state.chat_queue),
                daemon=True
            )
            thread.start()
            st.session_state.listener_thread = thread
            st.session_state.started = True
            st.success(f"Listener gestartet für {username}")
        except Exception as e:
            st.error(str(e))

    if new_capture:
        st.session_state.chat_queue = queue.Queue()
        st.session_state.messages = []
        st.session_state.listener_thread = None
        st.session_state.started = False
        st.session_state.username = ""
        st.session_state.export_base = ""
        st.session_state.export_txt_path = EXPORT_DIR / "chat_placeholder.txt"
        st.session_state.export_jsonl_path = EXPORT_DIR / "chat_placeholder.jsonl"
        st.session_state.capture_started_at = None
        st.session_state.auto_report = ""
        st.success("Session zurückgesetzt.")

    st.divider()
    st.subheader("Filter")
    search_text = st.text_input("Suche", placeholder="z. B. Merz")
    tone_filter = st.selectbox("Tonlage", ["Alle", "neutral", "fragend", "polarisierend", "abwertend"])
    only_questions = st.checkbox("Nur Fragen")
    only_triggers = st.checkbox("Nur Trigger")
    only_toxic = st.checkbox("Nur abwertend/toxisch")

    sidebar_comment_df = build_dataframe(get_comment_messages(st.session_state.messages))
    all_users = ["Alle"] + sorted(sidebar_comment_df["username"].dropna().unique().tolist()) if not sidebar_comment_df.empty else ["Alle"]
    user_filter = st.selectbox("User", all_users)

    st.divider()
    st.subheader("Auto-Report")
    run_report = st.button("📝 Report erstellen", use_container_width=True)

    st.divider()
    st.subheader("Export & Sharing")
    if st.session_state.messages:
        export_name = st.session_state.export_base or "chat_export"
        st.download_button("TXT herunterladen", data=messages_to_txt(st.session_state.messages), file_name=f"{export_name}.txt", mime="text/plain", use_container_width=True)
        st.download_button("CSV herunterladen", data=messages_to_csv_bytes(st.session_state.messages), file_name=f"{export_name}.csv", mime="text/csv", use_container_width=True)
        st.download_button("JSON herunterladen", data=messages_to_json_bytes(st.session_state.messages), file_name=f"{export_name}.json", mime="application/json", use_container_width=True)
        if st.session_state.auto_report:
            st.download_button("Report herunterladen", data=st.session_state.auto_report, file_name=f"{export_name}_report.txt", mime="text/plain", use_container_width=True)
        st.caption("Exporte enthalten immer den kompletten Verlauf.")
    else:
        st.info("Noch kein Verlauf vorhanden.")

if st.session_state.started:
    st_autorefresh(interval=AUTO_REFRESH_MS, key="live_refresh")

while not st.session_state.chat_queue.empty():
    msg = st.session_state.chat_queue.get()
    if isinstance(msg, dict):
        persist_message(msg)

all_messages = clean_message_store(st.session_state.messages)
comment_messages = get_comment_messages(all_messages)
comment_df = build_dataframe(comment_messages)

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
roles = role_summary(scores_df)

if run_report:
    st.session_state.auto_report = generate_rule_based_report(comment_df, scores_df, clusters_df, impact)
    st.sidebar.success("Report erstellt.")

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Nachrichten", summary["messages"])
k2.metric("User", summary["users"])
k3.metric("Fragen", summary["questions"])
k4.metric("Trigger", summary["trigger_msgs"])
k5.metric("Abwertend", summary["toxic_msgs"])
k6.metric("Laufzeit", elapsed_label(st.session_state.capture_started_at))

s1, s2, s3 = st.columns(3)
s1.markdown(f'<div class="status-box"><strong>Board:</strong> {st.session_state.board_id}</div>', unsafe_allow_html=True)
s2.markdown(f'<div class="status-box"><strong>Host:</strong> {st.session_state.username or "-"}</div>', unsafe_allow_html=True)
s3.markdown(f'<div class="status-box"><strong>Status:</strong> {"running" if st.session_state.started else "idle"}</div>', unsafe_allow_html=True)

st.caption("Die fünf Wirkungsfelder orientieren sich an deinem Live-Impact-Kompass. Die Skala ist hier bewusst konservativer kalibriert: +3 ist selten, 0 ist wirklich neutral.")

score_cols = st.columns(5)
for idx, (name, val) in enumerate(impact.items()):
    with score_cols[idx]:
        top = st.columns([0.82, 0.18])
        with top[0]:
            st.markdown(f"**{name}**")
        with top[1]:
            with st.popover("ℹ️"):
                st.write(IMPACT_HELP[name])
        color = score_color(val)
        arrow = score_arrow(val)
        st.markdown(
            f"""
            <div class="score-card" style="border-left: 6px solid {color};">
                <div class="score-big" style="color:{color};">{val}<span class="score-arrow" style="color:{color};">{arrow}</span></div>
                <div class="score-sub">{score_label(val)}</div>
                <div class="score-sub" style="margin-top:.35rem;">Ampel: {'grün' if val >= 1 else 'gelb' if val == 0 else 'rot'}</div>
                <div class="score-sub">Skala -3 bis +3</div>
            </div>
            """,
            unsafe_allow_html=True
        )

left, right = st.columns([1.25, 1.0], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Live-Feed")
    i1, i2, i3 = st.columns(3)
    i1.info(f"Host: {st.session_state.username or '-'}")
    i2.info(f"Sichtbar: {min(len(filtered_df), DISPLAY_LIMIT)} - neueste oben")
    i3.info(f"Gesamt: {len(comment_df)}")

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
            badge_html = "".join(badges)
            username_col = user_color(row["username"])
            ts = row["dt"].strftime("%H:%M:%S") if pd.notna(row["dt"]) else "--:--:--"
            avatar_col, content_col = st.columns([0.10, 0.90], gap="small")
            with avatar_col:
                if row.get("avatar_url"):
                    st.image(row["avatar_url"], width=44)
                else:
                    st.markdown(
                        f'<div class="avatar-fallback" style="background:{username_col};">{initials(row["username"])}</div>',
                        unsafe_allow_html=True
                    )
            with content_col:
                st.markdown(
                    f"""
                    <div class="chat-item">
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
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Dynamik")
    activity_df = activity_per_minute(comment_df)
    if not activity_df.empty:
        st.altair_chart(
            alt.Chart(activity_df).mark_line(point=True).encode(
                x=alt.X("minute:T", title="Zeit"),
                y=alt.Y("messages:Q", title="Nachrichten/Minute"),
                tooltip=["minute:T", "messages:Q"]
            ).properties(height=250),
            use_container_width=True
        )
    else:
        st.info("Noch keine Zeitreihe vorhanden.")
    st.info(salience_warning(comment_df, scores_df))
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Top-Wörter und Emojis")
    c1, c2 = st.columns(2)
    words_df = top_words(filtered_df if not filtered_df.empty else comment_df)
    emojis_df = top_emojis(filtered_df if not filtered_df.empty else comment_df)
    with c1:
        st.dataframe(words_df if not words_df.empty else pd.DataFrame(columns=["word", "count"]), use_container_width=True, hide_index=True)
    with c2:
        st.dataframe(emojis_df if not emojis_df.empty else pd.DataFrame(columns=["emoji", "count"]), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

l2, r2 = st.columns(2, gap="large")

with l2:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Aktivste User")
    top_users_df = top_users(comment_df)
    if not top_users_df.empty:
        st.altair_chart(
            alt.Chart(top_users_df).mark_bar().encode(
                x=alt.X("messages:Q", title="Nachrichten"),
                y=alt.Y("username:N", sort="-x", title="User"),
                tooltip=["username", "messages"]
            ).properties(height=340),
            use_container_width=True
        )
    else:
        st.info("Noch keine User-Daten.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Wiederholungen / mögliche Spam-Muster")
    rep_df = repeated_messages(comment_df, min_count=2)
    if not rep_df.empty:
        st.dataframe(rep_df, use_container_width=True, hide_index=True)
    else:
        st.info("Bisher keine auffälligen Wiederholungen erkannt.")
    st.markdown("</div>", unsafe_allow_html=True)

with r2:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Auffällige User / Diskursverschiebung")
    if not scores_df.empty:
        st.dataframe(scores_df.head(25), use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine User-Scores verfügbar.")
    st.caption("Shift-Score kombiniert Frequenz, Triggerbegriffe, Wiederholungen, Frage-Druck, Capslock und abwertende Marker.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Themencluster")
    if not clusters_df.empty:
        st.dataframe(clusters_df, use_container_width=True, hide_index=True)
    else:
        st.info("Für Themencluster werden mehr Chatdaten benötigt.")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.subheader("Rollenbild und Narrative")
c1, c2 = st.columns(2)
with c1:
    if roles:
        role_df = pd.DataFrame([{"Rolle": k, "Anzahl": v} for k, v in roles.items()])
        st.dataframe(role_df, use_container_width=True, hide_index=True)
    else:
        st.info("Noch keine Rollenverteilung verfügbar.")
with c2:
    narratives = narrative_candidates(comment_df)
    if narratives:
        for item in narratives:
            st.write(f"- {item}")
    else:
        st.info("Noch keine stabilen Narrative erkannt.")
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.subheader("Automatischer Report")
if st.session_state.auto_report:
    st.markdown(f'<div class="report-box">{st.session_state.auto_report}</div>', unsafe_allow_html=True)
else:
    st.info("Noch kein Report erstellt. Nutze links in der Sidebar den Button 'Report erstellen'.")
st.markdown("</div>", unsafe_allow_html=True)

st.caption("Verlauf bleibt nach Disconnect erhalten. Nur 'Reset' setzt die Session zurück. Sichtbar sind maximal die letzten 2000 Nachrichten, Exporte enthalten alles.")
