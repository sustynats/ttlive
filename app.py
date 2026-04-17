import streamlit as st
import threading
import queue
from datetime import datetime
from pathlib import Path

from streamlit_autorefresh import st_autorefresh
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent


st.set_page_config(page_title="TikTok Live Chat Reader", layout="centered")

DISPLAY_LIMIT = 2000
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)


def normalize_username(username: str) -> str:
    username = username.strip()
    if not username:
        raise ValueError("Bitte einen TikTok-Usernamen eingeben.")
    if not username.startswith("@"):
        username = "@" + username
    return username


def make_export_filename(username: str) -> str:
    safe_username = username.replace("@", "").replace("/", "_").replace("\\", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(EXPORT_DIR / f"chat_{safe_username}_{timestamp}.txt")


def append_to_file(filepath: str, line: str) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def start_client(username: str, chat_queue, export_file: str):
    try:
        client = TikTokLiveClient(unique_id=username)

        @client.on(ConnectEvent)
        async def on_connect(event):
            timestamp = datetime.now().strftime("%H:%M:%S")
            message = f"[{timestamp}] SYSTEM: Verbunden mit {username}"
            chat_queue.put(message)
            append_to_file(export_file, message)

        @client.on(CommentEvent)
        async def on_comment(event):
            timestamp = datetime.now().strftime("%H:%M:%S")
            nickname = getattr(event.user, "nickname", "Unbekannt")
            comment = getattr(event, "comment", "")
            message = f"[{timestamp}] {nickname}: {comment}"
            chat_queue.put(message)
            append_to_file(export_file, message)

        @client.on(DisconnectEvent)
        async def on_disconnect(event):
            timestamp = datetime.now().strftime("%H:%M:%S")
            message = f"[{timestamp}] SYSTEM: Verbindung getrennt"
            chat_queue.put(message)
            append_to_file(export_file, message)

        client.run()

    except Exception as e:
        timestamp = datetime.now().strftime("%H:%M:%S")
        message = f"[{timestamp}] FEHLER: {e}"
        chat_queue.put(message)
        append_to_file(export_file, message)


if "chat_queue" not in st.session_state:
    st.session_state.chat_queue = queue.Queue()

if "messages" not in st.session_state:
    st.session_state.messages = []

if "started" not in st.session_state:
    st.session_state.started = False

if "username" not in st.session_state:
    st.session_state.username = ""

if "export_file" not in st.session_state:
    st.session_state.export_file = ""

if "listener_thread" not in st.session_state:
    st.session_state.listener_thread = None


st.title("TikTok Live Chat Reader")

username_input = st.text_input(
    "TikTok Username eingeben",
    value=st.session_state.username,
    placeholder="@username"
)

col1, col2 = st.columns(2)

with col1:
    if st.button("Chat starten"):
        try:
            username = normalize_username(username_input)

            if st.session_state.started and st.session_state.username == username:
                st.info(f"Der Chat für {username} läuft bereits.")
            else:
                st.session_state.username = username
                st.session_state.chat_queue = queue.Queue()

                # WICHTIG:
                # messages werden hier NICHT geleert,
                # damit der bisherige Verlauf erhalten bleibt.
                if not st.session_state.export_file:
                    st.session_state.export_file = make_export_filename(username)

                if not st.session_state.messages:
                    header = (
                        f"=== Chat-Mitschnitt für {username} gestartet am "
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==="
                    )
                    st.session_state.messages.append(header)
                    append_to_file(st.session_state.export_file, header)

                thread = threading.Thread(
                    target=start_client,
                    args=(username, st.session_state.chat_queue, st.session_state.export_file),
                    daemon=True
                )
                thread.start()

                st.session_state.listener_thread = thread
                st.session_state.started = True
                st.success(f"Starte Chat für {username}")

        except Exception as e:
            st.error(str(e))

with col2:
    if st.button("Neuen Mitschnitt beginnen"):
        st.session_state.chat_queue = queue.Queue()
        st.session_state.messages = []
        st.session_state.started = False
        st.session_state.listener_thread = None
        st.session_state.export_file = ""
        st.success("Alter Mitschnitt abgeschlossen. Du kannst jetzt einen neuen starten.")

# Auto-Refresh nur wenn einmal gestartet wurde
if st.session_state.started:
    st_autorefresh(interval=2000, key="chat_refresh")

# Queue einlesen und dauerhaft im Session State behalten
while not st.session_state.chat_queue.empty():
    msg = st.session_state.chat_queue.get()
    st.session_state.messages.append(msg)

st.subheader("Live Chat")

if st.session_state.messages:
    st.caption(f"Angezeigt werden die letzten {DISPLAY_LIMIT} Nachrichten. Export enthält alles.")
    chat_container = st.container(height=500)
    with chat_container:
        for msg in st.session_state.messages[-DISPLAY_LIMIT:]:
            st.write(msg)
else:
    st.info("Noch keine Nachrichten. Falls der Stream live ist, warte ein paar Sekunden.")

# Download für den kompletten Verlauf
if st.session_state.messages:
    full_chat_text = "\n".join(st.session_state.messages)
    st.download_button(
        label="Gesamten Chat als TXT herunterladen",
        data=full_chat_text,
        file_name="tiktok_live_chat_export.txt",
        mime="text/plain"
    )

# Zusatzinfo zur lokalen Datei
if st.session_state.export_file:
    st.caption(f"Lokale Exportdatei: {st.session_state.export_file}")
