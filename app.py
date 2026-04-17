import streamlit as st
import threading
import queue
from streamlit_autorefresh import st_autorefresh
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent

st.set_page_config(page_title="TikTok Live Chat Reader", layout="centered")

if "chat_queue" not in st.session_state:
    st.session_state.chat_queue = queue.Queue()

if "messages" not in st.session_state:
    st.session_state.messages = []

if "started" not in st.session_state:
    st.session_state.started = False

if "listener_running" not in st.session_state:
    st.session_state.listener_running = False

if "username" not in st.session_state:
    st.session_state.username = ""


def normalize_username(username: str) -> str:
    username = username.strip()
    if not username:
        raise ValueError("Bitte einen TikTok-Usernamen eingeben.")
    if not username.startswith("@"):
        username = "@" + username
    return username


def start_client(username: str, chat_queue):
    try:
        client = TikTokLiveClient(unique_id=username)

        @client.on(ConnectEvent)
        async def on_connect(event):
            chat_queue.put(f"✅ Verbunden mit {username}")

        @client.on(CommentEvent)
        async def on_comment(event):
            nickname = getattr(event.user, "nickname", "Unbekannt")
            comment = getattr(event, "comment", "")
            chat_queue.put(f"{nickname}: {comment}")

        client.run()

    except Exception as e:
        chat_queue.put(f"❌ Fehler: {e}")


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
            st.session_state.username = username
            st.session_state.chat_queue = queue.Queue()
            st.session_state.messages = []
            st.session_state.started = True
            st.session_state.listener_running = True

            thread = threading.Thread(
                target=start_client,
                args=(username, st.session_state.chat_queue),
                daemon=True
            )
            thread.start()

            st.success(f"Starte Chat für {username}")

        except Exception as e:
            st.error(str(e))

with col2:
    if st.button("Chat leeren"):
        st.session_state.messages = []
        st.session_state.chat_queue = queue.Queue()
        st.success("Chat geleert.")

if st.session_state.started:
    st_autorefresh(interval=2000, key="chat_refresh")

while not st.session_state.chat_queue.empty():
    st.session_state.messages.append(st.session_state.chat_queue.get())

st.subheader("Live Chat")

if st.session_state.messages:
    for msg in st.session_state.messages[-200:]:
        st.write(msg)
else:
    st.info("Noch keine Nachrichten. Falls der Stream live ist, warte ein paar Sekunden.")
