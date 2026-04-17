import streamlit as st
import threading
import queue
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent

# Queue für Chatnachrichten
chat_queue = queue.Queue()

# Username normalisieren
def normalize_username(username):
    username = username.strip()
    if not username.startswith("@"):
        username = "@" + username
    return username

# TikTok Listener starten
def start_client(username):
    client = TikTokLiveClient(unique_id=username)

    @client.on(ConnectEvent)
    async def on_connect(event):
        chat_queue.put(f"Verbunden mit {username}")

    @client.on(CommentEvent)
    async def on_comment(event):
        chat_queue.put(f"{event.user.nickname}: {event.comment}")

    client.run()

# UI
st.title("TikTok Live Chat Reader")

username_input = st.text_input("TikTok Username eingeben")

if st.button("Chat starten"):
    username = normalize_username(username_input)

    thread = threading.Thread(target=start_client, args=(username,), daemon=True)
    thread.start()

    st.success(f"Starte Chat für {username}")

st.subheader("Live Chat")

while not chat_queue.empty():
    st.write(chat_queue.get())
