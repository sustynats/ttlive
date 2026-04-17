# TikTok Live Chat Monitor

## Dateien
- `app.py`
- `requirements.txt`

## Features
- TikTok Username eingeben und Chat live mitlesen
- Sichtbar: letzte 2000 Nachrichten
- Export: kompletter Verlauf als TXT, CSV und JSON
- Verlauf bleibt nach Disconnect erhalten
- Such- und User-Filter
- Kennzahlen zu Fragen, Triggern, abwertender Sprache und Capslock
- Aktivitäts-Chart pro Minute
- Top-Wörter, Emojis und aktivste User
- Wiederholungen und mögliche Spam-Muster
- Heuristischer Shift-Score für auffällige User
- Themencluster mit TF-IDF + KMeans

## Deployment
Für Streamlit Community Cloud:
1. `app.py` und `requirements.txt` in dein GitHub-Repo legen
2. App in Streamlit neu deployen
3. Python 3.12 auswählen

## Hinweis
TikTokLive ist eine inoffizielle Bibliothek. Wenn TikTok intern Änderungen macht, kann die Verbindung zeitweise brechen.
