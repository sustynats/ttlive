# TikTok Live Chat Monitor

Enthalten:
- Live-Feed mit besser lesbarem Zeitstempel rechts unten
- Zeitzone Europe/Berlin
- letzte 2000 sichtbare Nachrichten
- kompletter Verlauf als TXT, CSV und JSON
- Verlauf bleibt nach Disconnect erhalten
- Suchfeld, User- und Tonlagenfilter
- Kennzahlen zu Fragen, Triggern und abwertender Sprache
- Aktivitätsanalyse pro Minute
- Top-Wörter, Emojis und aktivste User
- Wiederholungen / mögliche Spam-Muster
- heuristischer Shift-Score für auffällige User
- Themencluster mit TF-IDF + KMeans
- Diskursprofil
- 1-Klick-KI-Report

Hinweis zum KI-Report:
- Die App nutzt dafür die OpenAI Python Library und die Responses API.
- Du brauchst einen OpenAI API Key als Streamlit Secret/Umgebungsvariable oder im Passwortfeld der Sidebar.
- Die OpenAI Python Library unterstützt Python 3.9+ und die Responses API ist die empfohlene API für neue Projekte. citeturn336821search0turn336821search3turn336821search8

Deployment:
1. `app.py` und `requirements.txt` in GitHub ersetzen
2. in Streamlit neu deployen
3. Browser hart neu laden

Wichtig:
TikTokLive ist eine inoffizielle Bibliothek. Wenn TikTok intern etwas ändert, kann die Verbindung zeitweise brechen.
