# TikTok Live Chat Monitor

## Enthalten
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

## Deployment
1. `app.py` und `requirements.txt` in GitHub ersetzen
2. in Streamlit neu deployen
3. danach Browser hart neu laden

## Hinweis
TikTokLive ist eine inoffizielle Bibliothek. Wenn TikTok intern etwas ändert, kann die Verbindung zeitweise brechen.
