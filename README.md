# TikTok Live Impact Monitor V2 - kostenlose Version

Enthalten:
- stabile Queue-Anbindung für den Live-Thread
- Zeitzone Europe/Berlin
- besser lesbarer Zeitstempel rechts unten
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
- fünf Wirkungsfelder nach dem Live-Impact-Kompass
- Rollenbild und Narrative
- kostenloser Auto-Report ohne API-Key
- Best-Effort-Profilbilder mit Fallback auf Initialen

Deployment:
1. app.py und requirements.txt in GitHub ersetzen
2. neu deployen
3. Browser hart neu laden

Hinweis:
TikTokLive ist eine inoffizielle Bibliothek. Profilbilder werden genutzt, wenn TikTokLive sie im Event mitliefert. Sonst zeigt die App Initialen an.
