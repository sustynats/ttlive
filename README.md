# TikTok Live Impact Monitor V2 - Shared Dashboard

Neu in dieser Version:
- Board-ID und teilbare URL
- gemeinsamer Datenstand pro Dashboard
- persönliche Filter pro Nutzer-Session
- gemeinsamer Report für alle auf demselben Board
- SQLite als gemeinsamer Speicher
- Best-Effort-Profilbilder mit Fallback auf Initialen

Beispiel:
https://ttlivechat.streamlit.app/?board=abc123

Wichtig:
- Eine Person sollte den Mitschnitt für ein Board starten
- Andere können dieselbe URL öffnen und live mitsehen
- Filter, Suche und User-Fokus bleiben lokal je Session

Deployment:
1. app.py und requirements.txt in GitHub ersetzen
2. neu deployen
3. Browser hart neu laden
