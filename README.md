# TikTok Live Impact Monitor V3

Streamlit-Dashboard für die Echtzeit-Analyse von TikTok-Live-Kommentaren mit Fokus auf
Diskursqualität, Wirkungsfeldern und Moderationsunterstützung.

## Was ist neu in V3

### Performance
- Vektorisierte Klassifikation aller Nachrichten in `build_dataframe` (statt Row-Loop)
- Zusätzliche SQLite-Indizes auf `(board_id, type)` und `(board_id, username)`
- WAL-Mode + `cache_size=-20000` für ~20 MB Page-Cache
- Connection-Locking via `threading.RLock` für stabile Writes aus dem Listener-Thread

### Robustheit
- Defensive Empty-State-Behandlung in allen V3-Komponenten
- `try/except`-Wrapping um Score-Pipelines im neuen Moderation-Tab
- `busy_timeout=5000` ms gegen kurzzeitige DB-Locks

### Neue Features
- **🛡️ Moderation & Alerts** (neuer Tab) bündelt:
  - **Moderator-Cockpit**: priorisierte Aktionen (Hoch/Mittel/Niedrig) auf Basis von
    Shift-Score, Risiko-Signalen, Korrelationen und aktueller Tonalität.
  - **Tonalitäts-Prognose**: lineare Trend-Schätzung (numpy.polyfit) der Trigger-/
    Abwertung-Anteile für die nächsten 5 Minuten.
  - **Volltext-Suche** im Chatverlauf (Text + Username) mit Sortierung nach Zeit.
  - **Custom-Alert-Builder**: Nutzer:innen definieren eigene Schlüsselwörter mit
    Schwellwerten; Alerts werden persistent in SQLite gespeichert und live ausgewertet.

## Schnellstart

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Konfiguration (Streamlit Secrets)

Optional in `.streamlit/secrets.toml`:

```toml
# Google OIDC-Login
[auth]
client_id = "..."
client_secret = "..."
cookie_secret = "..."
allowed_emails = ["alice@example.com"]

# Gemini-API
[google]
api_key = "..."
```

## Datenmodell

- `boards` — Analyse-Räume (Board-ID, Host, Status, Report)
- `messages` — alle Live-Events (Comment, Like, Gift, Join, ...)
- `custom_alerts` — V3: vom Nutzer definierte Keyword-Alerts pro Board

## Deployment

1. `app.py` und `requirements.txt` in GitHub aktualisieren
2. Streamlit-App neu deployen
3. Browser hart neu laden (Strg+Shift+R)

## Architekturhinweise

Die App ist (noch) als Single-File-Streamlit-App organisiert. Eine vollständige Modularisierung
in `ttlive/`-Pakete ist als V4-Schritt vorgesehen. V3 fokussiert auf Performance, Robustheit
und neue, sofort spürbare User-Features ohne die laufende App zu zerlegen.
