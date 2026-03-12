#!/usr/bin/env python3
"""
fetch_odds_betfair.py — gira su GitHub Actions ogni 15 minuti
Prende le quote tennis da Betfair Exchange API (gratuita, illimitata)
e le salva su Firebase per l'app Tennis Fantasy.

VARIABILI D'AMBIENTE (GitHub Secrets):
  BETFAIR_APP_KEY     — la tua App Key da developer.betfair.com
  BETFAIR_USERNAME    — email account Betfair
  BETFAIR_PASSWORD    — password account Betfair
  FIREBASE_KEY_JSON   — contenuto del firebase-key.json (copia-incolla tutto)
"""

import os, json, requests, firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone, date, timedelta

# ── BETFAIR API ───────────────────────────────────────────────────────────────

BETFAIR_LOGIN_URL    = "https://identitysso.betfair.com/api/login"
BETFAIR_API_URL      = "https://api.betfair.com/exchange/betting/json-rpc/v1"
BETFAIR_APP_KEY      = os.environ.get("BETFAIR_APP_KEY", "")
BETFAIR_USERNAME     = os.environ.get("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD     = os.environ.get("BETFAIR_PASSWORD", "")

# Betfair event type: Tennis = 2
TENNIS_EVENT_TYPE_ID = "2"

# ── LOGIN ─────────────────────────────────────────────────────────────────────

def betfair_login() -> str:
    """
    Esegue il login su Betfair e ritorna il session token.
    Il token dura ~4 ore — rinnovato ad ogni run di GitHub Actions.
    """
    r = requests.post(
        BETFAIR_LOGIN_URL,
        data={"username": BETFAIR_USERNAME, "password": BETFAIR_PASSWORD},
        headers={
            "X-Application": BETFAIR_APP_KEY,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        },
        timeout=10
    )
    data = r.json()
    if data.get("status") != "SUCCESS":
        raise RuntimeError(f"Login Betfair fallito: {data.get('error', 'unknown')}")
    token = data.get("token")
    print(f"  ✅ Login OK, token: ...{token[-8:]}")
    return token


def betfair_request(session_token: str, method: str, params: dict) -> dict:
    """Esegue una chiamata all'API Betfair Exchange."""
    headers = {
        "X-Application": BETFAIR_APP_KEY,
        "X-Authentication": session_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = [{"jsonrpc": "2.0", "method": f"SportsAPING/v1.0/{method}", "params": params, "id": 1}]
    r = requests.post(BETFAIR_API_URL, json=payload, headers=headers, timeout=15)
    r.raise_for_status()
    result = r.json()
    if isinstance(result, list):
        result = result[0]
    if "error" in result:
        raise RuntimeError(f"Betfair API error: {result['error']}")
    return result.get("result", {})

# ── FETCH TENNIS MARKETS ──────────────────────────────────────────────────────

def get_tennis_events(session_token: str) -> list[dict]:
    """
    Recupera tutti gli eventi tennis delle prossime 24h.
    Filtra solo i match (non tornei o qualificazioni).
    """
    now = datetime.now(timezone.utc)
    tomorrow = now + timedelta(hours=36)

    result = betfair_request(session_token, "listEvents", {
        "filter": {
            "eventTypeIds": [TENNIS_EVENT_TYPE_ID],
            "marketStartTime": {
                "from": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": tomorrow.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }
    })

    events = []
    for e in (result or []):
        ev = e.get("event", {})
        name = ev.get("name", "")
        # Betfair naming: "A. Sinner v C. Alcaraz" — filtra solo match diretti
        if " v " in name or " vs " in name.lower():
            events.append({
                "id": ev.get("id"),
                "name": name,
                "countryCode": ev.get("countryCode", ""),
                "startTime": ev.get("openDate", ""),
                "marketCount": e.get("marketCount", 0)
            })

    print(f"  📅 {len(events)} match tennis trovati nelle prossime 36h")
    return events


def get_match_odds(session_token: str, event_ids: list[str]) -> list[dict]:
    """
    Per ogni evento, recupera i mercati MATCH_ODDS (vincente).
    Betfair ritorna: runner1 (p1), runner2 (p2), con bestAvailableToBack price.
    """
    if not event_ids:
        return []

    # Lista mercati MATCH_ODDS per gli eventi
    markets = betfair_request(session_token, "listMarketCatalogue", {
        "filter": {
            "eventIds": event_ids,
            "marketTypeCodes": ["MATCH_ODDS"],
            "eventTypeIds": [TENNIS_EVENT_TYPE_ID]
        },
        "marketProjection": ["RUNNER_DESCRIPTION", "EVENT", "MARKET_START_TIME"],
        "maxResults": 200
    })

    if not markets:
        return []

    market_ids = [m["marketId"] for m in markets]
    print(f"  🎯 {len(market_ids)} mercati MATCH_ODDS trovati")

    # Prendi i prezzi migliori per ogni mercato
    books = betfair_request(session_token, "listMarketBook", {
        "marketIds": market_ids,
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS"],
            "exBestOffersOverrides": {"bestPricesDepth": 1}
        }
    })

    # Mappa marketId → catalogue info
    cat_map = {m["marketId"]: m for m in markets}

    results = []
    for book in (books or []):
        mid = book.get("marketId")
        cat = cat_map.get(mid, {})
        runners = book.get("runners", [])
        cat_runners = cat.get("runners", [])

        if len(runners) < 2 or len(cat_runners) < 2:
            continue

        # Estrai nomi giocatori
        def runner_name(selection_id):
            for r in cat_runners:
                if r.get("selectionId") == selection_id:
                    return r.get("runnerName", "")
            return ""

        def best_back_price(runner):
            ex = runner.get("ex", {})
            backs = ex.get("availableToBack", [])
            if backs:
                return float(backs[0].get("price", 0))
            return 0.0

        r0, r1 = runners[0], runners[1]
        p1_name = runner_name(r0.get("selectionId"))
        p2_name = runner_name(r1.get("selectionId"))
        q1 = best_back_price(r0)
        q2 = best_back_price(r1)

        if not p1_name or not p2_name or q1 <= 1.0 or q2 <= 1.0:
            continue

        # Determine ATP/WTA from event name or country
        event = cat.get("event", {})
        event_name = event.get("name", "")
        tour = "WTA" if any(w in event_name.upper() for w in ["WTA", "WOMEN", "LADIES"]) else "ATP"

        start_time = cat.get("marketStartTime", "")

        results.append({
            "id": mid,
            "p1": p1_name,
            "p2": p2_name,
            "q1": round(q1, 2),
            "q2": round(q2, 2),
            "commence_time": start_time,
            "tour": tour,
            "source": "betfair",
            "marketId": mid,
            "eventId": event.get("id", ""),
            "updatedAt": datetime.now(timezone.utc).isoformat()
        })

    return results


def get_over_under_odds(session_token: str, event_ids: list[str]) -> dict[str, dict]:
    """
    Recupera mercati Over/Under games per ogni evento.
    Betfair chiama questi mercati "TOTAL_GAMES" o "GAME_HANDICAP".
    Ritorna mappa eventId → {line, over, under}
    """
    try:
        markets = betfair_request(session_token, "listMarketCatalogue", {
            "filter": {
                "eventIds": event_ids,
                "marketTypeCodes": ["TOTAL_GAMES", "GAME_HANDICAP"],
                "eventTypeIds": [TENNIS_EVENT_TYPE_ID]
            },
            "marketProjection": ["RUNNER_DESCRIPTION", "EVENT"],
            "maxResults": 200
        })

        if not markets:
            return {}

        market_ids = [m["marketId"] for m in markets]
        books = betfair_request(session_token, "listMarketBook", {
            "marketIds": market_ids,
            "priceProjection": {"priceData": ["EX_BEST_OFFERS"],
                                "exBestOffersOverrides": {"bestPricesDepth": 1}}
        })

        cat_map = {m["marketId"]: m for m in markets}
        result = {}

        for book in (books or []):
            mid = book.get("marketId")
            cat = cat_map.get(mid, {})
            event_id = cat.get("event", {}).get("id", "")
            runners = book.get("runners", [])
            cat_runners = cat.get("runners", [])

            if len(runners) < 2:
                continue

            def rname(sid):
                for r in cat_runners:
                    if r.get("selectionId") == sid:
                        return r.get("runnerName", "")
                return ""

            def bbprice(runner):
                backs = runner.get("ex", {}).get("availableToBack", [])
                return float(backs[0].get("price", 0)) if backs else 0.0

            r0_name = rname(runners[0].get("selectionId", ""))
            r1_name = rname(runners[1].get("selectionId", ""))
            p0 = bbprice(runners[0])
            p1 = bbprice(runners[1])

            # Cerca linea nel nome (es. "Over 22.5 Games")
            import re
            m = re.search(r'(\d+\.?\d*)', r0_name)
            line = float(m.group(1)) if m else None

            if line and p0 > 1 and p1 > 1:
                over_price = p0 if "over" in r0_name.lower() else p1
                under_price = p1 if "over" in r0_name.lower() else p0
                result[event_id] = {"line": line, "over": over_price, "under": under_price}

        return result
    except Exception as e:
        print(f"  ⚠️  Over/Under fetch fallito: {e}")
        return {}

# ── FIREBASE ──────────────────────────────────────────────────────────────────

def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON", "")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON non trovato")
    cred = credentials.Certificate(json.loads(key_json))
    firebase_admin.initialize_app(cred)
    return firestore.client()


def upload_to_firebase(db, matches: list[dict]):
    if not matches:
        print("  Nessuna partita da caricare")
        return

    batch = db.batch()

    for m in matches:
        ref = db.collection("scrapedOdds").document(m["id"])
        batch.set(ref, m, merge=True)

    # Elimina partite vecchie
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    old = db.collection("scrapedOdds").where("updatedAt", "<", cutoff).stream()
    deleted = 0
    for doc in old:
        batch.delete(doc.reference)
        deleted += 1

    batch.commit()

    # Aggiorna config
    db.collection("config").document("game").set({
        "oddsLastScraped": datetime.now(timezone.utc).isoformat(),
        "oddsScrapedCount": len(matches),
        "oddsScrapeSource": "betfair"
    }, merge=True)

    print(f"  ✅ {len(matches)} partite salvate, {deleted} vecchie eliminate")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n🎾 Tennis Fantasy — Betfair Odds Fetcher")
    print(f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    # Verifica config
    if not all([BETFAIR_APP_KEY, BETFAIR_USERNAME, BETFAIR_PASSWORD]):
        print("❌ Variabili mancanti! Controlla i GitHub Secrets:")
        print("   BETFAIR_APP_KEY, BETFAIR_USERNAME, BETFAIR_PASSWORD")
        return

    # 1. Login Betfair
    print("\n🔑 Login Betfair...")
    try:
        token = betfair_login()
    except Exception as e:
        print(f"  ❌ {e}")
        return

    # 2. Firebase
    print("\n🔥 Connessione Firebase...")
    try:
        db = init_firebase()
        print("  ✅ Connesso")
    except Exception as e:
        print(f"  ❌ {e}")
        return

    # 3. Recupera eventi tennis
    print("\n📡 Recupero match tennis...")
    try:
        events = get_tennis_events(token)
        if not events:
            print("  Nessun match nelle prossime 36h")
            return
    except Exception as e:
        print(f"  ❌ {e}")
        return

    event_ids = [e["id"] for e in events]

    # 4. Quote vincente (MATCH_ODDS)
    print("\n💰 Recupero quote MATCH_ODDS...")
    try:
        matches = get_match_odds(token, event_ids)
        print(f"  → {len(matches)} partite con quote valide")
    except Exception as e:
        print(f"  ❌ {e}")
        matches = []

    # 5. Quote Over/Under (opzionale)
    print("\n📊 Recupero quote Over/Under...")
    try:
        ou_map = get_over_under_odds(token, event_ids)
        # Aggiungi OU ai match corrispondenti
        for m in matches:
            ou = ou_map.get(m.get("eventId"))
            if ou:
                m["ou"] = ou
        print(f"  → {len(ou_map)} partite con Over/Under")
    except Exception as e:
        print(f"  ⚠️  {e}")

    if not matches:
        print("\n⚠️  Nessun match trovato — possibile che il torneo non sia attivo su Betfair")
        return

    # 6. Mostra riepilogo
    print(f"\n📋 Riepilogo ({len(matches)} partite):")
    for m in matches[:10]:
        ou_str = f"  OU:{m['ou']['line']}" if m.get("ou") else ""
        print(f"  {m['tour']:3} {m['p1']:22} {m['q1']:.2f} vs {m['p2']:22} {m['q2']:.2f}{ou_str}")
    if len(matches) > 10:
        print(f"  ... e altre {len(matches)-10}")

    # 7. Upload Firebase
    print(f"\n💾 Upload su Firebase...")
    upload_to_firebase(db, matches)

    print(f"\n✅ Fatto! Quote aggiornate ogni 15 min automaticamente.")


if __name__ == "__main__":
    main()
