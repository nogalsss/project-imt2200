import os, time, json, pathlib, datetime as dt, requests, pandas as pd

CLIENT_ID     = "lbosp4quzjq9oe0nhgd7k291j7lf7y"
CLIENT_SECRET = "0qlq0q84p2zfzucsxa0ocgg167cnsb"

RAW_DIR   = pathlib.Path("datos/no_limpios");  RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR = pathlib.Path("datos/limpios");     CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def epoch(d: dt.date) -> int:
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())

def igdb_token():
    r = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={"client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET,
              "grant_type": "client_credentials"},
        timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]

# Descargar juegos IGDB (último año) 
def fetch_igdb_last_year():
    today  = dt.date.today()
    start  = today.replace(year=today.year - 1)            # hace 12 meses
    ini, fin = epoch(start), epoch(today)

    token = igdb_token()
    hdrs  = {"Client-ID": CLIENT_ID, "Authorization": f"Bearer {token}"}
    api   = "https://api.igdb.com/v4/games"

    STEP = 500
    query = f"""
    fields id,name,first_release_date,
           genres.name,platforms.name,
           rating,rating_count,aggregated_rating,total_rating,
           follows,popularity,hypes,
           involved_companies.company.name,
           involved_companies.developer,
           involved_companies.publisher,
           external_games;
    where first_release_date >= {ini}
      & first_release_date <  {fin};
    limit {STEP};
    offset %d;
    """

    data, offset = [], 0
    while True:
        batch = requests.post(api, headers=hdrs, data=query % offset, timeout=30).json()
        if not batch:
            break
        data.extend(batch)
        offset += STEP
        print(f"IGDB +{len(batch):3d}  (offset {offset})")
        time.sleep(0.35)
    RAW_DIR.joinpath("igdb_last_year.json").write_text(json.dumps(data, indent=2))
    return data, hdrs

def steam_ids_from_igdb(igdb_data, hdrs):
    api_ext = "https://api.igdb.com/v4/external_games"
    CHUNK, mapping = 200, {}
    ids_list = [g["id"] for g in igdb_data]
    for chunk in [ids_list[i:i+CHUNK] for i in range(0, len(ids_list), CHUNK)]:
        q = f"fields game, uid, category; where game = ({','.join(map(str,chunk))}) & category = 1;"  # 1=Steam
        for eg in requests.post(api_ext, headers=hdrs, data=q, timeout=30).json():
            mapping[eg["game"]] = int(eg["uid"])
        time.sleep(0.35)
    RAW_DIR.joinpath("steam_appids_last_year.json").write_text(json.dumps(mapping, indent=2))
    return mapping

def fetch_steam(appids, throttle=0.25):
    players, details = {}, {}
    for aid in appids:
        p = requests.get(
            "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
            params={"appid": aid}, timeout=10).json()
        players[aid] = p["response"].get("player_count", 0)

        d = requests.get(
            "https://store.steampowered.com/api/appdetails",
            params={"appids": aid, "cc": "us", "l": "en"}, timeout=10).json()
        if d[str(aid)]["success"]:
            details[aid] = d[str(aid)]["data"]

        print(f"Steam {aid} OK")
        time.sleep(throttle)
    RAW_DIR.joinpath("steam_players_last_year.json").write_text(json.dumps(players, indent=2))
    RAW_DIR.joinpath("steam_details_last_year.json").write_text(json.dumps(details, indent=2))
    return players, details

def save_csv_igdb(games):
    df = pd.json_normalize(games, sep="_")
    df.to_csv(CLEAN_DIR / "igdb_games_last_year.csv", index=False)

def save_csv_steam(players, details):
    df_players = pd.Series(players, name="player_count").to_frame()
    df_details = pd.json_normalize(details).set_index("steam_appid")
    df_players.join(df_details, how="left").to_csv(CLEAN_DIR / "steam_last_year.csv")
if __name__ == "__main__":
    igdb_raw, hdrs = fetch_igdb_last_year()
    steam_map      = steam_ids_from_igdb(igdb_raw, hdrs)

    players, details = fetch_steam(list(steam_map.values()))
    save_csv_igdb(igdb_raw)
    save_csv_steam(players, details)

    print("\n✅ Proceso completo: CSV listos en  datos/limpios/")
