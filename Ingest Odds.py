# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest NFL survivor data (no Google Sheet)
# MAGIC Builds `workspace.default.nfl_survivor_csv_export` straight from the web, with the same columns the Google Sheet "CSV Export" tab had:
# MAGIC
# MAGIC | Data | Source |
# MAGIC |---|---|
# MAGIC | Schedule (Week, Team, Team_Opp, Home) | ESPN public scoreboard API |
# MAGIC | Game moneylines (`Target`) | ESPN odds API (DraftKings lines). Optional: FanDuel lines via The Odds API if you give a key |
# MAGIC | Futures (Super Bowl, Conference, Division, Wins O/U, Playoffs Y/N) | ESPN futures article table (same page the sheet used); falls back to ESPN's futures API |
# MAGIC
# MAGIC Games with no line yet are left with a blank `Target`, so `Odds Predictions` predicts them just as before.
# MAGIC It also publishes the current NFL week as a job task value (`current_week`) for the `Picks` notebook.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("season", "2026", "Season")
dbutils.widgets.text("max_week", "18", "Last regular-season week to load")
dbutils.widgets.text(
    "espn_futures_url",
    "https://www.espn.com/espn/betting/story/_/id/47809892/2026-super-bowl-lxi-odds-nfl-sports-betting-season-win-totals-playoffs",
    "ESPN futures article URL",
)
dbutils.widgets.text("odds_api_key", "", "Optional: The Odds API key (FanDuel lines)")

SEASON = int(dbutils.widgets.get("season"))
MAX_WEEK = int(dbutils.widgets.get("max_week"))
ESPN_FUTURES_URL = dbutils.widgets.get("espn_futures_url").strip()
ODDS_API_KEY = dbutils.widgets.get("odds_api_key").strip()
dbutils.widgets.dropdown("data_source", "auto", ["auto", "web", "github", "volume"], "Data source")
dbutils.widgets.text("github_data_url", "", "GitHub raw data folder, e.g. https://raw.githubusercontent.com/<you>/<repo>/main/data")
DATA_SOURCE = dbutils.widgets.get("data_source")
GITHUB_DATA_URL = dbutils.widgets.get("github_data_url").strip().rstrip("/")
dbutils.widgets.text("github_token", "", "Only for a private GitHub repo: a read-only access token")
GITHUB_TOKEN = dbutils.widgets.get("github_token").strip()
GITHUB_HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
VOLUME = "/Volumes/workspace/default/nfl_survivor"

OUTPUT_TABLE = "workspace.default.nfl_survivor_csv_export"

# COMMAND ----------

# DBTITLE 1,No internet? Use the data GitHub Actions fetched (or a volume upload)
import requests


def web_reachable():
    try:
        requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard", timeout=10)
        return True
    except Exception:
        return False


source = DATA_SOURCE
if source == "auto":
    source = "web" if web_reachable() else ("github" if GITHUB_DATA_URL else "volume")
print(f"Data source: {source}")

if source in ("github", "volume"):
    import io
    import os
    import pandas as pd

    if source == "github":
        if not GITHUB_DATA_URL:
            raise Exception("Set the github_data_url job parameter (see Job setup.md).")
        r = requests.get(f"{GITHUB_DATA_URL}/nfl_survivor_csv_export.csv", headers=GITHUB_HEADERS, timeout=30)
        if r.status_code == 404:
            raise Exception("GitHub returned 404. If the repo is private, make it public or set the github_token job parameter (see Job setup.md).")
        r.raise_for_status()
        csv_export = pd.read_csv(io.StringIO(r.text))
        current_week = int(requests.get(f"{GITHUB_DATA_URL}/current_week.txt", headers=GITHUB_HEADERS, timeout=30).text.strip())
        where = GITHUB_DATA_URL
    else:
        csv_path = f"{VOLUME}/nfl_survivor_csv_export.csv"
        if not os.path.exists(csv_path):
            raise Exception(f"This workspace can't reach ESPN and {csv_path} doesn't exist yet.")
        csv_export = pd.read_csv(csv_path)
        with open(f"{VOLUME}/current_week.txt") as f:
            current_week = int(f.read().strip())
        where = VOLUME

    dbutils.jobs.taskValues.set(key="current_week", value=current_week)
    spark.createDataFrame(csv_export).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(OUTPUT_TABLE)
    print(f"Loaded {len(csv_export)} rows from {where} (current_week = {current_week}) into {OUTPUT_TABLE}")
    dbutils.notebook.exit(f"loaded from {source}")

# COMMAND ----------

# DBTITLE 1,Helpers: HTTP, team names, odds parsing
import re
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (nfl-survivor-databricks-job)"})


def get_json(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


# ESPN abbreviation -> the abbreviation style used in your sheet / picks
ESPN_TO_SHEET = {
    "ARI": "Ari", "ATL": "Atl", "BAL": "Bal", "BUF": "Buf", "CAR": "Car", "CHI": "Chi",
    "CIN": "Cin", "CLE": "Cle", "DAL": "Dal", "DEN": "Den", "DET": "Det", "GB": "GB",
    "HOU": "Hou", "IND": "Ind", "JAX": "Jac", "KC": "KC", "LAC": "LAC", "LAR": "LAR",
    "LV": "LV", "MIA": "Mia", "MIN": "Min", "NE": "NE", "NO": "NO", "NYG": "NYG",
    "NYJ": "NYJ", "PHI": "Phi", "PIT": "Pit", "SF": "SF", "SEA": "Sea", "TB": "TB",
    "TEN": "Ten", "WSH": "Was", "WAS": "Was",
}

FULLNAME_TO_SHEET = {
    "Arizona Cardinals": "Ari", "Atlanta Falcons": "Atl", "Baltimore Ravens": "Bal",
    "Buffalo Bills": "Buf", "Carolina Panthers": "Car", "Chicago Bears": "Chi",
    "Cincinnati Bengals": "Cin", "Cleveland Browns": "Cle", "Dallas Cowboys": "Dal",
    "Denver Broncos": "Den", "Detroit Lions": "Det", "Green Bay Packers": "GB",
    "Houston Texans": "Hou", "Indianapolis Colts": "Ind", "Jacksonville Jaguars": "Jac",
    "Kansas City Chiefs": "KC", "Los Angeles Chargers": "LAC", "Los Angeles Rams": "LAR",
    "Las Vegas Raiders": "LV", "Miami Dolphins": "Mia", "Minnesota Vikings": "Min",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "Phi", "Pittsburgh Steelers": "Pit",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "Sea", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "Ten", "Washington Commanders": "Was",
}


def sheet_abbr(espn_abbr):
    return ESPN_TO_SHEET.get(str(espn_abbr).upper(), str(espn_abbr).title())


def parse_odds(val):
    """'+500' -> 500, '-156' -> -156, '10-1' -> 1000 (same conversion the sheet used), '--' -> NaN."""
    if val is None:
        return np.nan
    s = str(val).strip().replace(",", "").replace("−", "-")
    if s in ("", "--", "-", "N/A", "nan", "OFF"):
        return np.nan
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*-\s*1", s)  # fractional "X-1"
    if m:
        return float(m.group(1)) * 100
    m = re.fullmatch(r"[+-]?\d+(?:\.\d+)?", s)
    if m:
        return float(s)
    if s.upper() in ("EVEN", "EV"):
        return 100.0
    return np.nan

# COMMAND ----------

# DBTITLE 1,Current week (published for the Picks notebook)
board = get_json("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard")
season_type = board.get("season", {}).get("type")
espn_week = board.get("week", {}).get("number", 1)

# Regular season = type 2. Before the season, use week 1; after it, use the last week.
if season_type == 1:
    current_week = 1
elif season_type == 2:
    # If every game this week is already final, move on to next week
    statuses = [e["status"]["type"].get("completed", False) for e in board.get("events", [])]
    current_week = espn_week + 1 if statuses and all(statuses) else espn_week
else:
    current_week = MAX_WEEK
current_week = int(min(max(current_week, 1), MAX_WEEK))

dbutils.jobs.taskValues.set(key="current_week", value=current_week)
print(f"ESPN season type {season_type}, week {espn_week} -> current_week = {current_week}")

# COMMAND ----------

# DBTITLE 1,Schedule from ESPN (all regular-season weeks)
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"


def load_week(week):
    data = get_json(SCOREBOARD, params={"seasontype": 2, "week": week, "dates": SEASON, "limit": 50})
    games = []
    for ev in data.get("events", []):
        comp = ev["competitions"][0]
        teams = {c["homeAway"]: c for c in comp["competitors"]}
        if "home" not in teams or "away" not in teams:
            continue
        games.append({
            "Week": week,
            "event_id": ev["id"],
            "comp_id": comp["id"],
            "date": pd.to_datetime(ev["date"], utc=True),
            "home": sheet_abbr(teams["home"]["team"]["abbreviation"]),
            "away": sheet_abbr(teams["away"]["team"]["abbreviation"]),
        })
    return games


with ThreadPoolExecutor(max_workers=6) as pool:
    games = [g for wk in pool.map(load_week, range(1, MAX_WEEK + 1)) for g in wk]

games_df = pd.DataFrame(games)
print(f"Loaded {len(games_df)} games across {games_df['Week'].nunique()} weeks")

# COMMAND ----------

# DBTITLE 1,Moneylines from ESPN's odds API (DraftKings)
ODDS_URL = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{e}/competitions/{c}/odds"


def money_line(side):
    """Pick the current moneyline, falling back to the open line."""
    if not side:
        return np.nan
    for path in (("current", "moneyLine", "american"), ("moneyLine",), ("open", "moneyLine", "american")):
        v = side
        for k in path:
            v = v.get(k) if isinstance(v, dict) else None
        if v not in (None, ""):
            val = parse_odds(v)
            if not np.isnan(val):
                return val
    return np.nan


def load_odds(row):
    try:
        data = get_json(ODDS_URL.format(e=row.event_id, c=row.comp_id))
    except Exception:
        return row.event_id, np.nan, np.nan
    items = data.get("items", [])
    # Prefer DraftKings (provider 100) if several books are listed
    items = sorted(items, key=lambda i: 0 if str(i.get("provider", {}).get("id")) == "100" else 1)
    for it in items:
        h, a = money_line(it.get("homeTeamOdds")), money_line(it.get("awayTeamOdds"))
        if not (np.isnan(h) and np.isnan(a)):
            return row.event_id, h, a
    return row.event_id, np.nan, np.nan


with ThreadPoolExecutor(max_workers=8) as pool:
    odds = list(pool.map(load_odds, games_df.itertuples()))

odds_df = pd.DataFrame(odds, columns=["event_id", "home_ml", "away_ml"])
games_df = games_df.merge(odds_df, on="event_id", how="left")
print(f"Games with a moneyline: {games_df['home_ml'].notna().sum()} / {len(games_df)}")

# COMMAND ----------

# DBTITLE 1,Optional: override with FanDuel lines from The Odds API
if ODDS_API_KEY:
    resp = SESSION.get(
        "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds",
        params={"apiKey": ODDS_API_KEY, "regions": "us", "markets": "h2h",
                "bookmakers": "fanduel", "oddsFormat": "american"},
        timeout=20,
    )
    resp.raise_for_status()
    print("The Odds API requests remaining:", resp.headers.get("x-requests-remaining"))
    replaced = 0
    for g in resp.json():
        home, away = FULLNAME_TO_SHEET.get(g["home_team"]), FULLNAME_TO_SHEET.get(g["away_team"])
        when = pd.to_datetime(g["commence_time"], utc=True)
        prices = {}
        for bm in g.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk["key"] == "h2h":
                    prices = {FULLNAME_TO_SHEET.get(o["name"]): o["price"] for o in mk["outcomes"]}
        if not (home and away and prices):
            continue
        mask = (games_df["home"] == home) & (games_df["away"] == away) & \
               ((games_df["date"] - when).abs() <= pd.Timedelta(days=3))
        if mask.any():
            games_df.loc[mask, "home_ml"] = prices.get(home, np.nan)
            games_df.loc[mask, "away_ml"] = prices.get(away, np.nan)
            replaced += int(mask.sum())
    print(f"Replaced {replaced} games with FanDuel lines")

# COMMAND ----------

# DBTITLE 1,Futures from the ESPN article table
FUTURE_COLS = ["Super_Bowl", "Conference", "Division", "Wins", "Over_Wins",
               "Under_Wins", "Playoffs_Yes", "Playoffs_No"]


def futures_from_article(url):
    html = SESSION.get(url, timeout=20).text
    tables = pd.read_html(html)
    tbl = next(t for t in tables if any("Super Bowl" in str(c) for c in t.columns))
    tbl.columns = [str(c).strip() for c in tbl.columns]
    col = lambda key: next(c for c in tbl.columns if key.lower() in c.lower())

    rows = []
    for _, r in tbl.iterrows():
        team = FULLNAME_TO_SHEET.get(str(r[col("Team")]).strip())
        if not team:
            continue
        wins = str(r[col("Wins")])
        m = re.match(r"\s*([\d.]+)\s*\(\s*([^\s,]+)\s*O\s*,\s*([^\s)]+)\s*U", wins)
        po = str(r[col("Playoffs")]).split("|")
        rows.append({
            "Team": team,
            "Super_Bowl": parse_odds(r[col("Super Bowl")]),
            "Conference": parse_odds(r[col("Conf")]),
            "Division": parse_odds(r[col("Div")]),
            "Wins": float(m.group(1)) if m else np.nan,
            "Over_Wins": parse_odds(m.group(2)) if m else np.nan,
            "Under_Wins": parse_odds(m.group(3)) if m else np.nan,
            "Playoffs_Yes": parse_odds(po[0]) if len(po) > 0 else np.nan,
            "Playoffs_No": parse_odds(po[1]) if len(po) > 1 else np.nan,
        })
    return pd.DataFrame(rows)


def futures_from_api():
    """Fallback: ESPN futures API. Has Super Bowl, conference, division and make-playoffs,
    but no win totals (those are left blank and median-filled by Odds Predictions)."""
    base = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{SEASON}/futures"
    items = get_json(base, params={"limit": 100}).get("items", [])
    team_cache = {}

    def team_from_ref(ref):
        if ref not in team_cache:
            team_cache[ref] = sheet_abbr(get_json(ref.replace("http://", "https://"))["abbreviation"])
        return team_cache[ref]

    out = {}
    for it in items:
        kind = it.get("type")
        name = (it.get("name") or "").lower()
        target = {"winLeague": "Super_Bowl", "winConference": "Conference",
                  "winDivision": "Division"}.get(kind)
        if target is None and "make the playoffs" in name:
            target = "Playoffs_Yes"
        if target is None:
            continue
        for fut in it.get("futures", [])[:1]:
            for b in fut.get("books", []):
                ref = b.get("team", {}).get("$ref")
                if ref:
                    out.setdefault(team_from_ref(ref), {})[target] = parse_odds(b.get("value"))
    df_ = pd.DataFrame.from_dict(out, orient="index").rename_axis("Team").reset_index()
    for c in FUTURE_COLS:
        if c not in df_.columns:
            df_[c] = np.nan
    return df_[["Team"] + FUTURE_COLS]


futures = pd.DataFrame()
if ESPN_FUTURES_URL:
    try:
        futures = futures_from_article(ESPN_FUTURES_URL)
        print(f"Futures from ESPN article: {len(futures)} teams")
    except Exception as e:
        print(f"Couldn't read the ESPN article ({e}); using ESPN's futures API instead")
if len(futures) < 32:
    api_futures = futures_from_api()
    futures = api_futures if futures.empty else pd.concat(
        [futures, api_futures[~api_futures["Team"].isin(futures["Team"])]], ignore_index=True)
    print(f"Futures after API fallback: {len(futures)} teams")

futures

# COMMAND ----------

# DBTITLE 1,Build the CSV Export table (one row per team per game)
home_rows = pd.DataFrame({
    "Week": games_df["Week"], "Team": games_df["home"], "Opp": games_df["away"],
    "Team_Opp": games_df["away"], "Home": 1, "Target": games_df["home_ml"],
})
away_rows = pd.DataFrame({
    "Week": games_df["Week"], "Team": games_df["away"], "Opp": games_df["home"],
    "Team_Opp": "@" + games_df["home"], "Home": 0, "Target": games_df["away_ml"],
})
rows = pd.concat([home_rows, away_rows], ignore_index=True)

opp_futures = futures.rename(columns={c: f"{c}_Opp" for c in FUTURE_COLS}).rename(columns={"Team": "Opp"})
rows = rows.merge(futures, on="Team", how="left").merge(opp_futures, on="Opp", how="left")

columns = (["Week", "Team", "Team_Opp"] + FUTURE_COLS + ["Home"]
           + [f"{c}_Opp" for c in FUTURE_COLS] + ["Target"])
csv_export = rows[columns].sort_values(["Week", "Team"]).reset_index(drop=True)
csv_export["Week"] = csv_export["Week"].astype(int)
csv_export["Home"] = csv_export["Home"].astype(int)
for c in columns[3:]:
    if c != "Home":
        csv_export[c] = csv_export[c].astype(float)

print(f"{len(csv_export)} rows, {csv_export['Target'].notna().sum()} with a moneyline, "
      f"{csv_export['Target'].isna().sum()} to be predicted")
display(csv_export)

# COMMAND ----------

# DBTITLE 1,Write to the same table the Google Sheet used to feed
spark.createDataFrame(csv_export).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(OUTPUT_TABLE)
print(f"Wrote {OUTPUT_TABLE}")
