"""
Fetch NFL survivor data on your own PC and upload it to Databricks.

Runs in GitHub Actions (see .github/workflows/fetch-nfl-data.yml) with --no-upload --out-dir data,
so Databricks Free Edition can read the result from raw.githubusercontent.com.
It can also run on a normal PC and upload straight to a Databricks volume.
It does the same work as the "Ingest Odds" notebook: it pulls the schedule, moneylines and
futures from ESPN, builds the CSV Export table, and then:
  1. saves nfl_survivor_csv_export.csv and current_week.txt next to this script, plus odds history:
     history/YYYY-MM-DD.csv (today's snapshot) and odds_history.csv (every day so far, long format)
  2. uploads both to the Databricks volume /Volumes/workspace/default/nfl_survivor/
  3. optionally starts your Databricks job (--job-id)

One-time setup (Windows, in a terminal):
  pip install requests pandas lxml databricks-sdk
  Create a personal access token in Databricks (your profile picture > Settings > Developer > Access tokens), then
  setx DATABRICKS_HOST "https://<your-workspace>.cloud.databricks.com"
  setx DATABRICKS_TOKEN "<token>"
  (open a new terminal afterwards)

Run:
  python fetch_nfl_data.py --job-id 123456789
"""
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--season", type=int, default=2026)
parser.add_argument("--max-week", type=int, default=18)
parser.add_argument("--espn-futures-url", default="https://www.espn.com/espn/betting/story/_/id/47809892/2026-super-bowl-lxi-odds-nfl-sports-betting-season-win-totals-playoffs")
parser.add_argument("--odds-api-key", default=os.environ.get("ODDS_API_KEY", ""), help="Optional, for FanDuel lines")
parser.add_argument("--volume", default="/Volumes/workspace/default/nfl_survivor")
parser.add_argument("--job-id", type=int, default=None, help="Databricks job to start after uploading")
parser.add_argument("--no-upload", action="store_true", help="Only write the CSV locally")
parser.add_argument("--out-dir", default=None, help="Folder to write the CSV to (default: next to this script)")
args = parser.parse_args()

SEASON = args.season
MAX_WEEK = args.max_week
ESPN_FUTURES_URL = args.espn_futures_url.strip()
ODDS_API_KEY = args.odds_api_key.strip()
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

print(f"ESPN season type {season_type}, week {espn_week} -> current_week = {current_week}")

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"


def load_week(week):
    data = get_json(SCOREBOARD, params={"seasontype": 2, "week": week, "dates": SEASON, "limit": 50})
    games = []
    for ev in data.get("events", []):
        comp = ev["competitions"][0]
        teams = {c["homeAway"]: c for c in comp["competitors"]}
        if "home" not in teams or "away" not in teams:
            continue
        done = bool(comp.get("status", ev.get("status", {})).get("type", {}).get("completed", False))
        games.append({
            "Week": week,
            "event_id": ev["id"],
            "comp_id": comp["id"],
            "date": pd.to_datetime(ev["date"], utc=True),
            "home": sheet_abbr(teams["home"]["team"]["abbreviation"]),
            "away": sheet_abbr(teams["away"]["team"]["abbreviation"]),
            # Final scores of finished games, used by Odds Predictions for recent point differential
            "home_score": pd.to_numeric(teams["home"].get("score"), errors="coerce") if done else np.nan,
            "away_score": pd.to_numeric(teams["away"].get("score"), errors="coerce") if done else np.nan,
        })
    return games


with ThreadPoolExecutor(max_workers=6) as pool:
    games = [g for wk in pool.map(load_week, range(1, MAX_WEEK + 1)) for g in wk]

games_df = pd.DataFrame(games)
print(f"Loaded {len(games_df)} games across {games_df['Week'].nunique()} weeks")

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

# ---- Current team strength: ESPN Football Power Index (FPI) ----
# FPI is ESPN's rating of how good each team is right now (points better than an average team on a
# neutral field), and it reacts to injuries such as a starting QB going out. Free, no key needed.
# If ESPN changes the endpoint, FPI is left blank and Odds Predictions simply doesn't use it.
EXTRA_COLS = ["FPI", "FPI_Proj_Wins", "FPI_Opp", "FPI_Proj_Wins_Opp", "Team_Score", "Opp_Score"]


def _num(v):
    try:
        return float(str(v).replace("+", "").replace(",", ""))
    except (TypeError, ValueError):
        return np.nan


def fpi_from_site_api():
    data = get_json("https://site.web.api.espn.com/apis/fitt/v3/sports/football/nfl/powerindex",
                    params={"region": "us", "lang": "en", "season": SEASON, "limit": 50})
    # Stat names for each category, e.g. {"fpi": ["fpi", "fpirk", "trend", "projw", ...]}
    names = {c.get("name"): [str(n).lower() for n in (c.get("names") or [])]
             for c in data.get("categories", [])}
    out = []
    for t in data.get("teams", []):
        team = sheet_abbr((t.get("team") or {}).get("abbreviation", ""))
        stats = {}
        for c in t.get("categories", []):
            vals = c.get("values") or c.get("totals") or []
            stats.update(zip(names.get(c.get("name"), []), vals))
        out.append({"Team": team, "FPI": _num(stats.get("fpi")),
                    "FPI_Proj_Wins": _num(stats.get("projw", stats.get("projectedwins")))})
    return pd.DataFrame(out)


def fpi_from_core_api():
    data = get_json(f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{SEASON}/powerindex",
                    params={"limit": 50})
    out = []
    for it in data.get("items", []):
        if "stats" not in it and "$ref" in it:
            it = get_json(it["$ref"].replace("http://", "https://"))
        ref = (it.get("team") or {}).get("$ref")
        if not ref:
            continue
        team = sheet_abbr(get_json(ref.replace("http://", "https://"))["abbreviation"])
        stats = {str(s.get("name", "")).lower(): s.get("value") for s in it.get("stats", [])}
        out.append({"Team": team, "FPI": _num(stats.get("fpi")),
                    "FPI_Proj_Wins": _num(stats.get("projw", stats.get("projectedwins")))})
    return pd.DataFrame(out)


fpi = pd.DataFrame(columns=["Team", "FPI", "FPI_Proj_Wins"])
for source in (fpi_from_site_api, fpi_from_core_api):
    try:
        got = source()
        if got["FPI"].notna().sum() >= 28:
            fpi = got.dropna(subset=["FPI"]).drop_duplicates("Team")
            print(f"FPI from {source.__name__}: {len(fpi)} teams")
            break
        print(f"FPI from {source.__name__} had only {got['FPI'].notna().sum()} teams; trying the next source")
    except Exception as e:
        print(f"Couldn't read FPI from {source.__name__} ({e})")
if fpi.empty:
    print("No FPI this run; Odds Predictions will skip it")
fpi = fpi[["Team", "FPI", "FPI_Proj_Wins"]].astype({"FPI": float, "FPI_Proj_Wins": float})

home_rows = pd.DataFrame({
    "Week": games_df["Week"], "Team": games_df["home"], "Opp": games_df["away"],
    "Team_Opp": games_df["away"], "Home": 1, "Target": games_df["home_ml"],
    "Team_Score": games_df["home_score"], "Opp_Score": games_df["away_score"],
})
away_rows = pd.DataFrame({
    "Week": games_df["Week"], "Team": games_df["away"], "Opp": games_df["home"],
    "Team_Opp": "@" + games_df["home"], "Home": 0, "Target": games_df["away_ml"],
    "Team_Score": games_df["away_score"], "Opp_Score": games_df["home_score"],
})
rows = pd.concat([home_rows, away_rows], ignore_index=True)

opp_futures = futures.rename(columns={c: f"{c}_Opp" for c in FUTURE_COLS}).rename(columns={"Team": "Opp"})
rows = rows.merge(futures, on="Team", how="left").merge(opp_futures, on="Opp", how="left")
rows = rows.merge(fpi, on="Team", how="left").merge(
    fpi.rename(columns={"Team": "Opp", "FPI": "FPI_Opp", "FPI_Proj_Wins": "FPI_Proj_Wins_Opp"}), on="Opp", how="left")

columns = (["Week", "Team", "Team_Opp"] + FUTURE_COLS + ["Home"]
           + [f"{c}_Opp" for c in FUTURE_COLS] + EXTRA_COLS + ["Target"])
csv_export = rows[columns].sort_values(["Week", "Team"]).reset_index(drop=True)
csv_export["Week"] = csv_export["Week"].astype(int)
csv_export["Home"] = csv_export["Home"].astype(int)
for c in columns[3:]:
    if c != "Home":
        csv_export[c] = csv_export[c].astype(float)

print(f"{len(csv_export)} rows, {csv_export['Target'].notna().sum()} with a moneyline, "
      f"{csv_export['Target'].isna().sum()} to be predicted")

# ---- Save locally ----
here = args.out_dir or os.path.dirname(os.path.abspath(__file__))
os.makedirs(here, exist_ok=True)
csv_path = os.path.join(here, "nfl_survivor_csv_export.csv")
week_path = os.path.join(here, "current_week.txt")
csv_export.to_csv(csv_path, index=False)
with open(week_path, "w") as f:
    f.write(str(current_week))
print(f"Saved {csv_path}")

# ---- Odds history: a dated snapshot plus one combined long-format file ----
# Lets Odds Predictions and the robustness simulation learn how lines really move.
# One row per team per game per fetch day. Running twice on the same day replaces that day's rows.
fetched_at = pd.Timestamp.now(tz="UTC")
fetched_date = fetched_at.strftime("%Y-%m-%d")
kickoffs = pd.concat([
    games_df[["Week", "home", "date"]].rename(columns={"home": "Team"}),
    games_df[["Week", "away", "date"]].rename(columns={"away": "Team"}),
]).drop_duplicates(["Week", "Team"])
history = csv_export.merge(kickoffs, on=["Week", "Team"], how="left")
history.insert(0, "fetched_date", fetched_date)
history.insert(1, "fetched_at", fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"))
history.insert(2, "season", SEASON)
history.insert(3, "current_week", current_week)
history["Opp"] = history["Team_Opp"].astype(str).str.lstrip("@")
history["kickoff"] = history.pop("date").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
history["days_to_kickoff"] = ((pd.to_datetime(history["kickoff"], utc=True) - fetched_at)
                              .dt.total_seconds() / 86400).round(2)
history = history.rename(columns={"Target": "moneyline"})
history_cols = (["fetched_date", "fetched_at", "season", "current_week", "Week", "Team", "Opp", "Team_Opp",
                 "Home", "kickoff", "days_to_kickoff", "moneyline"]
                + FUTURE_COLS + [f"{c}_Opp" for c in FUTURE_COLS] + EXTRA_COLS)
history = history[history_cols]

history_dir = os.path.join(here, "history")
os.makedirs(history_dir, exist_ok=True)
snapshot_path = os.path.join(history_dir, f"{fetched_date}.csv")
history.to_csv(snapshot_path, index=False)
print(f"Saved {snapshot_path}")

combined_path = os.path.join(here, "odds_history.csv")
if os.path.exists(combined_path):
    old = pd.read_csv(combined_path)
    old = old[old["fetched_date"].astype(str) != fetched_date]
    combined = pd.concat([old, history], ignore_index=True)
else:
    combined = history
combined.to_csv(combined_path, index=False)
print(f"Saved {combined_path}: {len(combined)} rows over {combined['fetched_date'].nunique()} day(s)")

# ---- Upload to Databricks and start the job ----
if not args.no_upload:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_TOKEN
    catalog, schema, volume = args.volume.strip("/").split("/")[1:4]
    try:
        w.volumes.read(f"{catalog}.{schema}.{volume}")
    except Exception:
        from databricks.sdk.service.catalog import VolumeType
        w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume,
                         volume_type=VolumeType.MANAGED)
        print(f"Created volume {catalog}.{schema}.{volume}")
    for local, name in ((csv_path, "nfl_survivor_csv_export.csv"), (week_path, "current_week.txt")):
        with open(local, "rb") as f:
            w.files.upload(f"{args.volume}/{name}", f, overwrite=True)
        print(f"Uploaded {args.volume}/{name}")
    if args.job_id:
        run = w.jobs.run_now(job_id=args.job_id)
        print(f"Started job {args.job_id}, run id {run.run_id}. Check the Picks task output in Databricks.")
