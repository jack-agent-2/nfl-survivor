# NFL Survivor: one-click Databricks job

The job runs three notebooks in order. No Google Sheet step.

| # | Task key | Notebook | What it does |
|---|---|---|---|
| 1 | `ingest_odds` | `Ingest Odds` | Pulls the schedule, moneylines and futures from ESPN, then writes `workspace.default.nfl_survivor_csv_export` |
| 2 | `odds_predictions` | `Odds Predictions` | Predicts moneylines for games with no line yet, then writes `nfl_survivor_predictions` |
| 3 | `picks` | `Picks` | Builds the pick plan, then writes `nfl_survivor_optimal_picks` (and shows it in the run output) |

## Setup (once)

1. Import `Ingest Odds.py` into the same workspace folder as your other two notebooks (Workspace, then ⋮, then Import), and re-import the updated `Odds Predictions.py` and `Picks.ipynb`.
2. Go to **Jobs & Pipelines**, then **Create**, then **Job**.
3. Add task `ingest_odds` (type Notebook, path = `Ingest Odds`, serverless compute).
4. Add task `odds_predictions` (Notebook `Odds Predictions`), with **Depends on** set to `ingest_odds`.
5. Add task `picks` (Notebook `Picks`), with **Depends on** set to `odds_predictions`.
6. Add these **job parameters**. Databricks passes them to every notebook's widgets with the same name:

| Parameter | Default | Notes |
|---|---|---|
| `current_week` | `auto` | `auto` uses the current NFL week from ESPN. Or set a number. |
| `buybacks_used` | `0` | 0, 1 or 2 |
| `used_teams_manual` | *(blank)* | Teams you've already picked, comma separated, in sheet style, e.g. `LAC,SF,KC,Min` |
| `odds_api_key` | *(blank)* | Optional. A key from the-odds-api.com, which swaps ESPN's DraftKings lines for FanDuel lines on upcoming games |
| `espn_futures_url` | the ESPN Super Bowl odds article | ESPN posts a new article every week or so. Paste the newest URL here. If the article can't be read, the job falls back to ESPN's futures API, which has no win totals. |

7. Optional: add a **Schedule** trigger (for example, Tuesdays at 9 AM) so the job runs each week by itself.

Each week, update `used_teams_manual` (and `buybacks_used` if needed), then click **Run now**. The picks show up in the `picks` task's output and in `workspace.default.nfl_survivor_optimal_picks`.

## Team abbreviations
These match your sheet: Ari, Atl, Bal, Buf, Car, Chi, Cin, Cle, Dal, Den, Det, GB, Hou, Ind, Jac, KC, LAC, LAR, LV, Mia, Min, NE, NO, NYG, NYJ, Phi, Pit, SF, Sea, TB, Ten, Was.

## Free Edition: get the data through GitHub (browser only)

Free Edition blocks ESPN but allows GitHub. So GitHub fetches ESPN for you every day and saves the data in a repo, and Databricks reads it from there. You don't need your PC or a Databricks token.

**1. Create the repo.** On github.com, click **New repository**, name it `nfl-survivor-data`, choose **Public**, check **Add a README**, and click **Create**. Public is easiest because Databricks can read it with no token, and the data is just public odds.

**2. Add the fetch script.** In the repo, click **Add file**, then **Upload files**. Drag in `fetch_nfl_data.py` from your NFL Survivor folder, then click **Commit changes**.

**3. Add the schedule.** Click **Add file**, then **Create new file**. Type the name exactly as `.github/workflows/fetch-nfl-data.yml` (the slashes create the folders). Open `fetch-nfl-data.yml` from your folder in Notepad, copy everything, paste it in, and click **Commit changes**.

**4. Run it once.** Go to the **Actions** tab, then **Fetch NFL survivor data**, then **Run workflow**. After about a minute, you should see a green check and a new `data/` folder in the repo with `nfl_survivor_csv_export.csv` and `current_week.txt`. After that, it runs every day at 8 AM Eastern.

**5. Point Databricks at it.** Add this job parameter:

| Parameter | Value |
|---|---|
| `github_data_url` | `https://raw.githubusercontent.com/<your-github-username>/nfl-survivor-data/main/data` |

Leave `data_source` set to `auto`. `Ingest Odds` sees that ESPN is blocked and reads from GitHub instead. Then give the Databricks job its own **Schedule** trigger an hour after the GitHub run, for example 9 AM Eastern. That makes the whole thing hands-off.

**Optional:**
- **FanDuel lines:** in the repo, go to **Settings**, then **Secrets and variables**, then **Actions**, then **New repository secret**. Name it `ODDS_API_KEY` and paste your key from the-odds-api.com.
- **Newer ESPN futures article:** edit `.github/workflows/fetch-nfl-data.yml` on GitHub and add `--espn-futures-url "<new link>"` to the end of the `python fetch_nfl_data.py` line.
