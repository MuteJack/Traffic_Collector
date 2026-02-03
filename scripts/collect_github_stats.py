import os, csv, json, datetime, requests

GH_TOKEN = os.environ["GH_TOKEN"]
TARGET_REPOS = [x.strip() for x in os.environ["TARGET_REPOS"].split(",") if x.strip()]

HEADERS = {
  "Accept": "application/vnd.github+json",
  "Authorization": f"Bearer {GH_TOKEN}",
  "X-GitHub-Api-Version": "2022-11-28",
}

def get(url):
  r = requests.get(url, headers=HEADERS, timeout=30)
  r.raise_for_status()
  return r.json()

def utc_today():
  return datetime.datetime.utcnow().date().isoformat()  # YYYY-MM-DD

def append_csv(path, fieldnames, row):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  exists = os.path.exists(path)
  with open(path, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists: w.writeheader()
    w.writerow(row)

def collect_traffic(owner, repo):
  # views/visitors last 14 days
  views = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/views")
  # {"count":..,"uniques":..,"views":[{"timestamp":"...","count":..,"uniques":..}, ...]}
  # pick today's bucket (UTC aligned)
  today = utc_today()
  todays = None
  for v in views.get("views", []):
    if v["timestamp"].startswith(today):
      todays = v
      break
  if not todays:
    # if no bucket yet (repo inactive), treat as 0
    todays = {"count": 0, "uniques": 0}

  append_csv(
    "stats/traffic_daily.csv",
    ["date","repo","views","unique_visitors"],
    {"date": today, "repo": f"{owner}/{repo}", "views": todays["count"], "unique_visitors": todays["uniques"]}
  )

def collect_release_downloads(owner, repo):
  releases = get(f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=100")
  today = utc_today()

  for rel in releases:
    tag = rel.get("tag_name","")
    for a in rel.get("assets", []):
      append_csv(
        "stats/releases_daily.csv",
        ["date","repo","tag","asset_name","download_count"],
        {"date": today, "repo": f"{owner}/{repo}", "tag": tag,
         "asset_name": a.get("name",""), "download_count": a.get("download_count",0)}
      )

def main():
  for full in TARGET_REPOS:
    owner, repo = full.split("/", 1)
    print(f"Processing {owner}/{repo}...")

    try:
      collect_traffic(owner, repo)
      print(f"  Traffic data collected")
    except Exception as e:
      print(f"  Warning: Failed to collect traffic - {e}")

    try:
      collect_release_downloads(owner, repo)
      print(f"  Release data collected")
    except Exception as e:
      print(f"  Warning: Failed to collect releases - {e}")

if __name__ == "__main__":
  main()
