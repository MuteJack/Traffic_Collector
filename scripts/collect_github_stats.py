import os, csv, datetime, requests

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
  return datetime.datetime.utcnow().date().isoformat()

def load_existing_keys(path, key_fields):
  """Load existing (date, repo, ...) combinations from CSV to avoid duplicates."""
  keys = set()
  if os.path.exists(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
      reader = csv.DictReader(f)
      for row in reader:
        key = tuple(row.get(k, "") for k in key_fields)
        keys.add(key)
  return keys

def append_csv(path, fieldnames, row):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  exists = os.path.exists(path)
  with open(path, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists: w.writeheader()
    w.writerow(row)

def collect_traffic(owner, repo, existing_keys):
  """Collect all available traffic data (up to 14 days) and backfill missing dates."""
  views = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/views")
  repo_full = f"{owner}/{repo}"
  added = 0

  for v in views.get("views", []):
    # timestamp format: "2025-02-03T00:00:00Z"
    date = v["timestamp"][:10]
    key = (date, repo_full)

    if key in existing_keys:
      continue

    append_csv(
      "stats/traffic_daily.csv",
      ["date","repo","views","unique_visitors"],
      {"date": date, "repo": repo_full, "views": v["count"], "unique_visitors": v["uniques"]}
    )
    existing_keys.add(key)
    added += 1

  return added

def collect_release_downloads(owner, repo, existing_keys):
  """Collect release download counts, avoiding duplicates."""
  releases = get(f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=100")
  today = utc_today()
  repo_full = f"{owner}/{repo}"
  added = 0

  for rel in releases:
    tag = rel.get("tag_name","")
    for a in rel.get("assets", []):
      asset_name = a.get("name","")
      key = (today, repo_full, tag, asset_name)

      if key in existing_keys:
        continue

      append_csv(
        "stats/releases_daily.csv",
        ["date","repo","tag","asset_name","download_count"],
        {"date": today, "repo": repo_full, "tag": tag,
         "asset_name": asset_name, "download_count": a.get("download_count",0)}
      )
      existing_keys.add(key)
      added += 1

  return added

def main():
  # Load existing data to prevent duplicates
  traffic_keys = load_existing_keys("stats/traffic_daily.csv", ["date", "repo"])
  release_keys = load_existing_keys("stats/releases_daily.csv", ["date", "repo", "tag", "asset_name"])

  for full in TARGET_REPOS:
    owner, repo = full.split("/", 1)
    print(f"Processing {owner}/{repo}...")

    try:
      added = collect_traffic(owner, repo, traffic_keys)
      print(f"  Traffic: {added} new records")
    except Exception as e:
      print(f"  Warning: Failed to collect traffic - {e}")

    try:
      added = collect_release_downloads(owner, repo, release_keys)
      print(f"  Releases: {added} new records")
    except Exception as e:
      print(f"  Warning: Failed to collect releases - {e}")

if __name__ == "__main__":
  main()
