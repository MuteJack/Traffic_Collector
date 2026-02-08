import os, csv, json, datetime, requests
from collections import defaultdict
from urllib.parse import urlparse

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

def parse_repo(raw):
  """Parse 'owner/repo' or 'https://github.com/owner/repo' into (owner, repo)."""
  if raw.startswith("http"):
    path = urlparse(raw).path.strip("/")
    return path.split("/", 1)
  return raw.split("/", 1)

def load_existing_keys(path, key_fields):
  """Load existing key combinations from CSV to avoid duplicates."""
  keys = set()
  if os.path.exists(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
      for row in csv.DictReader(f):
        keys.add(tuple(row.get(k, "") for k in key_fields))
  return keys

def append_csv(path, fieldnames, row):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  exists = os.path.exists(path)
  with open(path, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists: w.writeheader()
    w.writerow(row)

# --- Traffic: Views ---
def collect_views(owner, repo, existing_keys):
  views = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/views")
  repo_full = f"{owner}/{repo}"
  added = 0
  for v in views.get("views", []):
    date = v["timestamp"][:10]
    if (date, repo_full) in existing_keys:
      continue
    append_csv("stats/traffic_views.csv",
      ["date","repo","views","unique_visitors"],
      {"date": date, "repo": repo_full, "views": v["count"], "unique_visitors": v["uniques"]})
    existing_keys.add((date, repo_full))
    added += 1
  return added

# --- Traffic: Clones ---
def collect_clones(owner, repo, existing_keys):
  clones = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/clones")
  repo_full = f"{owner}/{repo}"
  added = 0
  for c in clones.get("clones", []):
    date = c["timestamp"][:10]
    if (date, repo_full) in existing_keys:
      continue
    append_csv("stats/traffic_clones.csv",
      ["date","repo","clones","unique_cloners"],
      {"date": date, "repo": repo_full, "clones": c["count"], "unique_cloners": c["uniques"]})
    existing_keys.add((date, repo_full))
    added += 1
  return added

# --- Traffic: Popular Referrers ---
def collect_referrers(owner, repo, existing_keys):
  refs = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/popular/referrers")
  today = utc_today()
  repo_full = f"{owner}/{repo}"
  added = 0
  for r in refs:
    referrer = r.get("referrer", "")
    if (today, repo_full, referrer) in existing_keys:
      continue
    append_csv("stats/traffic_referrers.csv",
      ["date","repo","referrer","views","unique_visitors"],
      {"date": today, "repo": repo_full, "referrer": referrer,
       "views": r["count"], "unique_visitors": r["uniques"]})
    existing_keys.add((today, repo_full, referrer))
    added += 1
  return added

# --- Traffic: Popular Paths ---
def collect_paths(owner, repo, existing_keys):
  paths = get(f"https://api.github.com/repos/{owner}/{repo}/traffic/popular/paths")
  today = utc_today()
  repo_full = f"{owner}/{repo}"
  added = 0
  for p in paths:
    path = p.get("path", "")
    if (today, repo_full, path) in existing_keys:
      continue
    append_csv("stats/traffic_paths.csv",
      ["date","repo","path","title","views","unique_visitors"],
      {"date": today, "repo": repo_full, "path": path, "title": p.get("title",""),
       "views": p["count"], "unique_visitors": p["uniques"]})
    existing_keys.add((today, repo_full, path))
    added += 1
  return added

# --- Releases ---
def collect_releases(owner, repo, existing_keys):
  releases = get(f"https://api.github.com/repos/{owner}/{repo}/releases?per_page=100")
  today = utc_today()
  repo_full = f"{owner}/{repo}"
  added = 0
  for rel in releases:
    tag = rel.get("tag_name","")
    for a in rel.get("assets", []):
      asset_name = a.get("name","")
      if (today, repo_full, tag, asset_name) in existing_keys:
        continue
      append_csv("stats/releases_daily.csv",
        ["date","repo","tag","asset_name","download_count"],
        {"date": today, "repo": repo_full, "tag": tag,
         "asset_name": asset_name, "download_count": a.get("download_count",0)})
      existing_keys.add((today, repo_full, tag, asset_name))
      added += 1
  return added

COLLECTORS = [
  ("Views",     "stats/traffic_views.csv",     ["date","repo"],                    collect_views),
  ("Clones",    "stats/traffic_clones.csv",     ["date","repo"],                    collect_clones),
  ("Referrers", "stats/traffic_referrers.csv",  ["date","repo","referrer"],         collect_referrers),
  ("Paths",     "stats/traffic_paths.csv",      ["date","repo","path"],             collect_paths),
  ("Releases",  "stats/releases_daily.csv",     ["date","repo","tag","asset_name"], collect_releases),
]

def read_csv(path):
  """Read all rows from a CSV file."""
  if not os.path.exists(path):
    return []
  with open(path, "r", newline="", encoding="utf-8") as f:
    return list(csv.DictReader(f))

def write_csv(path, fieldnames, rows):
  """Overwrite a CSV file with sorted rows."""
  os.makedirs(os.path.dirname(path), exist_ok=True)
  with open(path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

def generate_summary():
  """Generate per-repo cumulative totals → stats/summary.csv"""
  totals = defaultdict(lambda: {"total_views":0, "total_unique_visitors":0,
    "total_clones":0, "total_unique_cloners":0, "total_downloads":0, "last_date":""})

  for row in read_csv("stats/traffic_views.csv"):
    r = totals[row["repo"]]
    r["total_views"] += int(row.get("views",0))
    r["total_unique_visitors"] += int(row.get("unique_visitors",0))
    r["last_date"] = max(r["last_date"], row["date"])

  for row in read_csv("stats/traffic_clones.csv"):
    r = totals[row["repo"]]
    r["total_clones"] += int(row.get("clones",0))
    r["total_unique_cloners"] += int(row.get("unique_cloners",0))
    r["last_date"] = max(r["last_date"], row["date"])

  for row in read_csv("stats/releases_daily.csv"):
    r = totals[row["repo"]]
    r["total_downloads"] += int(row.get("download_count",0))
    r["last_date"] = max(r["last_date"], row["date"])

  fields = ["repo","total_views","total_unique_visitors","total_clones","total_unique_cloners","total_downloads","last_date"]
  rows = [{"repo": repo, **vals} for repo, vals in sorted(totals.items())]
  write_csv("stats/summary.csv", fields, rows)
  print(f"Summary: {len(rows)} repos")

def generate_monthly():
  """Generate monthly aggregates → stats/monthly.csv"""
  buckets = defaultdict(lambda: {"views":0, "unique_visitors":0, "clones":0, "unique_cloners":0, "downloads":0})

  for row in read_csv("stats/traffic_views.csv"):
    month = row["date"][:7]  # YYYY-MM
    b = buckets[(row["repo"], month)]
    b["views"] += int(row.get("views",0))
    b["unique_visitors"] += int(row.get("unique_visitors",0))

  for row in read_csv("stats/traffic_clones.csv"):
    month = row["date"][:7]
    b = buckets[(row["repo"], month)]
    b["clones"] += int(row.get("clones",0))
    b["unique_cloners"] += int(row.get("unique_cloners",0))

  for row in read_csv("stats/releases_daily.csv"):
    month = row["date"][:7]
    b = buckets[(row["repo"], month)]
    b["downloads"] += int(row.get("download_count",0))

  fields = ["repo","month","views","unique_visitors","clones","unique_cloners","downloads"]
  rows = [{"repo": k[0], "month": k[1], **v} for k, v in sorted(buckets.items())]
  write_csv("stats/monthly.csv", fields, rows)
  print(f"Monthly: {len(rows)} entries")

def generate_json():
  """Generate JSON for blog charts → stats/summary.json"""
  data = {"generated": utc_today(), "repos": {}}

  for row in read_csv("stats/summary.csv"):
    repo = row["repo"]
    data["repos"][repo] = {
      "total_views": int(row.get("total_views",0)),
      "total_unique_visitors": int(row.get("total_unique_visitors",0)),
      "total_clones": int(row.get("total_clones",0)),
      "total_unique_cloners": int(row.get("total_unique_cloners",0)),
      "total_downloads": int(row.get("total_downloads",0)),
      "daily_views": [],
      "daily_clones": [],
      "monthly": [],
    }

  for row in read_csv("stats/traffic_views.csv"):
    repo = row["repo"]
    if repo in data["repos"]:
      data["repos"][repo]["daily_views"].append({
        "date": row["date"], "views": int(row.get("views",0)),
        "unique": int(row.get("unique_visitors",0))})

  for row in read_csv("stats/traffic_clones.csv"):
    repo = row["repo"]
    if repo in data["repos"]:
      data["repos"][repo]["daily_clones"].append({
        "date": row["date"], "clones": int(row.get("clones",0)),
        "unique": int(row.get("unique_cloners",0))})

  for row in read_csv("stats/monthly.csv"):
    repo = row["repo"]
    if repo in data["repos"]:
      data["repos"][repo]["monthly"].append({
        "month": row["month"],
        "views": int(row.get("views",0)),
        "clones": int(row.get("clones",0)),
        "downloads": int(row.get("downloads",0))})

  os.makedirs("stats", exist_ok=True)
  with open("stats/summary.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
  print(f"JSON: {len(data['repos'])} repos")

def main():
  # Load existing keys for each CSV
  all_keys = {}
  for name, path, key_fields, _ in COLLECTORS:
    all_keys[name] = load_existing_keys(path, key_fields)

  for raw in TARGET_REPOS:
    owner, repo = parse_repo(raw)
    print(f"Processing {owner}/{repo}...")

    for name, _, _, collect_fn in COLLECTORS:
      try:
        added = collect_fn(owner, repo, all_keys[name])
        print(f"  {name}: {added} new records")
      except Exception as e:
        print(f"  {name}: SKIP - {e}")

  # Generate summaries
  print("\nGenerating summaries...")
  generate_summary()
  generate_monthly()
  generate_json()

if __name__ == "__main__":
  main()
