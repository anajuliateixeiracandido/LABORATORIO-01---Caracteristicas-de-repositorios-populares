import requests
import csv
import os
import time
import random
from datetime import datetime, timezone

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
GITHUB_API_URL = "https://api.github.com/graphql"

if not GITHUB_TOKEN:
    raise ValueError("Defina a variável de ambiente GITHUB_TOKEN")

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.github+json",
    "User-Agent": "lab01s01-repo-metrics"
}

def jitter_sleep(base):
    time.sleep(base + random.uniform(0, 1.2))

def make_request_with_retry(query, variables=None, max_retries=7, timeout=90):
    variables = variables or {}
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                GITHUB_API_URL,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=timeout
            )

            if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
                reset = resp.headers.get("X-RateLimit-Reset")
                wait = 60
                if reset:
                    wait = max(0, int(reset) - int(datetime.now(timezone.utc).timestamp())) + 2
                print(f"[RateLimit] aguardando {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code in (502, 503, 504) or (resp.status_code == 403 and "secondary rate limit" in resp.text.lower()):
                print(f"[{resp.status_code}] backoff...")
                jitter_sleep(2 ** attempt)
                continue

            if resp.status_code != 200:
                print(f"[HTTP {resp.status_code}] {resp.text[:200]}")
                jitter_sleep(2 ** attempt)
                continue

            data = resp.json()
            if "errors" in data:
                print(f"[GraphQL] {data['errors']}")
                jitter_sleep(2 ** attempt)
                continue

            return data
        except requests.exceptions.RequestException as e:
            print(f"[Network] {e}")
            jitter_sleep(2 ** attempt)
    return None

def age_days(created_at_iso):
    try:
        created = datetime.fromisoformat(created_at_iso.replace('Z', '+00:00'))
        now = datetime.now(created.tzinfo)
        return (now - created).days
    except:
        return None

def days_since_update(pushed_at_iso):
    try:
        pushed = datetime.fromisoformat(pushed_at_iso.replace('Z', '+00:00'))
        now = datetime.now(pushed.tzinfo)
        return (now - pushed).days
    except:
        return None

SEARCH_QUERY = """
query($first: Int!, $after: String) {
  search(
    query: "stars:>1 sort:stars-desc is:public archived:false fork:false",
    type: REPOSITORY,
    first: $first,
    after: $after
  ) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on Repository {
        name
        owner { login }
        createdAt
        pushedAt
        primaryLanguage { name }
        url
      }
    }
  }
}
"""

def fetch_top_repositories(n=1000):
    repos, cursor = [], None
    while len(repos) < n:
        batch = min(20, n - len(repos))
        print(f"buscando... {len(repos)}/{n}")
        data = make_request_with_retry(SEARCH_QUERY, {"first": batch, "after": cursor})
        if not data:
            print("falhou Etapa 1"); break
        search = data["data"]["search"]
        repos.extend(search["nodes"] or [])
        if not search["pageInfo"]["hasNextPage"] or len(repos) >= n:
            break
        cursor = search["pageInfo"]["endCursor"]
        time.sleep(1.6)
    return repos[:n]

DETAILS_QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: MERGED) { totalCount }
    releases { totalCount }
    issues { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
  }
}
"""

def fetch_counters(owner, name):
    data = make_request_with_retry(DETAILS_QUERY, {"owner": owner, "name": name})
    if not data or not data["data"]["repository"]:
        return None
    r = data["data"]["repository"]
    return {
        "merged_prs": r["pullRequests"]["totalCount"],
        "releases": r["releases"]["totalCount"],
        "issues_total": r["issues"]["totalCount"],
        "issues_closed": r["closedIssues"]["totalCount"]
    }

def main():
    basics = fetch_top_repositories(1000)
    if not basics:
        print("nenhum repositório coletado"); return

    rows = []
    for i, b in enumerate(basics, 1):
        owner = b["owner"]["login"]
        name = b["name"]
        created_at = b["createdAt"]
        pushed_at = b["pushedAt"]
        print(f"[{i}/{len(basics)}] {owner}/{name}")

        c = fetch_counters(owner, name) or {"merged_prs": None, "releases": None, "issues_total": 0, "issues_closed": 0}
        ratio = round(c["issues_closed"] / c["issues_total"], 4) if c["issues_total"] > 0 else 0.0

        rows.append({
            "full_name": f"{owner}/{name}",
            "url": b["url"],
            "age_days": age_days(created_at),
            "merged_prs": c["merged_prs"],
            "releases": c["releases"],
            "days_since_update": days_since_update(pushed_at),
            "primary_language": (b["primaryLanguage"]["name"] if b["primaryLanguage"] else "Unknown"),
            "issues_total": c["issues_total"],
            "issues_closed": c["issues_closed"],
            "issues_closed_ratio": ratio
        })

        time.sleep(0.6)

    fields = ["full_name", "url", "age_days", "merged_prs", "releases",
              "days_since_update", "primary_language", "issues_total",
              "issues_closed", "issues_closed_ratio"]

    with open("lab01s01_repositories.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    print("OK! CSV salvo em lab01s01_repositories.csv")

if __name__ == "__main__":
    main()
