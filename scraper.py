import requests
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client

SUPABASE_URL = "https://dvhmuoohnqbfpwylfobm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "5ad4ce8f")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "f53516c223048c8b1c9de13ce546ff3f")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SEARCHES = [
    "game developer",
    "unity developer",
    "unreal engine developer",
    "gameplay engineer",
    "game programmer",
    "game designer",
    "3d game artist",
    "game audio designer",
    "QA game tester",
    "technical game artist",
    "godot developer",
    "indie game developer",
    "multiplayer game developer",
    "game producer",
    "community manager games",
]

CATEGORY_MAP = {
    "programmer": "programming", "engineer": "programming",
    "developer": "programming", "coder": "programming",
    "artist": "art", "animator": "art", "3d": "art",
    "technical art": "art", "vfx": "art",
    "audio": "audio", "sound": "audio", "composer": "audio",
    "designer": "design", "level design": "design",
    "producer": "production", "project manager": "production",
    "qa": "qa", "tester": "qa", "quality": "qa",
    "marketing": "marketing", "community": "marketing",
    "writer": "design", "narrative": "design",
}

TAG_KEYWORDS = [
    "unity","unreal","ue5","godot","gamemaker","c++","c#","python",
    "blueprints","hlsl","shader","multiplayer","netcode","opengl",
    "vulkan","directx","mobile","vr","ar","steam","console","ps5",
    "xbox","nintendo","blender","maya","zbrush","houdini","fmod",
    "wwise","agile","remote","hybrid","on-site",
]

def guess_category(title):
    t = title.lower()
    for kw, cat in CATEGORY_MAP.items():
        if kw in t:
            return cat
    return "programming"

def extract_tags(title, desc):
    text = (title + " " + desc).lower()
    return [t for t in TAG_KEYWORDS if t in text]

def fetch_jobs():
    all_jobs = []
    for query in SEARCHES:
        for country in ["gb", "us"]:
            try:
                url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
                params = {
                    "app_id": ADZUNA_APP_ID,
                    "app_key": ADZUNA_APP_KEY,
                    "results_per_page": 20,
                    "what": query,
                    "content-type": "application/json",
                }
                r = requests.get(url, params=params, timeout=15)
                if r.status_code == 200:
                    results = r.json().get("results", [])
                    for job in results:
                        job["_country"] = country.upper()
                    all_jobs.extend(results)
                    print(f"  [{country.upper()}] '{query}': {len(results)} results")
                else:
                    print(f"  [{country.upper()}] '{query}': HTTP {r.status_code}")
            except Exception as e:
                print(f"  Error [{country}] '{query}': {e}")
    return all_jobs

def upsert_jobs(jobs):
    seen_ids = set()
    rows = []
    for job in jobs:
        job_id = str(job.get("id", ""))
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title       = job.get("title", "Unknown Role")
        company     = job.get("company", {}).get("display_name", "Unknown Studio")
        location    = job.get("location", {}).get("display_name", "Remote")
        description = (job.get("description", ""))[:3000]
        salary_min  = job.get("salary_min")
        salary_max  = job.get("salary_max")
        currency    = "GBP" if job.get("_country") == "GB" else "USD"
        url         = job.get("redirect_url", "")
        posted_raw  = job.get("created", "")
        country     = job.get("_country", "US")

        text_combined = (title + " " + description + " " + location).lower()
        remote = any(w in text_combined for w in ["remote", "work from home", "wfh", "distributed"])

        try:
            posted_at = datetime.fromisoformat(posted_raw.replace("Z", "+00:00"))
        except Exception:
            posted_at = datetime.now(timezone.utc)

        expires_at = posted_at + timedelta(days=30)
        is_active  = expires_at > datetime.now(timezone.utc)

        rows.append({
            "id":              job_id,
            "title":           title,
            "company":         company,
            "location":        location,
            "description":     description,
            "salary_min":      float(salary_min) if salary_min else None,
            "salary_max":      float(salary_max) if salary_max else None,
            "salary_currency": currency,
            "url":             url,
            "remote":          remote,
            "job_type":        "fulltime",
            "category":        guess_category(title),
            "tags":            extract_tags(title, description),
            "posted_at":       posted_at.isoformat(),
            "expires_at":      expires_at.isoformat(),
            "is_active":       is_active,
            "source":          "adzuna",
            "country":         country,
        })

    if rows:
        result = supabase.table("jobs").upsert(rows, on_conflict="id").execute()
        print(f"Upserted {len(rows)} jobs")
    return len(rows)

def deactivate_expired():
    now = datetime.now(timezone.utc).isoformat()
    supabase.table("jobs") \
        .update({"is_active": False}) \
        .lt("expires_at", now) \
        .eq("is_active", True) \
        .execute()
    print("Expired jobs deactivated")

def print_stats():
    total  = supabase.table("jobs").select("id", count="exact").execute()
    active = supabase.table("jobs").select("id", count="exact").eq("is_active", True).execute()
    print(f"Stats — Total: {total.count}, Active: {active.count}")

if __name__ == "__main__":
    print("=== SideQuest Job Scraper ===")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    jobs = fetch_jobs()
    print(f"\nTotal fetched: {len(jobs)}")
    upserted = upsert_jobs(jobs)
    deactivate_expired()
    print_stats()
    print("=== Done ===")
