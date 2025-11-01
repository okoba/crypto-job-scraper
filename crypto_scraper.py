import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict

# === CONFIG ===
CSV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remoteok_crypto_jobs.csv")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ Error: TELEGRAM_TOKEN and CHAT_ID must be set as environment variables.")
    exit(1)

CHAT_ID = int(CHAT_ID)


# === TELEGRAM ===
def send_telegram_message(message: str):
    """Send message via Telegram Bot."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        res = requests.post(url, data=payload, timeout=10)
        res.raise_for_status()
        print("📨 Telegram message sent.")
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")


# === FETCH JOBS ===
def fetch_remoteok_jobs(tags: List[str] = None, max_age_days: int = 1) -> List[Dict]:
    """Fetch and filter crypto jobs from RemoteOK API."""
    if tags is None:
        tags = ["crypto", "blockchain", "web3", "bitcoin", "ethereum", "defi", "solidity", "nft", "dao"]

    url = "https://remoteok.com/api"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()[1:]  # skip metadata
        print(f"🌍 Retrieved {len(data)} total jobs from RemoteOK.")
    except Exception as e:
        print(f"⚠️ Request/JSON error: {e}")
        return []

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    print(f"🕓 Filtering jobs posted after {cutoff_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    matched_jobs = []
    for job in data:
        job_id = job.get("id")
        date_raw = job.get("date")
        job_date = None

        # --- Parse date safely ---
        if not date_raw:
            continue
        try:
            if isinstance(date_raw, str):
                try:
                    job_date = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
                except ValueError:
                    job_date = datetime.utcfromtimestamp(int(date_raw)).replace(tzinfo=timezone.utc)
            else:
                job_date = datetime.utcfromtimestamp(int(date_raw)).replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"⚠️ Date parse error for job {job_id}: {e}")
            continue

        if job_date < cutoff_date:
            continue

        job_tags = [t.lower() for t in job.get("tags", [])]
        position = (job.get("position") or "").lower()
        company = (job.get("company") or "").lower()

        # Match via tags or title/company text
        if any(tag in job_tags or tag in position or tag in company for tag in tags):
            matched_jobs.append({
                "job_id": job_id,
                "title": job.get("position", "No title"),
                "company": job.get("company", "Unknown"),
                "location": job.get("location", "Remote"),
                "date_posted": job_date.strftime("%Y-%m-%d %H:%M"),
                "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or job_id}",
                "epoch": int(job_date.timestamp())
            })

    print(f"✅ Matched {len(matched_jobs)} relevant jobs (posted within last {max_age_days} day(s)).")
    return matched_jobs


# === SAVE JOBS ===
def save_jobs(jobs: List[Dict], filename: str = CSV_FILE) -> List[Dict]:
    """Save all jobs to CSV and return new ones."""
    existing_ids = set()
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        existing_ids = set(df_existing["job_id"])
        print(f"📁 Loaded {len(existing_ids)} existing job IDs.")
    else:
        print("📁 No previous job file found. Creating new one.")

    new_jobs = [job for job in jobs if job["job_id"] not in existing_ids]
    print(f"🆕 Found {len(new_jobs)} new jobs.")

    if jobs:
        df_all = pd.DataFrame(jobs)
        if os.path.exists(filename):
            df_existing = pd.read_csv(filename)
            df_combined = pd.concat([df_existing, df_all], ignore_index=True).drop_duplicates(subset=["job_id"])
        else:
            df_combined = df_all
        df_combined.sort_values("epoch", ascending=False, inplace=True)
        df_combined.drop(columns=["epoch"], inplace=True, errors="ignore")
        df_combined.to_csv(filename, index=False)
        print(f"💾 Saved {len(df_combined)} total jobs to {filename}.")

    return new_jobs


# === MAIN ===
if __name__ == "__main__":
    print("🚀 Starting Crypto Job Scraper...")
    print(f"Current UTC time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    jobs = fetch_remoteok_jobs(max_age_days=2)  # ✅ Slightly increased window (2 days)

    if not jobs:
        print("❌ No jobs found from API.")
        exit(0)

    new_jobs = save_jobs(jobs)

    if new_jobs:
        print(f"📢 Sending {len(new_jobs)} new jobs to Telegram...")
        for job in new_jobs:
            msg = (
                f"*{job['title']}*\n"
                f"{job['company']} — {job['location']}\n"
                f"🗓 {job['date_posted']}\n"
                f"🔗 {job['link']}"
            )
            send_telegram_message(msg)
    else:
        print("ℹ️ No new jobs to notify.")

    print("✅ Scraper completed successfully.")
