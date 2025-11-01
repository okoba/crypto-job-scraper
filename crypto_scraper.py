#!/usr/bin/env python3
"""
Crypto Job Scraper for RemoteOK (production-ready)

Features:
- Incremental scraping using last run timestamp
- Robust date parsing (ISO & epoch)
- Deduplicated CSV storage
- Telegram notifications for new jobs only
"""

import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import json

# --- CONFIG ---
CSV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remoteok_crypto_jobs.csv")
LAST_RUN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "last_run.txt")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Tags to match (lowercase)
TAGS = [
    "crypto", "blockchain", "web3", "bitcoin", "ethereum",
    "defi", "solidity", "smart contract", "nft", "dao",
    "crypto engineer", "blockchain engineer"
]

REMOTEOK_API = "https://remoteok.com/api"

# Safety check
if not TELEGRAM_TOKEN or not CHAT_ID:
    print("❌ TELEGRAM_TOKEN and CHAT_ID must be set.")
    exit(1)

CHAT_ID = int(CHAT_ID)


# --- TELEGRAM ---
def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, data=payload, timeout=12)
        r.raise_for_status()
        print("📨 Telegram sent")
    except Exception as e:
        print(f"⚠️ Telegram send error: {e}")


# --- UTILITIES ---
def safe_parse_date(date_raw):
    """Parse date from ISO string or epoch to UTC datetime"""
    if not date_raw:
        return None
    try:
        if isinstance(date_raw, str):
            try:
                dt = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except:
                dt = datetime.utcfromtimestamp(float(date_raw)).replace(tzinfo=timezone.utc)
                return dt
        else:
            dt = datetime.fromtimestamp(float(date_raw), tz=timezone.utc)
            return dt
    except:
        return None


# --- FETCH JOBS ---
def fetch_remoteok_jobs() -> List[Dict]:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(REMOTEOK_API, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()[1:]  # skip metadata
        print(f"🌐 Fetched {len(data)} jobs from RemoteOK API.")
        return data
    except Exception as e:
        print(f"⚠️ RemoteOK fetch error: {e}")
        return []


# --- MATCH JOBS ---
def match_jobs(jobs: List[Dict], cutoff: datetime) -> List[Dict]:
    matched = []
    for job in jobs:
        jid = job.get("id")
        date_raw = job.get("date") or job.get("epoch")
        dt = safe_parse_date(date_raw)
        if not dt:
            continue
        if dt <= cutoff:
            continue

        tags_lower = [t.lower() for t in (job.get("tags") or [])]
        pos = (job.get("position") or "").lower()
        comp = (job.get("company") or "").lower()

        if any(tag in tags_lower or tag in pos or tag in comp for tag in TAGS):
            matched.append({
                "job_id": jid,
                "title": job.get("position", "No title"),
                "company": job.get("company", "Unknown"),
                "location": job.get("location", "Remote"),
                "date_posted": dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or jid}",
                "epoch": int(dt.timestamp())
            })
    print(f"✅ Matched {len(matched)} jobs after filtering with cutoff {cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
    return matched


# --- SAVE JOBS ---
def save_jobs(jobs: List[Dict], filename: str = CSV_FILE) -> List[Dict]:
    existing_ids = set()
    if os.path.exists(filename):
        try:
            df_existing = pd.read_csv(filename)
            existing_ids = set(df_existing["job_id"].astype(str))
        except:
            pass

    new_jobs = [job for job in jobs if str(job["job_id"]) not in existing_ids]

    if jobs:
        df_all = pd.DataFrame(jobs)
        if os.path.exists(filename):
            try:
                df_existing = pd.read_csv(filename)
                df_combined = pd.concat([df_existing, df_all], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["job_id"], keep="first")
            except:
                df_combined = df_all.drop_duplicates(subset=["job_id"], keep="first")
        else:
            df_combined = df_all.drop_duplicates(subset=["job_id"], keep="first")

        if "epoch" in df_combined.columns:
            df_combined = df_combined.sort_values("epoch", ascending=False)
            df_combined = df_combined.drop(columns=["epoch"], errors="ignore")

        df_combined.to_csv(filename, index=False)
        print(f"💾 CSV saved with {len(df_combined)} rows at {filename}.")

    return new_jobs


# --- MAIN ---
def main():
    print("🚀 Starting Crypto Job Scraper...")
    now_utc = datetime.now(timezone.utc)
    print(f"Current UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")

    # Load last run
    if os.path.exists(LAST_RUN_FILE):
        try:
            with open(LAST_RUN_FILE) as f:
                last_run = datetime.fromisoformat(f.read().strip())
        except:
            last_run = now_utc - timedelta(days=2)
    else:
        last_run = now_utc - timedelta(days=2)

    print(f"Last run timestamp: {last_run.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    jobs = fetch_remoteok_jobs()
    if not jobs:
        print("❌ No jobs fetched.")
        return

    matched = match_jobs(jobs, cutoff=last_run)
    new_jobs = save_jobs(matched)

    if new_jobs:
        print(f"📢 Sending {len(new_jobs)} new jobs to Telegram...")
        for job in new_jobs:
            msg = f"*{job['title']}*\n{job['company']} — {job['location']}\n{job['date_posted']}\n{job['link']}"
            send_telegram_message(msg)
    else:
        print("ℹ️ No new jobs to notify.")

    # Update last run timestamp
    try:
        with open(LAST_RUN_FILE, "w") as f:
            f.write(now_utc.isoformat())
    except:
        pass

    print("✅ Scraper completed successfully.")


if __name__ == "__main__":
    main()
