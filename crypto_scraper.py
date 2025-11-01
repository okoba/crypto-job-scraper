#!/usr/bin/env python3
"""
Diagnostic crypto job scraper for RemoteOK.

- Fetches RemoteOK API
- Dumps raw first N jobs to debug_remoteok.json
- Prints parsed dates for each job (date, epoch, parsed datetime)
- Optionally bypasses cutoff for testing via environment variable FORCE_SHOW_ALL=1
- Stores last run timestamp in last_run.txt for later incremental improvements
- Sends Telegram notifications only if SEND_TELEGRAM=1 (safe default: 0)
"""

import requests
import pandas as pd
import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict

# ----- Config -----
CSV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remoteok_crypto_jobs.csv")
DEBUG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_remoteok.json")
LAST_RUN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "last_run.txt")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
SEND_TELEGRAM = os.environ.get("SEND_TELEGRAM", "0") == "1"   # set to "1" to actually send messages
FORCE_SHOW_ALL = os.environ.get("FORCE_SHOW_ALL", "0") == "1" # set to "1" to bypass time filter and show all matches

# Tags to match (lowercase)
TAGS = [
    "crypto", "blockchain", "web3", "bitcoin", "ethereum",
    "defi", "solidity", "smart contract", "nft", "dao",
    "crypto engineer", "blockchain engineer"
]

# API endpoint
REMOTEOK_API = "https://remoteok.com/api"

# Safety: if Telegram not configured but SEND_TELEGRAM enabled, warn and disable
if SEND_TELEGRAM and (not TELEGRAM_TOKEN or not CHAT_ID):
    print("⚠️ SEND_TELEGRAM=1 but TELEGRAM_TOKEN/CHAT_ID missing. Disabling sends.")
    SEND_TELEGRAM = False

# ----- Utilities -----
def send_telegram_message(message: str):
    if not SEND_TELEGRAM:
        print("📨 (telemetry) Would send Telegram message (SEND_TELEGRAM=0):")
        print(message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": int(CHAT_ID), "text": message, "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        r = requests.post(url, data=payload, timeout=12)
        r.raise_for_status()
        print("✅ Telegram sent")
    except Exception as e:
        print(f"❌ Telegram send error: {e}")

def safe_parse_date(date_raw):
    """Return (parsed_dt: datetime with tz=UTC) or None, and human string"""
    if not date_raw:
        return None, "None"
    try:
        # ISO string like '2025-11-01T14:43:00+00:00' or '2025-11-01T14:43:00Z'
        if isinstance(date_raw, str):
            try:
                dt = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                # maybe it's a stringified epoch
                try:
                    epoch = float(date_raw)
                    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                    return dt, dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                except Exception:
                    return None, f"Unparseable-string:{date_raw}"
        else:
            # numeric epoch
            try:
                epoch = float(date_raw)
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                return dt, dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                return None, f"Unparseable-nonstr:{date_raw}"
    except Exception as e:
        return None, f"Error:{e}"

# ----- Fetch and debug -----
def fetch_remoteok_raw():
    headers = {"User-Agent": "Mozilla/5.0 (compatible; CryptoScraper/1.0)"}
    try:
        r = requests.get(REMOTEOK_API, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        # data[0] often metadata; jobs start at index 1
        jobs = data[1:] if isinstance(data, list) and len(data) > 1 else (data if isinstance(data, list) else [])
        print(f"🌐 Fetched {len(jobs)} jobs from RemoteOK API.")
        return jobs
    except Exception as e:
        print(f"❌ Fetch error: {e}")
        return []

def debug_dump(jobs):
    dump = {"fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "count": len(jobs),
            "sample": []}
    for j in jobs[:50]:
        # keep limited fields to avoid huge dump
        dump["sample"].append({
            "id": j.get("id"),
            "position": j.get("position"),
            "company": j.get("company"),
            "date_field": j.get("date"),
            "epoch_field": j.get("epoch"),
            "slug": j.get("slug"),
            "tags": j.get("tags"),
            "raw": j  # include full raw for deep inspection if needed
        })
    try:
        with open(DEBUG_FILE, "w", encoding="utf-8") as fh:
            json.dump(dump, fh, indent=2, ensure_ascii=False)
        print(f"💾 Wrote debug JSON to {DEBUG_FILE} (first {min(50, len(jobs))} jobs).")
    except Exception as e:
        print(f"❌ Failed to write debug file: {e}")

# ----- Core matching logic -----
def match_jobs(jobs, max_age_days=1, force_show_all=False):
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    print(f"🔎 Cutoff (UTC): {cutoff.strftime('%Y-%m-%d %H:%M:%S %Z')}, FORCE_SHOW_ALL={force_show_all}")
    matched = []
    parsed_info = []
    for idx, job in enumerate(jobs):
        jid = job.get("id") or f"no-id-{idx}"
        date_field = job.get("date") or job.get("epoch") or job.get("created_at") or job.get("posting_date")
        parsed_dt, human = safe_parse_date(date_field)
        parsed_info.append({
            "id": jid,
            "position": job.get("position"),
            "company": job.get("company"),
            "date_field_raw": date_field,
            "parsed_dt": parsed_dt.isoformat() if parsed_dt else None,
            "parsed_human": human,
            "tags": job.get("tags")
        })
        # for matching logic, require parsed_dt
        if not parsed_dt:
            continue
        if (force_show_all) or (parsed_dt >= cutoff):
            # check tags/title/company
            tags_lower = [t.lower() for t in (job.get("tags") or [])]
            pos = (job.get("position") or "").lower()
            comp = (job.get("company") or "").lower()
            matched_flag = False
            for t in TAGS:
                if t in tags_lower or t in pos or t in comp:
                    matched_flag = True
                    break
            if matched_flag:
                matched.append({
                    "job_id": jid,
                    "title": job.get("position"),
                    "company": job.get("company"),
                    "location": job.get("location", "Remote"),
                    "date_posted": parsed_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or jid}",
                    "epoch": int(parsed_dt.timestamp())
                })
    # print parsed_info for first 20 jobs to logs for inspection
    print("\n📋 Parsed date info for first 20 jobs:")
    for pi in parsed_info[:20]:
        print(f" - {pi['id']} | {pi['position']} | parsed: {pi['parsed_human']} | tags: {pi['tags']}")
    return matched

# ----- CSV save / dedupe -----
def save_jobs(jobs, filename=CSV_FILE):
    if not jobs:
        return []
    if os.path.exists(filename):
        try:
            df_existing = pd.read_csv(filename)
            existing_ids = set(df_existing["job_id"].astype(str))
            print(f"📂 Loaded {len(existing_ids)} existing job IDs from CSV.")
        except Exception as e:
            print(f"⚠️ Could not load existing CSV: {e}")
            existing_ids = set()
    else:
        existing_ids = set()
        print("📂 No existing CSV file found.")
    new_jobs = [j for j in jobs if str(j["job_id"]) not in existing_ids]
    print(f"➕ Identified {len(new_jobs)} new jobs (of {len(jobs)} matched).")
    # persist combined
    df_new = pd.DataFrame(jobs)
    if os.path.exists(filename):
        try:
            df_existing = pd.read_csv(filename)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(subset=["job_id"], keep="first")
        except Exception:
            df_combined = df_new.drop_duplicates(subset=["job_id"], keep="first")
    else:
        df_combined = df_new.drop_duplicates(subset=["job_id"], keep="first")
    # sort if epoch present
    if "epoch" in df_combined.columns:
        df_combined = df_combined.sort_values("epoch", ascending=False)
        df_combined = df_combined.drop(columns=["epoch"], errors="ignore")
    try:
        df_combined.to_csv(filename, index=False)
        print(f"💾 CSV saved with {len(df_combined)} rows at {filename}")
    except Exception as e:
        print(f"❌ Failed to save CSV: {e}")
    return new_jobs

# ----- Main -----
def main():
    print("🚀 Starting diagnostic crypto scraper...")
    print("UTC now:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"))
    jobs = fetch_remoteok_raw()
    if not jobs:
        print("❌ No jobs returned from API. Consider scraping website HTML as fallback or check network.")
        return

    # Dump raw debug file
    debug_dump(jobs)

    # Force show all if env var set
    force = FORCE_SHOW_ALL

    matched = match_jobs(jobs, max_age_days=1, force_show_all=force)
    print(f"\n✅ Matched {len(matched)} jobs after filtering (force={force}).")

    # Save matched to CSV and find new jobs
    new_jobs = save_jobs(matched)
    if new_jobs:
        print(f"📢 Will notify {len(new_jobs)} new jobs.")
        for j in new_jobs:
            msg = f"*{j['title']}*\n{j['company']} — {j['location']}\n{j['date_posted']}\n{j['link']}"
            send_telegram_message(msg)
    else:
        print("ℹ️ No new jobs to notify (or SEND_TELEGRAM disabled).")

    # persist last run timestamp
    try:
        with open(LAST_RUN_FILE, "w", encoding="utf-8") as fh:
            fh.write(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"))
    except Exception:
        pass

    print("✅ Diagnostic run complete.")

if __name__ == "__main__":
    main()
