import requests
import pandas as pd
from typing import List, Dict
import os

# CSV file to store jobs
CSV_FILE = "remoteok_crypto_jobs.csv"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not TELEGRAM_TOKEN or not CHAT_ID:
    print("Error: TELEGRAM_TOKEN and CHAT_ID environment variables must be set")
    exit(1)

CHAT_ID = int(CHAT_ID)

def send_telegram_message(message: str):
    """Send message via Telegram bot"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        print("Telegram message sent")
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")

def fetch_remoteok_jobs(tag: str = "crypto") -> List[Dict]:
    """Fetch jobs from RemoteOK API filtered by tag"""
    url = "https://remoteok.com/api"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Request failed: {e}")
        return []
    
    try:
        data = response.json()
        jobs_data = data[1:]  # skip metadata
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        return []
    
    jobs = []
    for job in jobs_data:
        job_tags = job.get("tags", [])
        if tag and not any(tag.lower() in t.lower() for t in job_tags):
            continue
        jobs.append({
            "job_id": job.get("id"),
            "title": job.get("position"),
            "company": job.get("company", "Unknown"),
            "location": job.get("location", "Remote"),
            "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or job.get('id')}"
        })
    
    print(f"Total jobs matched tag '{tag}': {len(jobs)}")
    return jobs

def save_jobs(jobs: List[Dict], filename: str = CSV_FILE) -> List[Dict]:
    """Save all jobs and return only new ones for notification"""
    
    # Get existing job IDs
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        existing_ids = set(df_existing["job_id"])
        print(f"Found {len(existing_ids)} existing jobs in CSV")
    else:
        existing_ids = set()
        print("No existing CSV file found")
    
    # Identify new jobs
    new_jobs = [job for job in jobs if job["job_id"] not in existing_ids]
    print(f"Identified {len(new_jobs)} new jobs out of {len(jobs)} total scraped jobs")
    
    # Save ALL current jobs (not just new ones)
    if jobs:
        df_all = pd.DataFrame(jobs)
        if os.path.exists(filename):
            # Combine with existing jobs, remove duplicates
            df_existing = pd.read_csv(filename)
            df_combined = pd.concat([df_existing, df_all], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['job_id'], keep='last')
            df_combined.to_csv(filename, index=False)
            print(f"CSV now contains {len(df_combined)} total jobs")
        else:
            df_all.to_csv(filename, index=False)
            print(f"Created new CSV with {len(df_all)} jobs")
    
    return new_jobs

if __name__ == "__main__":
    print("Starting crypto job scraper...")
    
    jobs = fetch_remoteok_jobs(tag="crypto")
    
    if jobs:
        new_jobs = save_jobs(jobs)
        
        if new_jobs:
            print(f"Sending {len(new_jobs)} Telegram notifications...")
            for job in new_jobs:
                message = f"*{job['title']}* | {job['company']} | {job['location']}\n{job['link']}"
                send_telegram_message(message)
        else:
            print("No new jobs to notify about")
    else:
        print("No jobs found.")
    
    print("Scraper completed successfully")
