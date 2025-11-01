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

def fetch_remoteok_jobs(tags: List[str] = None) -> List[Dict]:
    """Fetch jobs from RemoteOK API filtered by tags"""
    if tags is None:
        tags = ["crypto", "blockchain", "web3", "bitcoin", "ethereum", "defi"]
    
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
    seen_ids = set()
    
    for job in jobs_data:
        job_id = job.get("id")
        if job_id in seen_ids:
            continue
            
        job_tags = job.get("tags", [])
        job_tags_lower = [t.lower() for t in job_tags]
        
        # Check if any of our target tags are in the job tags
        if any(tag.lower() in job_tags_lower for tag in tags):
            seen_ids.add(job_id)
            jobs.append({
                "job_id": job_id,
                "title": job.get("position"),
                "company": job.get("company", "Unknown"),
                "location": job.get("location", "Remote"),
                "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or job_id}"
            })
    
    print(f"Total jobs matched tags {tags}: {len(jobs)}")
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
    
    # Fetch jobs from RemoteOK with multiple related tags
    jobs = fetch_remoteok_jobs(tags=["crypto", "blockchain", "web3", "bitcoin", "ethereum", "defi"])
    
    if jobs:
        # Save jobs and get only new ones
        new_jobs = save_jobs(jobs)
        
        # Send Telegram notifications for new jobs only
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
