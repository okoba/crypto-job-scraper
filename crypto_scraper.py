import requests
import pandas as pd
from typing import List, Dict
import os
from datetime import datetime, timedelta

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
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        print("Telegram message sent")
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")

def fetch_remoteok_jobs(tags: List[str] = None, max_age_days: int = 7) -> List[Dict]:
    """Fetch jobs from RemoteOK API filtered by tags and date"""
    if tags is None:
        tags = ["crypto", "blockchain", "web3", "bitcoin", "ethereum", "defi", "solidity", "smart contract"]
    
    url = "https://remoteok.com/api"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Cache-Control": "no-cache"
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
        print(f"Retrieved {len(jobs_data)} total jobs from API")
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        return []
    
    jobs = []
    seen_ids = set()
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    
    for job in jobs_data:
        job_id = job.get("id")
        if job_id in seen_ids:
            continue
        
        date_epoch = job.get("date")
        job_date = None
        date_str = "Unknown"
        
        if date_epoch:
            try:
                if isinstance(date_epoch, str):
                    date_epoch = int(date_epoch)
                job_date = datetime.fromtimestamp(date_epoch)
                date_str = job_date.strftime("%Y-%m-%d")
                
                if job_date < cutoff_date:
                    continue
            except Exception as e:
                print(f"Error parsing date for job {job_id}: {e}")
                continue
        else:
            continue
        
        job_tags = job.get("tags", [])
        job_tags_lower = [t.lower() for t in job_tags]
        
        position = (job.get("position") or "").lower()
        company = (job.get("company") or "").lower()
        
        tag_match = any(tag.lower() in job_tags_lower for tag in tags)
        text_match = any(tag.lower() in position or tag.lower() in company for tag in tags)
        
        if tag_match or text_match:
            seen_ids.add(job_id)
            
            jobs.append({
                "job_id": job_id,
                "title": job.get("position"),
                "company": job.get("company", "Unknown"),
                "location": job.get("location", "Remote"),
                "date_posted": date_str,
                "date_epoch": date_epoch,  
                "link": f"https://remoteok.com/remote-jobs/{job.get('slug') or job_id}"
            })
    
    print(f"Jobs matched (within {max_age_days} days): {len(jobs)}")
    return jobs

def save_jobs(jobs: List[Dict], filename: str = CSV_FILE) -> List[Dict]:
    """Save all jobs and return only new ones for notification"""
    
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        existing_ids = set(df_existing["job_id"])
        print(f"Found {len(existing_ids)} existing jobs in CSV")
    else:
        existing_ids = set()
        print("No existing CSV file found")
    
    new_jobs = [job for job in jobs if job["job_id"] not in existing_ids]
    print(f"Identified {len(new_jobs)} new jobs out of {len(jobs)} total scraped jobs")
    
    if jobs:
        df_all = pd.DataFrame(jobs)
        if os.path.exists(filename):
            df_existing = pd.read_csv(filename)
            df_combined = pd.concat([df_existing, df_all], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['job_id'], keep='last')
            
            if 'date_epoch' in df_combined.columns:
                df_combined = df_combined.sort_values('date_epoch', ascending=False, na_position='last')
                df_combined = df_combined.drop(columns=['date_epoch'])
            
            df_combined.to_csv(filename, index=False)
            print(f"CSV now contains {len(df_combined)} total jobs")
        else:
            df_all = df_all.drop(columns=['date_epoch'])
            df_all.to_csv(filename, index=False)
            print(f"Created new CSV with {len(df_all)} jobs")
    
    return new_jobs

if __name__ == "__main__":
    print("Starting crypto job scraper...")
    
    jobs = fetch_remoteok_jobs(
        tags=["crypto", "blockchain", "web3", "bitcoin", "ethereum", "defi", "solidity", "smart contract", "nft", "dao", "crypto engineer", "blockchain engineer"],
        max_age_days=1
    )
    
    if jobs:
        new_jobs = save_jobs(jobs)
        
        if new_jobs:
            print(f"Sending {len(new_jobs)} Telegram notifications...")
            for job in new_jobs:
                message = (
                    f"*{job['title']}*\n"
                    f"{job['company']}\n"
                    f"{job['location']}\n"
                    f"{job['date_posted']}\n"
                    f"{job['link']}"
                )
                send_telegram_message(message)
        else:
            print("No new jobs to notify about")
    else:
        print("No jobs found.")
    
    print("Scraper completed successfully")
