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
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    
    for job in jobs_data:
        job_id = job.get("id")
        if job_id in seen_ids:
            continue
        
        # Check date (epoch timestamp in seconds)
        date_epoch = job.get("date")
        job_date = None
        date_str = "Unknown"
        
        if date_epoch:
            try:
                # Handle ISO 8601 strings (e.g., '2025-10-31T08:00:36+00:00')
                if isinstance(date_epoch, str):
                    # Try parsing as ISO format first
                    try:
                        job_date = datetime.fromisoformat(date_epoch.replace('Z', '+00:00'))
                    except:
                        # Fall back to integer timestamp
                        date_epoch = int(date_epoch)
                        job_date = datetime.fromtimestamp(date_epoch, tz=timezone.utc)  # ✅ FIXED
                else:
                    # Integer timestamp
                    job_date = datetime.fromtimestamp(date_epoch, tz=timezone.utc)  # ✅ FIXED
                
                date_str = job_date.strftime("%Y-%m-%d")
                
                # Skip jobs older than cutoff
                if job_date < cutoff_date:
                    continue
                    
                # Convert to epoch for sorting
                date_epoch = int(job_date.timestamp())
            except Exception as e:
                print(f"Error parsing date for job {job_id}: {e}")
                # If we can't parse the date, skip this job to be safe
                continue
        else:
            # No date provided, skip to be safe
            continue
        
        job_tags = job.get("tags", [])
        job_tags_lower = [t.lower() for t in job_tags]
        
        # Also check in position title and company name for keywords
        position = (job.get("position") or "").lower()
        company = (job.get("company") or "").lower()
        
        # Match if tags OR title/company contains keywords
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
