from flask import Flask, request, jsonify, render_template_string
import uuid
import time
import threading
import random
import re

app = Flask(__name__)
lock = threading.Lock()

# Data Structures
backlog_jobs = []    # Raw incoming jobs
priority_queue = []  # Sorted jobs waiting for workers
in_progress_jobs = {}
completed_jobs = []

# Workers Configuration
workers = [
    {"id": "worker-1", "type": "python", "status": "IDLE"},
    {"id": "worker-2", "type": "node", "status": "IDLE"},
    {"id": "worker-3", "type": "java", "status": "IDLE"},
    {"id": "worker-4", "type": "c", "status": "IDLE"},
]

# Scoring Weights (Enterprise-Grade Impact-First)
BRANCH_WEIGHTS = {
    "master": 10000,
    "main": 10000,
    "staging": 5000,
    "dev": 2000,
    "main2": 1000
}

def calculate_priority(job):
    """Calculates priority with aggressive error handling."""
    try:
        # Start with Branch Weight
        branch = str(job.get("branch", "main")).lower()
        
        if job.get("source") == "AUTO":
            # For AUTO jobs, just give them 1 point per second of wait time
            # This keeps them below any WEBHOOK job but shows the system is active
            W = (time.time() - job.get("queued_at", time.time())) * 1
            return round(W, 2)
            
        B = BRANCH_WEIGHTS.get(branch, 1000) 
        W = (time.time() - job.get("queued_at", time.time())) * 50
        S = job.get("size", 100) / 10
        
        priority = B + W + S
        
        print(f"\n[PRIORITY MATH] {job['repo']} ({branch}) -> B:{B} + W:{round(W,1)} + S:{round(S,1)} = {round(priority, 1)}")
        return round(priority, 2)
    except Exception as e:
        print(f"[ERROR] Priority calc failed: {e}")
        return 99999 if job.get("source") == "WEBHOOK" else 0

def get_stages_from_jenkinsfile(repo_name=None):
    """Extracts stages from the specific repository's Jenkinsfile."""
    # Default path
    path = "Jenkinsfile"
    
    # If a repo name is provided, try to find its local Jenkinsfile
    if repo_name and repo_name != "Manual-Trigger":
        # Check if it's one of our sibling folders
        potential_path = f"../{repo_name}/Jenkinsfile"
        import os
        if os.path.exists(potential_path):
            path = potential_path

    try:
        with open(path, "r") as f:
            content = f.read()
        stages = re.findall(r"stage\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", content)
        if stages:
            return stages
    except Exception as e:
        pass
    return ["Fetch Code", "Security Scan", "Docker Build", "Push to Registry"]

def run_pipeline(job_id, worker_id):
    """Simulates job execution through stages with Preemption support."""
    with lock:
        if job_id not in in_progress_jobs:
            return
        job = in_progress_jobs[job_id]
        stages = job.get("stages_list", ["Fetch Code", "Security Scan", "Docker Build", "Push to Registry"])

    for stage_name in stages:
        # Check if stage was already completed before eviction
        if job.get("stages_status", {}).get(stage_name) == "done":
            continue

        # PREEMPTION CHECK: Wait if job is suspended
        while True:
            with lock:
                # EVICTION CHECK: If I'm no longer in progress, stop the thread
                if job_id not in in_progress_jobs:
                    return 
                if not in_progress_jobs[job_id].get("suspended", False):
                    break
            time.sleep(1) # Check every second

        with lock:
            if job_id in in_progress_jobs:
                in_progress_jobs[job_id]["current_stage"] = stage_name
                in_progress_jobs[job_id]["stages_status"][stage_name] = "done"
        
        # PRESENTATION MODE: Slower stage execution (5-8 seconds)
        time.sleep(random.uniform(5.0, 8.0))

    with lock:
        if job_id in in_progress_jobs:
            job = in_progress_jobs.pop(job_id)
            job["status"] = "COMPLETED"
            job["current_stage"] = "Finished"
            completed_jobs.insert(0, job)
            
            # Keep only last 10 completed jobs for UI performance
            if len(completed_jobs) > 10:
                completed_jobs.pop()
        
        # Free the worker
        for w in workers:
            if w["id"] == worker_id:
                w["status"] = "IDLE"

def scheduler():
    """Manages the 4-stage pipeline: Backlog -> Priority Dispatch -> In Progress."""
    while True:
        try:
            with lock:
                # 1. Promote from Backlog to Priority Dispatch
                for job in backlog_jobs[:]:
                    # PRESENTATION MODE: 6s in Backlog
                    if time.time() - job.get("queued_at", 0) >= 6.0:
                        backlog_jobs.remove(job)
                        job["status"] = "DISPATCHING"
                        job["dispatched_at"] = time.time()
                        priority_queue.append(job)

                # 2. Re-calculate and Sort the Priority Dispatch list
                active_webhook_jobs = [j for j in list(in_progress_jobs.values()) if j.get("source") == "WEBHOOK"]
                queued_webhook_jobs = [j for j in priority_queue if j.get("source") == "WEBHOOK"]
                has_priority_work = len(active_webhook_jobs) > 0 or len(queued_webhook_jobs) > 0
                
                for job in priority_queue:
                    job["priority_score"] = calculate_priority(job)
                    job["suspended"] = bool(job["source"] == "AUTO" and has_priority_work)
                
                priority_queue.sort(key=lambda x: x["priority_score"], reverse=True)

                # 3. Assign workers from the top of the Priority Dispatch list
                for job in priority_queue[:]:
                    if job.get("suspended"):
                        continue
                    
                    # PRESENTATION MODE: 10s in Dispatch for full explanation
                    dispatch_delay = 2.0 if job.get("source") == "WEBHOOK" else 10.0
                    if time.time() - job.get("dispatched_at", 0) < dispatch_delay:
                        continue
                        
                    worker = next((w for w in workers if w["status"] == "IDLE" and w["type"] == job["language"]), None)
                    
                    # UNIVERSAL PREEMPTION
                    if not worker and job.get("source") == "WEBHOOK":
                        evict_target = next((j for j in in_progress_jobs.values() if j.get("source") == "AUTO"), None)
                        if evict_target:
                            print(f"\n[PREEMPTION] !! UNIVERSAL STEAL !! Grabbing worker {evict_target['worker']} for {job['repo']}")
                            worker_id = evict_target["worker"]
                            evict_target["status"] = "QUEUED"
                            evict_target["worker"] = None
                            evict_target["suspended"] = True
                            evict_target["dispatched_at"] = time.time() # Reset dispatch timer
                            priority_queue.append(evict_target)
                            del in_progress_jobs[evict_target["id"]]
                            worker = next((w for w in workers if w["id"] == worker_id), None)
                            if worker: 
                                worker["status"] = "IDLE"
                                worker["type"] = job["language"]

                    if worker:
                        print(f"[SCHEDULER] OK - Starting {job['source']} job: {job['repo']}")
                        priority_queue.remove(job)
                        worker["status"] = "BUSY"
                        job["status"] = "IN_PROGRESS"
                        job["worker"] = worker["id"]
                        if "stages_status" not in job: job["stages_status"] = {}
                        in_progress_jobs[job["id"]] = job
                        
                        t = threading.Thread(target=run_pipeline, args=(job["id"], worker["id"]))
                        t.daemon = True
                        t.start()
        except Exception as e:
            print(f"[CRITICAL ERROR in Scheduler]: {e}")
            
        time.sleep(0.5)

def auto_job_generator():
    """Generates background traffic to show the system is alive."""
    stages = get_stages_from_jenkinsfile()
    while True:
        # Generate a job every 2-4 seconds to fill the queue faster for demo
        time.sleep(random.uniform(2.0, 4.0))
        
        with lock:
            # Allow up to 15 total jobs to show sorting dynamics
            if len(backlog_jobs) + len(priority_queue) >= 15:
                continue
                
            lang = random.choice(["python", "node", "java", "c"])
            job_id = str(uuid.uuid4())
            new_job = {
                "id": job_id,
                "repo": f"auto-repo-{random.randint(100, 999)}",
                "branch": random.choice(["master", "dev", "staging"]),
                "language": lang,
                "status": "QUEUED",
                "worker": None,
                "stages_list": stages,
                "stages_status": {},
                "current_stage": "Waiting",
                "source": "AUTO",
                "queued_at": time.time(),
                "size": random.randint(50, 500),
                "retries": 0,
                "priority_score": 0
            }
            backlog_jobs.append(new_job)

@app.route("/", methods=["GET", "POST"])
@app.route("/dashboard")
def dashboard():
    if request.method == "POST":
        return webhook()
    with lock:
        return render_template_string(HTML_TEMPLATE, 
                                     backlog=list(backlog_jobs),
                                     priority_q=list(priority_queue), 
                                     progress=list(in_progress_jobs.values()), 
                                     completed=list(completed_jobs))

@app.route("/webhook", methods=["POST"])
def webhook():
    """Handles GitHub-style webhooks for multiple repos and branches."""
    data = request.json or {}
    
    # Extract Repo Name
    repo_name = data.get("repository", {}).get("name", "Manual-Trigger")
    
    # Extract Branch Name (e.g., refs/heads/master -> master)
    ref = data.get("ref", "refs/heads/master")
    branch_name = ref.split("/")[-1] if "/" in ref else ref
    
    # Highlight if it's a specific commit
    is_readme = False
    if "commits" in data:
        for commit in data["commits"]:
            if any("README" in f.upper() for f in commit.get("modified", []) + commit.get("added", [])):
                is_readme = True
                break
    
    display_repo = f"🚀 {repo_name}"
    if is_readme:
        display_repo = f"📝 {repo_name} (README update)"

    job_id = str(uuid.uuid4())
    new_job = {
        "id": job_id,
        "repo": display_repo,
        "branch": branch_name,
        "language": "python" if "backend" not in repo_name.lower() else "node",
        "status": "QUEUED",
        "worker": None,
        "stages_list": get_stages_from_jenkinsfile(repo_name),
        "stages_status": {},
        "current_stage": "Waiting",
        "source": "WEBHOOK",
        "queued_at": time.time(),
        "size": random.randint(10, 100),
        "retries": 0,
        "priority_score": 0
    }
    
    with lock:
        backlog_jobs.append(new_job)
        
    return jsonify({"status": "Accepted", "job_id": job_id, "branch": branch_name}), 202

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="1">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jenkins Master Dashboard</title>
    <style>
        :root {
            --bg: #0d1117;
            --card-bg: #161b22;
            --border: #30363d;
            --text: #c9d1d9;
            --text-dim: #8b949e;
            --accent: #58a6ff;
            --success: #3fb950;
            --warning: #d29922;
            --webhook: #ff9800;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; 
            background: var(--bg); 
            color: var(--text); 
            margin: 0;
            padding: 20px;
        }
        
        h1 { text-align: center; color: var(--accent); margin-bottom: 30px; font-weight: 300; }
        
        .container { 
            display: flex; 
            gap: 20px; 
            justify-content: center;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .col { 
            flex: 1; 
            background: var(--card-bg); 
            border-radius: 12px; 
            padding: 15px; 
            border: 1px solid var(--border); 
            min-height: 80vh;
            display: flex;
            flex-direction: column;
        }
        
        .col h2 {
            font-size: 1.1rem;
            padding-bottom: 10px;
            border-bottom: 2px solid var(--border);
            margin-top: 0;
            display: flex;
            justify-content: space-between;
        }
        
        .count {
            background: var(--border);
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.8rem;
        }
        
        .job { 
            background: #21262d; 
            border: 1px solid var(--border); 
            padding: 15px; 
            margin-bottom: 15px; 
            border-radius: 8px; 
            border-left: 5px solid var(--success);
            transition: transform 0.2s;
            position: relative;
        }
        
        .job:hover { transform: translateY(-2px); }
        
        /* Specialized borders */
        .job.suspended { border-left-color: #6e7681; opacity: 0.5; }
        .suspended-label { 
            font-size: 0.65rem; 
            background: #f85149; 
            color: white; 
            padding: 2px 6px; 
            border-radius: 4px; 
            margin-left: 10px;
            font-weight: bold;
        }
        
        .priority-rank {
            position: absolute;
            top: 10px;
            left: -15px;
            background: var(--accent);
            color: white;
            width: 25px;
            height: 25px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8rem;
            font-weight: bold;
            border: 2px solid var(--bg);
        }
        
        .job-title { font-weight: 600; margin-bottom: 5px; display: block; }
        .job-meta { font-size: 0.75rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; }
        
        .current-stage { 
            color: var(--accent); 
            font-weight: bold; 
            font-size: 0.9rem; 
            margin: 10px 0;
            display: flex;
            align-items: center;
        }
        
        .current-stage::before {
            content: '⚡';
            margin-right: 5px;
            animation: pulse 1s infinite;
        }
        
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.4; }
            100% { opacity: 1; }
        }
        
        .stages-container {
            margin-top: 10px;
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        
        .stage-item { 
            font-size: 0.8rem; 
            color: var(--text-dim); 
            display: flex;
            align-items: center;
        }
        
        .stage-item.done { color: var(--success); }
        .stage-item.done::before { content: '✓ '; margin-right: 5px; font-weight: bold; }
        .stage-item:not(.done)::before { content: '○ '; margin-right: 5px; opacity: 0.5; }

        .worker-tag {
            position: absolute;
            top: 10px;
            right: 10px;
            font-size: 0.65rem;
            background: var(--border);
            padding: 2px 6px;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <h1>🚀 Pipeline Master Control</h1>
    
    <div class="container" style="max-width: 1700px;">
        <!-- 1. INCOMING BACKLOG -->
        <div class="col">
            <h2>Incoming Backlog <span class="count">{{ backlog|length }}</span></h2>
            {% for j in backlog %}
            <div class="job {{ 'webhook' if j.source == 'WEBHOOK' else '' }}">
                <span class="job-meta">{{ j.source }} | {{ j.language }}</span>
                <span class="job-title">{{ j.repo }}</span>
                <span style="font-size: 0.75rem; color: var(--accent);">Branch: {{ j.branch }}</span>
            </div>
            {% endfor %}
        </div>

        <!-- 2. PRIORITY DISPATCH -->
        <div class="col" style="background: rgba(88, 166, 255, 0.05);">
            <h2>Priority Dispatch <span class="count">{{ priority_q|length }}</span></h2>
            <div style="margin-left: 20px;">
                {% for j in priority_q %}
                <div class="job {{ 'webhook' if j.source == 'WEBHOOK' else '' }} {{ 'suspended' if j.suspended else '' }}">
                    <div class="priority-rank">{{ loop.index }}</div>
                    <span class="job-meta">
                        {{ j.language }} 
                        {% if j.suspended %}<span class="suspended-label">SUSPENDED</span>{% endif %}
                    </span>
                    <span class="job-title">{{ j.repo }}</span>
                    <div style="font-size: 0.7rem; color: var(--accent); margin-top: 5px;">
                        Priority: <strong>{{ j.priority_score }}</strong>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>

        <!-- 3. IN PROGRESS -->
        <div class="col">
            <h2>In Progress <span class="count">{{ progress|length }}</span></h2>
            {% for j in progress %}
            <div class="job progress-job {{ 'webhook' if j.source == 'WEBHOOK' else '' }} {{ 'suspended' if j.suspended else '' }}">
                <span class="worker-tag">{{ j.worker }}</span>
                <span class="job-meta">
                    {{ j.language }}
                    {% if j.suspended %}<span class="suspended-label">PREEMPTED</span>{% endif %}
                </span>
                <span class="job-title">{{ j.repo }}</span>
                <div class="current-stage">{% if j.suspended %}⏸ {% endif %}{{ j.current_stage }}</div>
                
                <div class="stages-container">
                    {% for s in j.stages_list %}
                        <div class="stage-item {{ 'done' if j.stages_status.get(s) == 'done' else '' }}">
                            {{ s }}
                        </div>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
        </div>

        <!-- 4. COMPLETED -->
        <div class="col">
            <h2>Completed <span class="count">{{ completed|length }}</span></h2>
            {% for j in completed %}
            <div class="job" style="border-left-color: var(--success); opacity: 0.8;">
                <span class="job-meta">{{ j.repo }}</span>
                <span class="job-title" style="color: var(--success);">SUCCESS</span>
                <span style="font-size: 0.7rem; color: var(--text-dim);">Completed Branch: {{ j.branch }}</span>
            </div>
            {% endfor %}
        </div>
    </div>
</body>
</html>
"""

if __name__ == "__main__":
    # Start background threads
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=auto_job_generator, daemon=True).start()
    
    print("Dashboard available at http://127.0.0.1:5000/dashboard")
    print("Webhook endpoint at http://127.0.0.1:5000/webhook")
    
    app.run(port=5000, debug=False, use_reloader=False)