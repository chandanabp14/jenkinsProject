from flask import Flask, request, jsonify, render_template_string
import uuid
import time
import threading
import random
import re

app = Flask(__name__)
lock = threading.Lock()

# Data Structures
queued_jobs = []
in_progress_jobs = {}
completed_jobs = []

# Workers Configuration
workers = [
    {"id": "worker-1", "type": "python", "status": "IDLE"},
    {"id": "worker-2", "type": "node", "status": "IDLE"},
    {"id": "worker-3", "type": "java", "status": "IDLE"},
    {"id": "worker-4", "type": "c", "status": "IDLE"},
]

# Scoring Weights
W_TYPE = 500
W_AGE = 2
W_SIZE = -0.5
W_PENALTY = 100

def calculate_priority(job):
    """Calculates priority based on the multi-factor scoring formula."""
    # T: Trigger Type (WEBHOOK = 1, AUTO = 0)
    T = 1 if job.get("source") == "WEBHOOK" else 0
    
    # A: Wait Time (seconds in queue)
    A = time.time() - job.get("queued_at", time.time())
    
    # S: Job Size (heuristic lines of code)
    S = job.get("size", 100)
    
    # R: Retry Penalty
    R = job.get("retries", 0)
    
    # P = (W_type * T) + (W_age * A) + (W_size * S) - (W_penalty * R)
    priority = (W_TYPE * T) + (W_AGE * A) + (W_SIZE * S) - (W_PENALTY * R)
    return round(priority, 2)

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
    """Simulates job execution through stages."""
    with lock:
        if job_id not in in_progress_jobs:
            return
        job = in_progress_jobs[job_id]
        # Use the stages already assigned to the job
        stages = job.get("stages_list", ["Fetch Code", "Security Scan", "Docker Build", "Push to Registry"])

    for stage_name in stages:
        with lock:
            if job_id in in_progress_jobs:
                in_progress_jobs[job_id]["current_stage"] = stage_name
                in_progress_jobs[job_id]["stages_status"][stage_name] = "done"
        
        # Simulated stage execution time
        time.sleep(random.uniform(2.0, 4.0))

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
    """Matches queued jobs to idle workers based on priority scoring.
    Waits until there are at least 5 jobs to demonstrate sorting logic.
    """
    while True:
        with lock:
            # Calculate priority for all queued jobs and sort them
            for job in queued_jobs:
                job["priority_score"] = calculate_priority(job)
            
            # Sort by priority score descending
            queued_jobs.sort(key=lambda x: x["priority_score"], reverse=True)

            # Only proceed if we have a decent number of jobs to show sorting
            # Or if it's a webhook job (which we should process anyway)
            has_webhook = any(j.get("source") == "WEBHOOK" for j in queued_jobs)
            
            if len(queued_jobs) >= 5 or has_webhook:
                for job in queued_jobs[:]:
                    # Still keep the 3-second visibility delay
                    if time.time() - job.get("queued_at", 0) < 3.0:
                        continue

                    # Match job language to worker type
                    worker = next((w for w in workers if w["status"] == "IDLE" and w["type"] == job["language"]), None)
                    
                    if worker:
                        queued_jobs.remove(job)
                        worker["status"] = "BUSY"
                        job["status"] = "IN_PROGRESS"
                        job["worker"] = worker["id"]
                        job["stages_status"] = {}
                        in_progress_jobs[job["id"]] = job
                        
                        # Start execution in a new thread
                        t = threading.Thread(target=run_pipeline, args=(job["id"], worker["id"]))
                        t.daemon = True
                        t.start()
                    
        time.sleep(0.5)

def auto_job_generator():
    """Generates background traffic to show the system is alive."""
    stages = get_stages_from_jenkinsfile()
    while True:
        # Generate a job every 2-4 seconds to fill the queue faster for demo
        time.sleep(random.uniform(2.0, 4.0))
        
        with lock:
            # Allow up to 10 jobs in queue to show sorting
            if len(queued_jobs) >= 10:
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
            queued_jobs.append(new_job)

@app.route("/", methods=["GET", "POST"])
@app.route("/dashboard")
def dashboard():
    if request.method == "POST":
        return webhook()
    with lock:
        return render_template_string(HTML_TEMPLATE, 
                                     queued=list(queued_jobs), 
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
        queued_jobs.append(new_job)
        
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
        .job.webhook { border-left-color: var(--webhook); }
        .job.progress-job { border-left-color: var(--warning); }
        .job.comp-job { border-left-color: var(--text-dim); opacity: 0.7; }
        
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
    
    <div class="container">
        <!-- QUEUED COLUMN -->
        <div class="col">
            <h2>Queued <span class="count">{{ queued|length }}</span></h2>
            {% for j in queued %}
            <div class="job {{ 'webhook' if j.source == 'WEBHOOK' else '' }}">
                <span class="job-meta">{{ j.source }} | {{ j.language }} | Size: {{ j.size }}</span>
                <span class="job-title">{{ j.repo }}</span>
                <span style="font-size: 0.75rem; color: var(--accent);">Branch: <strong>{{ j.branch }}</strong></span>
                <div style="font-size: 0.7rem; color: var(--text-dim); margin-top: 5px;">
                    Priority Score: <strong>{{ j.priority_score }}</strong>
                </div>
            </div>
            {% endfor %}
        </div>

        <!-- IN PROGRESS COLUMN -->
        <div class="col">
            <h2>In Progress <span class="count">{{ progress|length }}</span></h2>
            {% for j in progress %}
            <div class="job progress-job {{ 'webhook' if j.source == 'WEBHOOK' else '' }}">
                <span class="worker-tag">{{ j.worker }}</span>
                <span class="job-meta">{{ j.language }}</span>
                <span class="job-title">{{ j.repo }}</span>
                <span style="font-size: 0.75rem; color: var(--accent);">Branch: <strong>{{ j.branch }}</strong></span>
                
                <div class="current-stage">{{ j.current_stage }}</div>
                
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

        <!-- COMPLETED COLUMN -->
        <div class="col">
            <h2>Completed <span class="count">{{ completed|length }}</span></h2>
            {% for j in completed %}
            <div class="job comp-job {{ 'webhook' if j.source == 'WEBHOOK' else '' }}">
                <span class="job-title">{{ j.repo }}</span>
                <span style="font-size: 0.75rem; color: var(--text-dim);">Branch: <strong>{{ j.branch }}</strong></span>
                <div class="stages-container">
                    {% for s in j.stages_list %}
                        <div class="stage-item done">{{ s }}</div>
                    {% endfor %}
                </div>
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