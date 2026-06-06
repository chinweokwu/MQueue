package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func ServeFOQSDashboard(jwtSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"allowed_namespaces": "*",
			"scopes":             []string{"*:*"},
			"exp":                time.Now().Add(24 * time.Hour).Unix(),
		})
		tokenStr, err := token.SignedString([]byte(jwtSecret))
		if err != nil {
			http.Error(w, "Failed to generate dashboard token", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Del("Content-Security-Policy")

		fmt.Fprint(w, fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MQueue FOQS Core Engine</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-dark: #07070c;
            --bg-panel: rgba(14, 14, 23, 0.75);
            --border-glow: rgba(139, 92, 246, 0.15);
            --neon-purple: #a78bfa;
            --neon-blue: #38bdf8;
            --neon-green: #34d399;
            --neon-red: #f87171;
            --neon-yellow: #fbbf24;
            --text-main: #f3f4f6;
            --text-mute: #9ca3af;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background-color: var(--bg-dark);
            background-image: 
                radial-gradient(at 0%% 0%%, rgba(139, 92, 246, 0.12) 0px, transparent 40%%),
                radial-gradient(at 100%% 100%%, rgba(56, 189, 248, 0.08) 0px, transparent 40%%);
            background-attachment: fixed;
            font-family: 'Outfit', sans-serif;
            color: var(--text-main);
            min-height: 100vh;
            overflow-x: hidden;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 40px;
            backdrop-filter: blur(12px);
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            background: rgba(7, 7, 12, 0.4);
        }

        header h1 {
            font-size: 24px;
            font-weight: 800;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, var(--neon-blue), var(--neon-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .controls-top {
            display: flex;
            gap: 12px;
            align-items: center;
        }

        .btn {
            padding: 10px 20px;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            background: rgba(255, 255, 255, 0.03);
            color: var(--text-main);
            font-family: 'Outfit', sans-serif;
            font-weight: 600;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .btn:hover {
            transform: translateY(-1px);
            border-color: rgba(255, 255, 255, 0.2);
            background: rgba(255, 255, 255, 0.07);
        }

        .btn-blue { background: linear-gradient(135deg, #0284c7, #0369a1); border: none; color: #fff; }
        .btn-blue:hover { box-shadow: 0 0 15px rgba(56, 189, 248, 0.3); }
        .btn-purple { background: linear-gradient(135deg, #7c3aed, #6d28d9); border: none; color: #fff; }
        .btn-purple:hover { box-shadow: 0 0 15px rgba(167, 139, 250, 0.3); }
        .btn-green { background: linear-gradient(135deg, #059669, #047857); border: none; color: #fff; }
        .btn-green:hover { box-shadow: 0 0 15px rgba(52, 211, 153, 0.3); }

        main {
            max-width: 1600px;
            margin: 20px auto;
            padding: 0 20px;
            display: grid;
            grid-template-columns: 3.2fr 1fr;
            gap: 24px;
        }

        .workspace-card {
            background: var(--bg-panel);
            border: 1px solid var(--border-glow);
            border-radius: 20px;
            padding: 24px;
            backdrop-filter: blur(20px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
            position: relative;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 15px;
            margin-bottom: 20px;
        }

        .card-title {
            font-size: 18px;
            font-weight: 600;
            color: var(--text-main);
        }

        .diagram-container {
            position: relative;
            width: 100%%;
            height: 600px;
            background: rgba(0, 0, 0, 0.2);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.03);
        }

        .network-svg {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%%;
            height: 100%%;
            z-index: 1;
            pointer-events: none;
        }

        .topo-line {
            fill: none;
            stroke-width: 2;
            stroke-dasharray: 6, 6;
            animation: dash 30s linear infinite;
            transition: stroke 0.5s, stroke-width 0.5s;
        }

        .topo-line.normal { stroke: rgba(139, 92, 246, 0.2); }
        .topo-line.prefetch { stroke: rgba(52, 211, 153, 0.35); stroke-width: 2.5; }
        .topo-line.active-dequeue { stroke: rgba(56, 189, 248, 0.4); stroke-width: 2.5; }

        @keyframes dash {
            to { stroke-dashoffset: -1000; }
        }

        .block-node {
            position: absolute;
            background: rgba(20, 20, 35, 0.85);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 12px;
            padding: 12px 16px;
            z-index: 2;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 4px 12px rgba(0,0,0,0.5);
        }

        .block-node:hover {
            border-color: rgba(255, 255, 255, 0.2);
            transform: translateY(-2px);
        }

        .block-node .node-title {
            font-size: 11px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .node-client   { top: 260px; left: 40px; width: 120px; border-color: rgba(255, 255, 255, 0.15); }
        .node-k8s      { top: 260px; left: 220px; width: 130px; border-color: var(--neon-purple); }
        .node-api0     { top: 100px; left: 420px; width: 150px; }
        .node-api1     { top: 260px; left: 420px; width: 150px; }
        .node-api2     { top: 420px; left: 420px; width: 150px; }
        
        .node-prefetcher { top: 420px; left: 660px; width: 160px; border-color: var(--neon-green); box-shadow: 0 0 10px rgba(52, 211, 153, 0.1); }
        .node-redis      { top: 120px; left: 660px; width: 160px; border-color: var(--neon-blue); }
        .node-postgres   { top: 260px; left: 890px; width: 170px; border-color: var(--neon-yellow); }

        .counter-badge {
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            color: var(--text-main);
            background: rgba(255, 255, 255, 0.05);
            padding: 4px 8px;
            border-radius: 6px;
            display: inline-block;
            margin-top: 4px;
            border: 1px solid rgba(255,255,255,0.05);
        }

        .flow-particle {
            position: absolute;
            width: 8px;
            height: 8px;
            border-radius: 50%%;
            z-index: 10;
            pointer-events: none;
            box-shadow: 0 0 10px currentColor;
        }
        .flow-particle.enqueue { background-color: var(--neon-purple); color: var(--neon-purple); }
        .flow-particle.prefetch { background-color: var(--neon-green); color: var(--neon-green); }
        .flow-particle.dequeue { background-color: var(--neon-blue); color: var(--neon-blue); }
        .flow-particle.delay { background-color: var(--neon-yellow); color: var(--neon-yellow); }

        .flow-payload-badge {
            position: absolute;
            font-family: 'JetBrains Mono', monospace;
            font-size: 9px;
            background: rgba(10, 10, 15, 0.9);
            border: 1px solid rgba(255,255,255,0.15);
            color: var(--text-main);
            padding: 2px 6px;
            border-radius: 4px;
            z-index: 11;
            pointer-events: none;
            white-space: nowrap;
            transform: translate(-50%%, -120%%);
        }

        /* Sidebar Panels */
        .sidebar-panel {
            display: flex;
            flex-direction: column;
            gap: 24px;
        }

        .sidebar-card {
            background: var(--bg-panel);
            border: 1px solid var(--border-glow);
            border-radius: 20px;
            padding: 20px;
            backdrop-filter: blur(20px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }

        .sidebar-card-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-mute);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 15px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 8px;
        }

        .telemetry-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }

        .telemetry-item {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 10px;
            padding: 12px;
            text-align: center;
        }

        .telemetry-label {
            font-size: 10px;
            color: var(--text-mute);
            text-transform: uppercase;
            margin-bottom: 4px;
        }

        .telemetry-val {
            font-size: 20px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
        }

        .log-terminal {
            background: #020204;
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 15px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
            height: 250px;
            overflow-y: auto;
            box-shadow: inset 0 0 10px rgba(0,0,0,0.8);
        }

        .log-line {
            margin-bottom: 6px;
            line-height: 1.4;
            color: var(--text-mute);
        }

        .log-line.warn { color: var(--neon-yellow); }
        .log-line.error { color: var(--neon-red); }
        .log-line.success { color: var(--neon-green); }

        .nav-tab {
            text-decoration: none;
            color: var(--text-mute);
            font-weight: 600;
            font-size: 14px;
            padding: 8px 16px;
            border-radius: 20px;
            transition: all 0.2s;
            border: 1px solid transparent;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }
        .nav-tab:hover {
            color: var(--text-main);
            background: rgba(255, 255, 255, 0.05);
        }
        .nav-tab.active {
            color: var(--neon-blue);
            background: rgba(56, 189, 248, 0.08);
            border-color: rgba(56, 189, 248, 0.2);
        }

        .priority-badge {
            display: inline-block;
            font-size: 9px;
            font-weight: 800;
            padding: 2px 6px;
            border-radius: 4px;
            text-transform: uppercase;
        }
        .priority-high { background: rgba(248, 113, 113, 0.1); color: var(--neon-red); border: 1px solid rgba(248, 113, 113, 0.2); }
        .priority-med  { background: rgba(251, 191, 36, 0.1); color: var(--neon-yellow); border: 1px solid rgba(251, 191, 36, 0.2); }
        .priority-low  { background: rgba(56, 189, 248, 0.1); color: var(--neon-blue); border: 1px solid rgba(56, 189, 248, 0.2); }

        .queue-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
            max-height: 180px;
            overflow-y: auto;
            margin-top: 10px;
        }
        .queue-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(255,255,255,0.02);
            border: 1px solid rgba(255,255,255,0.05);
            border-radius: 6px;
            padding: 6px 10px;
            font-size: 11px;
        }
        .queue-item-text {
            font-family: 'JetBrains Mono', monospace;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 160px;
        }
    </style>
</head>
<body>
    <header>
        <div style="display: flex; align-items: center; gap: 30px;">
            <h1>MQUEUE // FOQS CORE</h1>
            <nav style="display: flex; gap: 10px;">
                <a href="/dashboard" class="nav-tab">💳 Ledger Engine</a>
                <a href="/dashboard/foqs" class="nav-tab active">🎯 FOQS Core</a>
            </nav>
        </div>
        <div class="controls-top">
            <div style="display: flex; align-items: center; gap: 8px; background: rgba(255,255,255,0.03); padding: 4px 12px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.08);">
                <label style="font-size: 12px; font-weight: 600; color: var(--text-mute);">Priority:</label>
                <select id="selPriority" style="background: #0c0c14; border: 1px solid rgba(255,255,255,0.15); color: #fff; border-radius: 4px; padding: 4px 8px; font-family: 'Outfit';">
                    <option value="1">High (Pri 1)</option>
                    <option value="5">Normal (Pri 5)</option>
                    <option value="10">Low (Pri 10)</option>
                </select>
            </div>
            <div style="display: flex; align-items: center; gap: 8px; background: rgba(255,255,255,0.03); padding: 4px 12px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.08);">
                <label style="font-size: 12px; font-weight: 600; color: var(--text-mute);">Delay:</label>
                <select id="selDelay" style="background: #0c0c14; border: 1px solid rgba(255,255,255,0.15); color: #fff; border-radius: 4px; padding: 4px 8px; font-family: 'Outfit';">
                    <option value="0">Immediate</option>
                    <option value="5">5s Delay</option>
                    <option value="15">15s Delay</option>
                </select>
            </div>
            <button class="btn btn-blue" onclick="enqueueTaskClick()">📥 Enqueue Task</button>
            <button class="btn btn-green" onclick="triggerPrefetchClick()">🔄 Trigger Prefetch</button>
            <button class="btn btn-purple" onclick="dequeueTaskClick()">🔍 Consume Task</button>
        </div>
    </header>

    <main>
        <div class="workspace-card">
            <div class="card-header">
                <div class="card-title">FOQS Priority & Dequeue Pipeline</div>
                <div style="font-size: 12px; color: var(--text-mute)">Watch background prefetcher load tasks from Postgres to Redis</div>
            </div>

            <div class="diagram-container" id="canvas">
                <svg class="network-svg">
                    <defs>
                        <linearGradient id="g-k8s" x1="0%%" y1="0%%" x2="100%%" y2="100%%">
                            <stop offset="0%%" stop-color="var(--neon-blue)" />
                            <stop offset="100%%" stop-color="var(--neon-purple)" />
                        </linearGradient>
                    </defs>

                    <!-- Path mappings -->
                    <path id="path-client-k8s" d="M 60,285 L 220,285" class="topo-line normal" />
                    
                    <!-- K8s -> API pods -->
                    <path id="path-k8s-api0" d="M 285,260 Q 330,120 420,120" class="topo-line normal" />
                    <path id="path-k8s-api1" d="M 330,285 L 420,285" class="topo-line normal" />
                    <path id="path-k8s-api2" d="M 285,310 Q 330,440 420,440" class="topo-line normal" />

                    <!-- API pods -> Postgres (for delayed/persistent enqueue) -->
                    <path id="path-api0-pg" d="M 570,120 Q 730,90 890,265" class="topo-line normal" />
                    <path id="path-api1-pg" d="M 570,285 L 890,285" class="topo-line normal" />
                    <path id="path-api2-pg" d="M 570,440 Q 730,460 890,305" class="topo-line normal" />

                    <!-- API pods -> Redis (for normal fast pipeline) -->
                    <path id="path-api0-redis" d="M 570,120 L 660,135" class="topo-line normal" />
                    <path id="path-api1-redis" d="M 570,285 L 660,165" class="topo-line normal" />
                    <path id="path-api2-redis" d="M 570,440 L 660,180" class="topo-line normal" />

                    <!-- Prefetcher backward loops (PG to Redis) -->
                    <path id="path-pg-prefetch" d="M 975,325 L 750,420" class="topo-line prefetch" />
                    <path id="path-prefetch-redis" d="M 720,420 L 720,195" class="topo-line prefetch" />

                    <!-- Dequeue path (Redis to Client/Consumer) -->
                    <path id="path-redis-consumer" d="M 720,120 Q 480,40 100,260" class="topo-line active-dequeue" />
                </svg>

                <!-- Nodes -->
                <div class="block-node node-client">
                    <div class="node-title">💻 Client</div>
                    <div style="font-size: 11px; color: var(--text-mute);">Payload Sender</div>
                </div>

                <div class="block-node node-k8s">
                    <div class="node-title">☸️ K8s Ingress</div>
                    <div style="font-size: 10px; color: var(--text-mute);">Round-Robin LB</div>
                </div>

                <div class="block-node node-api0" id="node-api0">
                    <div class="node-title">⚡ Pod mqueue-0</div>
                    <span class="counter-badge">Active Serving</span>
                </div>

                <div class="block-node node-api1" id="node-api1">
                    <div class="node-title">⚡ Pod mqueue-1</div>
                    <span class="counter-badge">Active Serving</span>
                </div>

                <div class="block-node node-api2" id="node-api2">
                    <div class="node-title">⚡ Pod mqueue-2</div>
                    <span class="counter-badge">Active Serving</span>
                </div>

                <div class="block-node node-redis" id="node-redis">
                    <div class="node-title" style="color: var(--neon-blue);">🧠 Redis Memory</div>
                    <div style="font-size: 10px; color: var(--text-mute);">ZSET Priority Queue</div>
                    <span class="counter-badge" id="redis-counter" style="color: var(--neon-blue)">0 items</span>
                </div>

                <div class="block-node node-prefetcher" id="node-prefetcher">
                    <div class="node-title" style="color: var(--neon-green);">🔄 Prefetcher</div>
                    <div style="font-size: 10px; color: var(--text-mute);">Leases SQL items</div>
                    <span class="counter-badge" id="prefetch-counter" style="color: var(--neon-green)">Idle</span>
                </div>

                <div class="block-node node-postgres" id="node-postgres">
                    <div class="node-title" style="color: var(--neon-yellow);">💾 Postgres Shards</div>
                    <div style="font-size: 10px; color: var(--text-mute);">Sharded Durable Store</div>
                    <span class="counter-badge" id="pg-counter" style="color: var(--neon-yellow)">0 items</span>
                </div>
            </div>
        </div>

        <div class="sidebar-panel">
            <div class="sidebar-card">
                <div class="sidebar-card-title">FOQS Core Metrics</div>
                <div class="telemetry-grid">
                    <div class="telemetry-item">
                        <div class="telemetry-label">Prefetched</div>
                        <div class="telemetry-val" id="valPrefetched" style="color: var(--neon-green);">0</div>
                    </div>
                    <div class="telemetry-item">
                        <div class="telemetry-label">Delayed Count</div>
                        <div class="telemetry-val" id="valDelayed" style="color: var(--neon-yellow);">0</div>
                    </div>
                    <div class="telemetry-item" style="grid-column: span 2;">
                        <div class="telemetry-label">Completed Jobs</div>
                        <div class="telemetry-val" id="valProcessed" style="color: var(--neon-blue);">0</div>
                    </div>
                </div>
            </div>

            <div class="sidebar-card">
                <div class="sidebar-card-title">Memory Queue (Redis ZSET)</div>
                <div class="queue-list" id="redis-list">
                    <div style="color: var(--text-mute); font-size: 11px; text-align: center; padding: 10px 0;">Redis priority cache empty.</div>
                </div>
            </div>

            <div class="sidebar-card">
                <div class="sidebar-card-title">Cluster Operations Log</div>
                <div class="log-terminal" id="terminal">
                    <div class="log-line success">SYSTEM: MQueue FOQS Core Engine Initialized. Ready for priority transactions.</div>
                </div>
            </div>
        </div>
    </main>

    <script>
        const AUTH_TOKEN = "%s";
        let counts = {
            prefetched: 0,
            delayed: 0,
            processed: 0
        };

        let pgItemsList = [];
        let redisItemsList = [];
        let apiCounter = 0;

        function genCID() {
            return "TX-" + Math.floor(1000 + Math.random() * 9000);
        }

        function writeLog(message, type = "info") {
            const terminal = document.getElementById("terminal");
            const line = document.createElement("div");
            line.className = "log-line " + type;
            line.innerText = "[" + new Date().toLocaleTimeString() + "] " + message;
            terminal.appendChild(line);
            terminal.scrollTop = terminal.scrollHeight;
        }

        function getPriorityClass(p) {
            if (p === 1) return "priority-high";
            if (p === 5) return "priority-med";
            return "priority-low";
        }

        function renderLists() {
            // Update counts in DOM
            document.getElementById("redis-counter").innerText = redisItemsList.length + " items";
            document.getElementById("pg-counter").innerText = pgItemsList.length + " items";
            
            document.getElementById("valPrefetched").innerText = counts.prefetched;
            document.getElementById("valDelayed").innerText = counts.delayed;
            document.getElementById("valProcessed").innerText = counts.processed;

            // Render Redis ZSET
            const redisList = document.getElementById("redis-list");
            if (redisItemsList.length === 0) {
                redisList.innerHTML = "<div style=\"color: var(--text-mute); font-size: 11px; text-align: center; padding: 10px 0;\">Redis priority cache empty.</div>";
            } else {
                redisList.innerHTML = "";
                // Sort by priority ASC
                redisItemsList.sort((a,b) => a.priority - b.priority);
                redisItemsList.forEach(item => {
                    const div = document.createElement("div");
                    div.className = "queue-item";
                    div.innerHTML = "<span class=\"priority-badge " + getPriorityClass(item.priority) + "\">Pri " + item.priority + "</span>" +
                                    "<span class=\"queue-item-text\">" + item.cid + "</span>";
                    redisList.appendChild(div);
                });
            }
        }

        function animateParticle(pathId, type, payloadText, duration = 850) {
            const path = document.getElementById(pathId);
            if (!path) return;
            const container = document.getElementById("canvas");
            const length = path.getTotalLength();

            const p = document.createElement("div");
            p.className = "flow-particle " + type;
            container.appendChild(p);

            let badge = null;
            if (payloadText) {
                badge = document.createElement("div");
                badge.className = "flow-payload-badge";
                badge.innerText = payloadText;
                container.appendChild(badge);
            }

            let start = null;
            function step(timestamp) {
                if (!start) start = timestamp;
                const progress = timestamp - start;
                const percent = Math.min(progress / duration, 1);
                
                const point = path.getPointAtLength(percent * length);
                p.style.left = (point.x - 3) + "px";
                p.style.top = (point.y - 3) + "px";

                if (badge) {
                    badge.style.left = point.x + "px";
                    badge.style.top = point.y + "px";
                }

                if (progress < duration) {
                    requestAnimationFrame(step);
                } else {
                    p.remove();
                    if (badge) badge.remove();
                }
            }
            requestAnimationFrame(step);
        }

        function enqueueTaskClick() {
            const priority = parseInt(document.getElementById("selPriority").value);
            const delay = parseInt(document.getElementById("selDelay").value);
            const cid = genCID();
            const now = Date.now();
            const deliverAfter = now + (delay * 1000);

            // 1. Client to K8s
            animateParticle("path-client-k8s", "enqueue", "[" + cid + "] Enqueue");

            // 2. K8s Ingress
            const targetPod = apiCounter %% 3;
            apiCounter++;
            const ingressPath = "path-k8s-api" + targetPod;
            setTimeout(() => {
                animateParticle(ingressPath, "enqueue", "[" + cid + "] Route");
            }, 100);

            // 3. API Pod handles
            setTimeout(() => {
                const apiPath = "path-api" + targetPod + (delay > 0 ? "-pg" : "-redis");
                const particleType = delay > 0 ? "delay" : "enqueue";
                const label = delay > 0 ? "DELAY: " + delay + "s" : "PRI: " + priority;
                
                animateParticle(apiPath, particleType, "[" + cid + "] " + label);

                // Add to collections
                const item = {
                    cid: cid,
                    priority: priority,
                    deliverAfter: deliverAfter,
                    prefetched: false,
                    isDelayed: delay > 0
                };

                if (delay > 0) {
                    counts.delayed++;
                    pgItemsList.push(item);
                    writeLog("[" + cid + "] Ingested DELAYED task (Delay: " + delay + "s, Priority: " + priority + ") stored in PostgreSQL Shards.", "warn");
                } else {
                    // Normal instant route (Ingests to Redis directly)
                    redisItemsList.push(item);
                    writeLog("[" + cid + "] Ingested IMMEDIATE task (Priority: " + priority + ") buffered to Redis memory queue.");
                }
                renderLists();
            }, 250);
        }

        function triggerPrefetchClick() {
            const prefetcher = document.getElementById("node-prefetcher");
            prefetcher.style.boxShadow = "0 0 20px var(--neon-green)";
            prefetcher.style.borderColor = "var(--neon-green)";
            document.getElementById("prefetch-counter").innerText = "Leasing...";

            writeLog("[PREFETCH] Background daemon scanning PostgreSQL shards for ready tasks...", "success");

            setTimeout(() => {
                const now = Date.now();
                // Find PG tasks that are ready (deliverAfter <= now) and not yet prefetched
                const readyItems = pgItemsList.filter(item => !item.prefetched && item.deliverAfter <= now);

                if (readyItems.length === 0) {
                    writeLog("[PREFETCH] Scan completed: 0 items ready in PostgreSQL.", "success");
                    document.getElementById("prefetch-counter").innerText = "Idle";
                    prefetcher.style.boxShadow = "";
                    prefetcher.style.borderColor = "";
                    return;
                }

                // Sort ready items by priority (ASC) then deliverAfter (ASC)
                readyItems.sort((a, b) => {
                    if (a.priority !== b.priority) return a.priority - b.priority;
                    return a.deliverAfter - b.deliverAfter;
                });

                // Take up to 3 items
                const batch = readyItems.slice(0, 3);
                
                batch.forEach((item, index) => {
                    // Mark as prefetched in main list
                    item.prefetched = true;
                    pgItemsList = pgItemsList.filter(i => i.cid !== item.cid);
                    
                    setTimeout(() => {
                        // Animate from PG -> Prefetcher
                        animateParticle("path-pg-prefetch", "prefetch", "LEASE: " + item.cid, 600);
                        
                        // Then Prefetcher -> Redis
                        setTimeout(() => {
                            animateParticle("path-prefetch-redis", "prefetch", "CACHE: " + item.cid, 600);
                            redisItemsList.push(item);
                            counts.prefetched++;
                            renderLists();
                            writeLog("[PREFETCH] Leased ready task " + item.cid + " (Priority: " + item.priority + ") and cached in Redis ZSET.");
                        }, 600);
                    }, index * 150);
                });

                setTimeout(() => {
                    document.getElementById("prefetch-counter").innerText = "Idle";
                    prefetcher.style.boxShadow = "";
                    prefetcher.style.borderColor = "";
                }, batch.length * 150 + 1200);

            }, 800);
        }

        function dequeueTaskClick() {
            const cid = genCID();
            
            if (redisItemsList.length === 0) {
                writeLog("[" + cid + "] Dequeue request: Redis warm cache is empty! Waking up Prefetcher...", "warn");
                triggerPrefetchClick();
                return;
            }

            // Dequeue the highest priority task (lowest priority value)
            redisItemsList.sort((a,b) => a.priority - b.priority);
            const activeItem = redisItemsList.shift();
            
            counts.processed++;
            renderLists();

            // Dequeue Animation from Redis back to client/consumer
            animateParticle("path-redis-consumer", "dequeue", "[" + activeItem.cid + "] Dequeue");
            writeLog("[" + activeItem.cid + "] Consumer: Dequeued task successfully. (Task Priority: " + activeItem.priority + ")", "success");
        }

        async function fetchLiveStats() {
            try {
                const response = await fetch('/api/dashboard/stats', {
                    headers: { 'Authorization': 'Bearer ' + AUTH_TOKEN }
                });
                const data = await response.json();
                
                // Reset pod highlights
                document.querySelectorAll('.block-node.api').forEach(el => {
                    el.style.boxShadow = '';
                    el.style.borderColor = '';
                });

                const activePod = data.served_by;
                if (activePod) {
                    let podId = '';
                    if (activePod.endsWith('-0')) podId = 'node-api0';
                    else if (activePod.endsWith('-1')) podId = 'node-api1';
                    else if (activePod.endsWith('-2')) podId = 'node-api2';

                    if (podId) {
                        const podEl = document.getElementById(podId);
                        if (podEl) {
                            podEl.style.boxShadow = '0 0 15px var(--neon-purple)';
                            podEl.style.borderColor = 'var(--neon-purple)';
                        }
                    }
                }
            } catch(e) {}
        }

        setInterval(fetchLiveStats, 2000);
        fetchLiveStats();
    </script>
</body>
</html>
`, tokenStr))
	}
}
