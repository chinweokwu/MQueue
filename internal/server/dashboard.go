package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func ServeDashboard(jwtSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generate temporary token for the dashboard's API requests
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
    <title>MQueue Architectural Visualizer</title>
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
        .btn-danger { background: rgba(248, 113, 113, 0.1); border: 1px solid var(--neon-red); color: var(--neon-red); }
        .btn-danger.active { background: var(--neon-red); color: #000; font-weight: 800; border: none; }

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
            margin-bottom: 20px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 12px;
        }

        .card-title {
            font-size: 18px;
            font-weight: 700;
        }

        /* SVG Network Layout */
        .diagram-container {
            position: relative;
            width: 100%%;
            height: 540px;
            background: rgba(0, 0, 0, 0.25);
            border-radius: 12px;
            border: 1px solid rgba(255, 255, 255, 0.03);
            overflow: hidden;
        }

        .network-svg {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%%;
            height: 100%%;
            z-index: 1;
        }

        .topo-line {
            fill: none;
            stroke-width: 2px;
            stroke-dasharray: 6, 8;
            animation: dash-flow 4s linear infinite;
        }

        @keyframes dash-flow {
            to {
                stroke-dashoffset: -100;
            }
        }

        .topo-line.normal { stroke: rgba(167, 139, 250, 0.25); }
        .topo-line.active-flow { stroke: var(--neon-blue); stroke-width: 2.5px; }
        .topo-line.wal-path { stroke: var(--neon-yellow); stroke-dasharray: 4, 6; }
        .topo-line.unhealthy { stroke: rgba(248, 113, 113, 0.15); stroke-dasharray: 0; }

        /* Architectural Blocks */
        .block-node {
            position: absolute;
            z-index: 10;
            background: rgba(20, 20, 35, 0.9);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 10px;
            padding: 10px 14px;
            display: flex;
            flex-direction: column;
            gap: 6px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            cursor: pointer;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }

        .block-node:hover {
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.2);
            box-shadow: 0 8px 24px rgba(0,0,0,0.5);
        }

        .block-node.k8s { border-color: rgba(56, 189, 248, 0.4); }
        .block-node.api { border-color: rgba(167, 139, 250, 0.4); }
        .block-node.redis { border-color: rgba(167, 139, 250, 0.3); }
        .block-node.pg { border-color: rgba(52, 211, 153, 0.4); }

        .block-node.unhealthy {
            border-color: var(--neon-red) !important;
            box-shadow: 0 0 15px rgba(248, 113, 113, 0.3) !important;
            animation: glitch 1.5s infinite;
        }

        @keyframes glitch {
            0%% { transform: translate(0) }
            20%% { transform: translate(-1px, 1px) }
            40%% { transform: translate(-1px, -1px) }
            60%% { transform: translate(1px, 1px) }
            80%% { transform: translate(1px, -1px) }
            100%% { transform: translate(0) }
        }

        .block-header {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .block-desc {
            font-size: 11px;
            color: var(--text-mute);
        }

        .block-counter {
            font-family: 'JetBrains Mono', monospace;
            font-size: 16px;
            font-weight: 700;
            color: var(--neon-green);
        }

        .wal-storage {
            background: rgba(251, 191, 36, 0.05);
            border: 1px dashed rgba(251, 191, 36, 0.4);
            border-radius: 6px;
            padding: 4px 8px;
            margin-top: 4px;
            font-size: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: var(--neon-yellow);
        }

        /* Flow Particles */
        .flow-particle {
            position: absolute;
            width: 6px;
            height: 6px;
            border-radius: 50%%;
            pointer-events: none;
            z-index: 5;
        }

        .flow-particle.enqueue { background: var(--neon-blue); box-shadow: 0 0 8px var(--neon-blue); }
        .flow-particle.flush { background: var(--neon-purple); box-shadow: 0 0 8px var(--neon-purple); }
        .flow-particle.wal { background: var(--neon-yellow); box-shadow: 0 0 8px var(--neon-yellow); }
        .flow-particle.dropped { background: var(--neon-red); box-shadow: 0 0 8px var(--neon-red); }

        /* Floating Payload Tooltip */
        .flow-payload-badge {
            position: absolute;
            z-index: 15;
            background: rgba(11, 11, 20, 0.95);
            border: 1px solid rgba(255, 255, 255, 0.12);
            color: #e5e7eb;
            padding: 4px 10px;
            border-radius: 6px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 9px;
            pointer-events: none;
            white-space: nowrap;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.6);
            transform: translate(-50%%, -100%%);
            margin-top: -12px;
            animation: pulse-glow 1s infinite alternate;
        }

        @keyframes pulse-glow {
            from { box-shadow: 0 4px 12px rgba(56, 189, 248, 0.1); }
            to { box-shadow: 0 4px 12px rgba(56, 189, 248, 0.35); }
        }

        /* Sidebar Stats & Shards */
        .side-column {
            display: flex;
            flex-direction: column;
            gap: 24px;
        }

        .side-card {
            background: var(--bg-panel);
            border: 1px solid var(--border-glow);
            border-radius: 20px;
            padding: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
        }

        .telemetry-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            margin-top: 14px;
        }

        .telemetry-box {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 10px;
            padding: 14px;
            text-align: center;
        }

        .telemetry-val {
            font-family: 'JetBrains Mono', monospace;
            font-size: 18px;
            font-weight: 700;
        }

        .telemetry-label {
            font-size: 11px;
            color: var(--text-mute);
            margin-top: 4px;
        }

        .log-terminal {
            grid-column: 1 / -1;
            background: rgba(5, 5, 10, 0.85);
            border: 1px solid var(--border-glow);
            border-radius: 16px;
            padding: 16px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            color: #34d399;
            height: 160px;
            overflow-y: auto;
            box-shadow: inset 0 0 10px rgba(0,0,0,0.8);
        }

        .log-entry { margin-bottom: 4px; line-height: 1.4; }
        .log-entry.error { color: var(--neon-red); }
        .log-entry.warn { color: var(--neon-yellow); }

        .outage-switch {
            font-size: 10px;
            background: rgba(248, 113, 113, 0.1);
            color: var(--neon-red);
            border: 1px solid rgba(248, 113, 113, 0.2);
            padding: 2px 6px;
            border-radius: 4px;
            margin-top: 4px;
            text-align: center;
        }

        .dlq-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(248, 113, 113, 0.04);
            border: 1px solid rgba(248, 113, 113, 0.15);
            border-radius: 6px;
            padding: 6px 10px;
            font-size: 11px;
            margin-top: 8px;
        }
        .dlq-item-text {
            font-family: 'JetBrains Mono', monospace;
            color: #f87171;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 140px;
        }
        .dlq-item-btn {
            font-size: 10px;
            background: rgba(52, 211, 153, 0.1);
            color: var(--neon-green);
            border: 1px solid rgba(52, 211, 153, 0.2);
            padding: 2px 6px;
            border-radius: 4px;
            cursor: pointer;
        }
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
    </style>
</head>
<body>
    <header>
        <div style="display: flex; align-items: center; gap: 30px;">
            <h1>MQUEUE // FINTECH</h1>
            <nav style="display: flex; gap: 10px;">
                <a href="/dashboard" class="nav-tab active">💳 Ledger Engine</a>
                <a href="/dashboard/foqs" class="nav-tab">🎯 FOQS Core</a>
            </nav>
        </div>
        <div class="controls-top">
            <button class="btn btn-blue" onclick="produceBatch(false)">💳 Send Money ($50)</button>
            <button class="btn btn-blue" style="background: var(--neon-purple); border-color: var(--neon-purple);" onclick="produceBatch(true)">🚨 Trigger AML Flag ($15K)</button>
            <button class="btn btn-purple" onclick="consumeBatch()">🔍 Run Ledger Worker</button>
            <button class="btn btn-danger" id="btnStress" onclick="toggleStress()">⚡ Black Friday Transfers</button>
        </div>
    </header>

    <main>
        <div class="workspace-card">
            <div class="card-header">
                <div class="card-title">Distributed Data Flow Pipeline</div>
                <div style="font-size: 12px; color: var(--text-mute)">Click shards to simulate node failures</div>
            </div>

            <div class="diagram-container" id="canvas">
                <svg class="network-svg">
                    <defs>
                        <!-- Gradients -->
                        <linearGradient id="g-k8s" x1="0%%" y1="0%%" x2="100%%" y2="100%%">
                            <stop offset="0%%" stop-color="var(--neon-blue)" />
                            <stop offset="100%%" stop-color="var(--neon-purple)" />
                        </linearGradient>
                    </defs>

                    <!-- Connection Lines -->
                    <!-- Ingress paths -->
                    <path id="path-client-k8s" d="M 60,260 L 140,260" class="topo-line normal" />
                    <path id="path-k8s-api0" d="M 190,230 Q 230,100 280,100" class="topo-line normal" />
                    <path id="path-k8s-api1" d="M 190,260 L 280,265" class="topo-line normal" />
                    <path id="path-k8s-api2" d="M 190,290 Q 230,430 280,430" class="topo-line normal" />

                    <!-- API to Redis paths -->
                    <path id="path-api0-redis0" d="M 400,80 L 510,130" class="topo-line normal" />
                    <path id="path-api0-redis1" d="M 400,100 L 510,290" class="topo-line normal" />
                    <path id="path-api1-redis0" d="M 400,245 L 510,150" class="topo-line normal" />
                    <path id="path-api1-redis1" d="M 400,265 L 510,295" class="topo-line normal" />
                    <path id="path-api2-redis0" d="M 400,410 L 510,170" class="topo-line normal" />
                    <path id="path-api2-redis1" d="M 400,430 L 510,310" class="topo-line normal" />

                    <!-- Redis to Postgres paths -->
                    <path id="path-redis0-pg0" d="M 610,130 L 720,130" class="topo-line normal" />
                    <path id="path-redis1-pg1" d="M 610,390 L 720,390" class="topo-line normal" />

                    <!-- Recovery log paths (API WAL directly to Postgres) -->
                    <path id="path-wal0-pg0" d="M 400,90 Q 550,40 720,110" class="topo-line wal-path" style="display: none;" />
                    <path id="path-wal1-pg1" d="M 400,265 Q 550,265 720,380" class="topo-line wal-path" style="display: none;" />
                    <path id="path-wal2-pg1" d="M 400,420 Q 550,480 720,410" class="topo-line wal-path" style="display: none;" />
                </svg>

                <!-- Client / Traffic Origin -->
                <div class="block-node" style="left: 10px; top: 215px; border-color: var(--neon-blue)">
                    <div class="block-header">💻 Client</div>
                    <div class="block-desc">Enqueue Traffic</div>
                </div>

                <!-- Kubernetes Ingress -->
                <div class="block-node k8s" style="left: 110px; top: 215px;">
                    <div class="block-header">🌐 K8s Service</div>
                    <div class="block-desc">mqueue ClusterIP</div>
                </div>

                <!-- API Server Pods (StatefulSet Replicas) -->
                <div class="block-node api" id="node-api0" style="left: 280px; top: 50px;">
                    <div class="block-header">📦 Pod mqueue-0</div>
                    <div class="block-desc">FNV-1a Sharding</div>
                    <div class="wal-storage">
                        <span>WAL Disk:</span>
                        <span class="block-counter" id="wal0-counter" style="color: var(--neon-yellow)">0</span>
                    </div>
                </div>

                <div class="block-node api" id="node-api1" style="left: 280px; top: 215px;">
                    <div class="block-header">📦 Pod mqueue-1</div>
                    <div class="block-desc">FNV-1a Sharding</div>
                    <div class="wal-storage">
                        <span>WAL Disk:</span>
                        <span class="block-counter" id="wal1-counter" style="color: var(--neon-yellow)">0</span>
                    </div>
                </div>

                <div class="block-node api" id="node-api2" style="left: 280px; top: 380px;">
                    <div class="block-header">📦 Pod mqueue-2</div>
                    <div class="block-desc">FNV-1a Sharding</div>
                    <div class="wal-storage">
                        <span>WAL Disk:</span>
                        <span class="block-counter" id="wal2-counter" style="color: var(--neon-yellow)">0</span>
                    </div>
                </div>

                <!-- Redis Memory Buffers -->
                <div class="block-node redis" id="node-redis0" style="left: 510px; top: 90px;" onclick="toggleRedisShard(0)">
                    <div class="block-header">⚡ Redis Shard 0</div>
                    <div class="block-desc">Buffer List</div>
                    <div class="block-counter" id="redis0-counter">0</div>
                    <div class="outage-switch">Click to Fail</div>
                </div>

                <div class="block-node redis" id="node-redis1" style="left: 510px; top: 250px;" onclick="toggleRedisShard(1)">
                    <div class="block-header">⚡ Redis Shard 1</div>
                    <div class="block-desc">Buffer List</div>
                    <div class="block-counter" id="redis1-counter">0</div>
                    <div class="outage-switch">Click to Fail</div>
                </div>

                <!-- PostgreSQL Storage Shards -->
                <div class="block-node pg" id="node-pg0" style="left: 720px; top: 90px;">
                    <div class="block-header">🗄️ PG Shard 0</div>
                    <div class="block-desc">Primary DB</div>
                    <div class="block-counter" id="pg0-counter">0</div>
                </div>

                <div class="block-node pg" id="node-pg1" style="left: 720px; top: 350px;">
                    <div class="block-header">🗄️ PG Shard 1</div>
                    <div class="block-desc">Primary DB</div>
                    <div class="block-counter" id="pg1-counter">0</div>
                </div>
            </div>
        </div>

        <div class="side-column">
            <div class="side-card">
                <div class="card-title">Live Shard Clusters</div>
                <div class="telemetry-grid">
                    <div class="telemetry-box">
                        <div class="telemetry-val" id="valPoolSaturation" style="color: var(--neon-blue)">0%%</div>
                        <div class="telemetry-label">DB POOL LOAD</div>
                    </div>
                    <div class="telemetry-box">
                        <div class="telemetry-val" id="valRejections" style="color: var(--neon-red)">0</div>
                        <div class="telemetry-label">AML SUSPICIOUS HOLDS</div>
                    </div>
                    <div class="telemetry-box">
                        <div class="telemetry-val" id="valProcessed" style="color: var(--neon-green)">0</div>
                        <div class="telemetry-label">LEDGER POSTED TXS</div>
                    </div>
                    <div class="telemetry-box">
                        <div class="telemetry-val" id="valWalTotal" style="color: var(--neon-yellow)">0</div>
                        <div class="telemetry-label">WAL REPLAYS</div>
                    </div>
                </div>
            </div>

            <div class="side-card" style="margin-top: 16px;">
                <div class="card-title">Core Queue Operations</div>
                <div style="display: flex; flex-direction: column; gap: 8px; margin-top: 12px;">
                    <button class="btn btn-blue" style="justify-content: center;" onclick="produceBatchCore()">➕ Ingest Batch</button>
                    <button class="btn btn-purple" style="justify-content: center;" onclick="consumeBatchCore()">📤 Consume & Ack</button>
                    <button class="btn btn-danger" style="justify-content: center;" id="btnStressCore" onclick="toggleStressCore()">🔥 Stress Test</button>
                    <button class="btn" style="justify-content: center; background: rgba(251, 191, 36, 0.08); color: var(--neon-yellow); border: 1px solid rgba(251, 191, 36, 0.25);" id="btnChaos" onclick="toggleChaos()">💥 Activate Chaos Mode</button>
                </div>
            </div>

            <div class="side-card" style="margin-top: 16px;">
                <div class="card-title" style="color: var(--neon-red); display: flex; justify-content: space-between; align-items: center;">
                    <span>💀 Dead Letter Queue (DLQ)</span>
                    <span id="dlq-count" style="font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 700; background: rgba(248, 113, 113, 0.1); padding: 2px 8px; border-radius: 4px;">0</span>
                </div>
                <div id="dlq-container" style="max-height: 150px; overflow-y: auto; margin-top: 12px;">
                    <div style="color: var(--text-mute); font-size: 11px; text-align: center; padding: 10px 0;">No dead-lettered items.</div>
                </div>
            </div>

            <div class="side-card" style="display: flex; flex-direction: column; height: 320px;">
                <div class="card-title">Cluster Operations Log</div>
                <div class="log-terminal" id="terminal" style="height: 250px; overflow-y: auto; margin-top: 12px;">
                    <div class="log-entry">Booting MQueue visualizer node... OK</div>
                    <div class="log-entry">Poller healthy. Listening to cluster events.</div>
                </div>
            </div>
        </div>
    </main>

    <script>
        const AUTH_TOKEN = "%s";
        let apiCounter = 0; // Round robin indicator
        let stressActive = false;
        let stressInterval = null;

        // Health states
        let shardHealth = {
            redis0: true,
            redis1: true
        };

        // Node item statistics
        let counts = {
            wal0: 0,
            wal1: 0,
            wal2: 0,
            redis0: 0,
            redis1: 0,
            pg0: 0,
            pg1: 0,
            processed: 0,
            rejections: 0,
            walTotal: 0
        };

        // Sample payloads for visual floating text
        const samplePayloads = [
            '{"tx_id":"tx_8412","amt":50,"from":"@alice","to":"@bob"}',
            '{"tx_id":"tx_1024","amt":250,"from":"@charlie","to":"@dave"}',
            '{"tx_id":"tx_4950","amt":15,"from":"@eve","to":"@frank"}',
            '{"tx_id":"tx_9321","amt":800,"from":"@grace","to":"@heidi"}',
            '{"tx_id":"tx_6720","amt":95,"from":"@ivan","to":"@judy"}'
        ];

        function writeLog(message, type) {
            const terminal = document.getElementById("terminal");
            const entry = document.createElement("div");
            entry.className = "log-entry";
            if (type) entry.classList.add(type);
            entry.innerText = "[" + new Date().toLocaleTimeString() + "] " + message;
            terminal.appendChild(entry);
            
            // Limit log entries in DOM to prevent page elongation
            while (terminal.children.length > 100) {
                terminal.removeChild(terminal.firstChild);
            }
            
            terminal.scrollTop = terminal.scrollHeight;
        }

        function triggerWalRecovery(shardID) {
            const pathRecover0 = document.getElementById("path-wal0-pg" + shardID);
            const pathRecover1 = document.getElementById("path-wal1-pg" + shardID);
            const pathRecover2 = document.getElementById("path-wal2-pg" + shardID);

            // Show recovery paths flowing
            if (counts.wal0 > 0 && shardID === 0) pathRecover0.style.display = "block";
            if (counts.wal1 > 0) pathRecover1.style.display = "block";
            if (counts.wal2 > 0 && shardID === 1) pathRecover2.style.display = "block";
            
            // Drain WAL into PG Shards
            setTimeout(function() {
                for (let p = 0; p < 3; p++) {
                    const walKey = "wal" + p;
                    const walCount = counts[walKey];
                    if (walCount > 0) {
                        counts["pg" + shardID] += walCount;
                        counts.walTotal += walCount;
                        counts[walKey] = 0;
                        updateDomCounters();
                        
                        // Fire particles along recovery path with payload content label
                        const targetPath = "path-wal" + p + "-pg" + shardID;
                        for (let i = 0; i < Math.min(walCount, 10); i++) {
                            setTimeout(function() {
                                animateParticle(targetPath, "wal", "REPLAY: wal-" + p + ".log");
                            }, i * 100);
                        }
                        
                        writeLog("SUCCESS: RecoveryDaemon replayed " + walCount + " un-flushed items from rotated WAL logs of Pod mqueue-" + p + " into Postgres Shard " + shardID + ".", "warn");
                    }
                }
                pathRecover0.style.display = "none";
                pathRecover1.style.display = "none";
                pathRecover2.style.display = "none";
            }, 1200);
        }

        function toggleRedisShard(shardID) {
            const key = "redis" + shardID;
            shardHealth[key] = !shardHealth[key];
            
            const node = document.getElementById("node-redis" + shardID);
            const path0 = document.getElementById("path-api0-redis" + shardID);
            const path1 = document.getElementById("path-api1-redis" + shardID);
            const path2 = document.getElementById("path-api2-redis" + shardID);

            if (!shardHealth[key]) {
                node.classList.add("unhealthy");
                node.querySelector(".outage-switch").innerText = "Outage Simulated";
                path0.setAttribute("class", "topo-line unhealthy");
                path1.setAttribute("class", "topo-line unhealthy");
                path2.setAttribute("class", "topo-line unhealthy");
                writeLog("CRITICAL: Redis Shard " + shardID + " connection lost! Shard Offline.", "error");
            } else {
                node.classList.remove("unhealthy");
                node.querySelector(".outage-switch").innerText = "Click to Fail";
                path0.setAttribute("class", "topo-line normal");
                path1.setAttribute("class", "topo-line normal");
                path2.setAttribute("class", "topo-line normal");
                writeLog("INFO: Redis Shard " + shardID + " recovery detected. Starting WAL RecoveryDaemon...", "warn");
                triggerWalRecovery(shardID);
            }
        }

        function updateDomCounters() {
            document.getElementById("wal0-counter").innerText = counts.wal0;
            document.getElementById("wal1-counter").innerText = counts.wal1;
            document.getElementById("wal2-counter").innerText = counts.wal2;
            document.getElementById("redis0-counter").innerText = counts.redis0;
            document.getElementById("redis1-counter").innerText = counts.redis1;
            document.getElementById("pg0-counter").innerText = counts.pg0;
            document.getElementById("pg1-counter").innerText = counts.pg1;

            document.getElementById("valRejections").innerText = counts.rejections;
            document.getElementById("valProcessed").innerText = counts.processed;
            document.getElementById("valWalTotal").innerText = counts.walTotal;
        }

        let dlqItemsList = [];
        function genCID() {
            return "TX-" + Math.floor(1000 + Math.random() * 9000);
        }

        function renderDlq() {
            const container = document.getElementById("dlq-container");
            const countEl = document.getElementById("dlq-count");
            countEl.innerText = dlqItemsList.length;

            if (dlqItemsList.length === 0) {
                container.innerHTML = "<div style=\"color: var(--text-mute); font-size: 11px; text-align: center; padding: 10px 0;\">No dead-lettered items.</div>";
                return;
            }

            container.innerHTML = "";
            dlqItemsList.forEach((item, index) => {
                const div = document.createElement("div");
                div.className = "dlq-item";
                div.innerHTML = "<div class=\"dlq-item-text\" title=\"" + item.payload + "\">[" + item.cid + "] " + item.type + "</div>" +
                                "<button class=\"dlq-item-btn\" onclick=\"replayDlqItem(" + index + ")\">Replay</button>";
                container.appendChild(div);
            });
        }

        function replayDlqItem(index) {
            const item = dlqItemsList[index];
            dlqItemsList.splice(index, 1);
            renderDlq();
            writeLog("[" + item.cid + "] DLQ REPLAY: Re-ingesting transaction payload into MQueue...", "warn");
            
            if (item.isCore) {
                produceBatchCore(item.payload, item.cid);
            } else {
                produceBatch(item.payload.includes("15000"), item.payload, item.cid);
            }
        }

        function animateParticle(pathId, type, payloadText) {
            const path = document.getElementById(pathId);
            if (!path) return;
            const container = document.getElementById("canvas");
            const length = path.getTotalLength();

            // Create particle dot
            const p = document.createElement("div");
            p.className = "flow-particle " + type;
            container.appendChild(p);

            // Create floating payload badge (only if text is passed)
            let badge = null;
            if (payloadText) {
                badge = document.createElement("div");
                badge.className = "flow-payload-badge";
                badge.innerText = payloadText;
                container.appendChild(badge);
            }

            let start = null;
            const duration = 850; // ms

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

        async function produceBatch(isHighValue = false, overridePayload = null, overrideCid = null) {
            const cid = overrideCid || genCID();
            let payload = "";
            let amt = 0;
            if (overridePayload) {
                payload = overridePayload;
                amt = payload.includes("15000") ? 15000 : 50;
            } else if (isHighValue) {
                payload = '{"tx_id":"tx_9999","amt":15000,"from":"@mallory","to":"@sybil"}';
                amt = 15000;
            } else {
                const raw = samplePayloads[Math.floor(Math.random() * samplePayloads.length)];
                payload = raw;
                const parsed = JSON.parse(raw);
                amt = parsed.amt;
            }

            // 1. Client to K8s
            animateParticle("path-client-k8s", "enqueue", "[" + cid + "] Enqueue");

            // 2. K8s Ingress load balancer chooses API server (round-robin style between all 3 pods)
            const targetPod = apiCounter %% 3;
            apiCounter++;
            const ingressPath = "path-k8s-api" + targetPod;
            
            setTimeout(function() {
                animateParticle(ingressPath, "enqueue", "[" + cid + "] Route");
            }, 100);

            // 3. API Node decides target Shard using FNV-1a (simulating payload sharding)
            setTimeout(async function() {
                const targetShard = Math.floor(Math.random() * 2);
                const redisKey = "redis" + targetShard;
                const writeToRedis = shardHealth[redisKey];

                const flowPath = "path-api" + targetPod + "-redis" + targetShard;
                
                if (writeToRedis) {
                    if (isHighValue) {
                        // High-value transactions trigger Compliance verification queue!
                        animateParticle(flowPath, "wal", "[" + cid + "] Hold");
                        counts[redisKey]++;
                        updateDomCounters();
                        writeLog("[" + cid + "] AML RULE MATCHED: Transaction of $" + amt + " exceeds $10k limit. Forwarding to Compliance hold queue.", "warn");
                        
                        setTimeout(function() {
                            if (counts[redisKey] > 0) {
                                counts[redisKey]--;
                                counts.rejections++; // AML Holds
                                
                                // Push to DLQ!
                                dlqItemsList.push({
                                    cid: cid,
                                    payload: payload,
                                    type: "AML Hold ($15K)",
                                    isCore: false
                                });
                                renderDlq();
                                
                                updateDomCounters();
                                writeLog("[" + cid + "] COMPLIANCE AUDIT: Shard " + targetShard + " flagged transaction as SUSPICIOUS. Debit held in DLQ.", "error");
                            }
                        }, 900);
                        return;
                    }

                    // Normal Path: Writes to Redis buffer
                    animateParticle(flowPath, "enqueue", "[" + cid + "] Buffer");
                    counts[redisKey]++;
                    updateDomCounters();
                    writeLog("[" + cid + "] Ingested transaction resolved to Shard " + targetShard + ". Buffered in Redis.");

                    // Background DB flusher drains to PG after 800ms
                    setTimeout(function() {
                        if (counts[redisKey] > 0) {
                            counts[redisKey]--;
                            counts["pg" + targetShard]++;
                            updateDomCounters();
                            animateParticle("path-redis" + targetShard + "-pg" + targetShard, "flush", "[" + cid + "] Flush");
                            writeLog("[" + cid + "] Flusher: Persisted transaction details to PostgreSQL Shard " + targetShard + " (Reconciling balances).");
                        }
                    }, 900);
                } else {
                    // Outage path: Redis offline -> Falls back to local WAL storage on that specific pod
                    counts["wal" + targetPod]++;
                    updateDomCounters();
                    animateParticle(ingressPath, "wal", "[" + cid + "] WAL"); // Yellow particle indicates WAL logging
                    writeLog("[" + cid + "] WARNING: Redis Shard " + targetShard + " offline! Safely logged transaction to local WAL storage on pod mqueue-" + targetPod + ".", "warn");
                }
            }, 250);
        }

        async function consumeBatch() {
            const cid = genCID();
            // Find a Postgres Shard with items
            let targetShard = -1;
            if (counts.pg0 > 0) targetShard = 0;
            else if (counts.pg1 > 0) targetShard = 1;

            if (targetShard === -1) {
                writeLog("Ledger: No pending transactions to post.", "warn");
                return;
            }

            counts["pg" + targetShard]--;
            counts.processed++;
            updateDomCounters();
            
            // Flow backwards from PG to show dequeue and Ack
            animateParticle("path-redis" + targetShard + "-pg" + targetShard, "enqueue", "[" + cid + "] Dequeue");
            writeLog("[" + cid + "] Ledger Worker: Posted double-entry balances (Debit/Credit) for transaction on Shard " + targetShard + ". Dispatching SMS alert.");
        }

        function toggleStress() {
            stressActive = !stressActive;
            const btn = document.getElementById("btnStress");
            if (stressActive) {
                btn.className = "btn btn-danger active";
                btn.innerText = "🛑 Stop Surge";
                writeLog("⚡ SURGE INITIATED: Simulating massive volume of transaction requests...", "warn");
                
                stressInterval = setInterval(function() {
                    produceBatch(false);
                }, 180);
            } else {
                btn.className = "btn btn-danger";
                btn.innerText = "⚡ Black Friday Transfers";
                clearInterval(stressInterval);
                writeLog("Load surge resolved. Transaction volume stabilized.");
            }
        }

        async function produceBatchCore(overridePayload = null, overrideCid = null) {
            const cid = overrideCid || genCID();
            const payload = overridePayload || samplePayloads[Math.floor(Math.random() * samplePayloads.length)];

            // 1. Client to K8s
            animateParticle("path-client-k8s", "enqueue", "[" + cid + "] Enqueue");

            // 2. K8s Ingress load balancer chooses API server (round-robin style between all 3 pods)
            const targetPod = apiCounter %% 3;
            apiCounter++;
            const ingressPath = "path-k8s-api" + targetPod;
            
            setTimeout(function() {
                animateParticle(ingressPath, "enqueue", "[" + cid + "] Route");
            }, 100);

            // 3. API Node decides target Shard using FNV-1a (simulating payload sharding)
            setTimeout(async function() {
                const targetShard = Math.floor(Math.random() * 2);
                const redisKey = "redis" + targetShard;
                const writeToRedis = shardHealth[redisKey];

                const flowPath = "path-api" + targetPod + "-redis" + targetShard;
                
                if (writeToRedis) {
                    // Normal Path: Writes to Redis buffer
                    animateParticle(flowPath, "enqueue", "[" + cid + "] Buffer");
                    counts[redisKey]++;
                    updateDomCounters();
                    writeLog("[" + cid + "] Ingested payload resolved to Shard " + targetShard + ". Buffered in memory.");

                    // Background DB flusher drains to PG after 800ms
                    setTimeout(function() {
                        if (counts[redisKey] > 0) {
                            counts[redisKey]--;
                            counts["pg" + targetShard]++;
                            updateDomCounters();
                            animateParticle("path-redis" + targetShard + "-pg" + targetShard, "flush", "[" + cid + "] Flush");
                            writeLog("[" + cid + "] Flusher: Persisted buffered items to PostgreSQL Shard " + targetShard + ".");
                        }
                    }, 900);
                } else {
                    // Outage path: Redis offline -> Falls back to local WAL storage on that specific pod
                    counts["wal" + targetPod]++;
                    updateDomCounters();
                    animateParticle(ingressPath, "wal", "[" + cid + "] WAL"); // Yellow particle indicates WAL logging
                    writeLog("[" + cid + "] WARNING: Redis Shard " + targetShard + " is offline! Logged batch safely to local WAL storage on pod mqueue-" + targetPod + ".", "warn");
                }
            }, 250);
        }

        async function consumeBatchCore() {
            const cid = genCID();
            // Find a Postgres Shard with items
            let targetShard = -1;
            if (counts.pg0 > 0) targetShard = 0;
            else if (counts.pg1 > 0) targetShard = 1;

            if (targetShard === -1) {
                writeLog("Consumer: Queue database empty.", "warn");
                return;
            }

            counts["pg" + targetShard]--;
            counts.processed++;
            updateDomCounters();
            
            // Flow backwards from PG to show dequeue and Ack
            animateParticle("path-redis" + targetShard + "-pg" + targetShard, "enqueue", "[" + cid + "] Dequeue");
            writeLog("[" + cid + "] Consumer: Dequeued job from PG Shard " + targetShard + ". Ack sent successfully.");
        }

        let stressActiveCore = false;
        let stressIntervalCore = null;
        function toggleStressCore() {
            stressActiveCore = !stressActiveCore;
            const btn = document.getElementById("btnStressCore");
            if (stressActiveCore) {
                btn.className = "btn btn-danger active";
                btn.innerText = "🛑 Stop Stress";
                writeLog("STRESS INITIATED: Scaling enqueues up to simulate load...", "warn");
                
                stressIntervalCore = setInterval(function() {
                    produceBatchCore();
                }, 180);
            } else {
                btn.className = "btn btn-danger";
                btn.innerText = "🔥 Stress Test";
                clearInterval(stressIntervalCore);
                writeLog("STRESS CONCLUDED: Traffic returned to normal.");
            }
        }

        let chaosActive = false;
        let chaosInterval = null;
        function toggleChaos() {
            chaosActive = !chaosActive;
            const btn = document.getElementById("btnChaos");
            if (chaosActive) {
                btn.style.background = "rgba(239, 68, 68, 0.2)";
                btn.style.color = "var(--neon-red)";
                btn.style.borderColor = "var(--neon-red)";
                btn.innerText = "🛑 Stop Chaos Mode";
                writeLog("SYSTEM: Chaos Engineering active. Simulating network splits...", "warn");

                chaosInterval = setInterval(function() {
                    const cid = "CHAOS-" + Math.floor(1000 + Math.random() * 9000);
                    const shardToFail = Math.random() < 0.5 ? 0 : 1;
                    const key = "redis" + shardToFail;
                    
                    // Toggle shard health
                    shardHealth[key] = !shardHealth[key];
                    
                    const node = document.getElementById("node-" + key);
                    const path0 = document.getElementById("path-api0-" + key);
                    const path1 = document.getElementById("path-api1-" + key);
                    const path2 = document.getElementById("path-api2-" + key);

                    if (!shardHealth[key]) {
                        node.classList.add("unhealthy");
                        node.querySelector(".outage-switch").innerText = "Outage Simulated";
                        path0.setAttribute("class", "topo-line unhealthy");
                        path1.setAttribute("class", "topo-line unhealthy");
                        path2.setAttribute("class", "topo-line unhealthy");
                        writeLog("[" + cid + "] CHAOS: Redis Shard " + shardToFail + " network split detected! Shard offline.", "error");
                    } else {
                        node.classList.remove("unhealthy");
                        node.querySelector(".outage-switch").innerText = "Click to Fail";
                        path0.setAttribute("class", "topo-line normal");
                        path1.setAttribute("class", "topo-line normal");
                        path2.setAttribute("class", "topo-line normal");
                        writeLog("[" + cid + "] CHAOS: Redis Shard " + shardToFail + " connection recovered.", "warn");
                        triggerWalRecovery(shardToFail);
                    }
                }, 3500);
            } else {
                btn.style.background = "rgba(251, 191, 36, 0.08)";
                btn.style.color = "var(--neon-yellow)";
                btn.style.borderColor = "rgba(251, 191, 36, 0.25)";
                btn.innerText = "💥 Activate Chaos Mode";
                clearInterval(chaosInterval);
                writeLog("SYSTEM: Chaos Mode disabled. Network topology stabilized.");
                
                // Restore all shards
                for (let i = 0; i < 2; i++) {
                    const key = "redis" + i;
                    if (!shardHealth[key]) {
                        shardHealth[key] = true;
                        const node = document.getElementById("node-" + key);
                        const path0 = document.getElementById("path-api0-" + key);
                        const path1 = document.getElementById("path-api1-" + key);
                        const path2 = document.getElementById("path-api2-" + key);

                        node.classList.remove("unhealthy");
                        node.querySelector(".outage-switch").innerText = "Click to Fail";
                        path0.setAttribute("class", "topo-line normal");
                        path1.setAttribute("class", "topo-line normal");
                        path2.setAttribute("class", "topo-line normal");
                        triggerWalRecovery(i);
                    }
                }
            }
        }

        async function fetchLiveStats() {
            try {
                const response = await fetch('/api/dashboard/stats', {
                    headers: { 'Authorization': 'Bearer ' + AUTH_TOKEN }
                });
                const data = await response.json();
                
                const saturation = Math.round(data.db_pool_saturation);
                const satEl = document.getElementById("valPoolSaturation");
                satEl.innerText = saturation + "%%";
                
                if (saturation > 80) {
                    satEl.style.color = "var(--neon-red)";
                } else {
                    satEl.style.color = "var(--neon-blue)";
                }

                // Reset all pod styles
                document.querySelectorAll('.block-node.api').forEach(el => {
                    el.style.boxShadow = '';
                    el.style.borderColor = '';
                });

                // Highlight the active serving pod
                const activePod = data.served_by;
                if (activePod) {
                    let podId = '';
                    if (activePod.endsWith('-0')) podId = 'node-api0';
                    else if (activePod.endsWith('-1')) podId = 'node-api1';
                    else if (activePod.endsWith('-2')) podId = 'node-api2';

                    if (podId) {
                        const podEl = document.getElementById(podId);
                        if (podEl) {
                            podEl.style.boxShadow = '0 0 20px var(--neon-blue)';
                            podEl.style.borderColor = 'var(--neon-blue)';
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
