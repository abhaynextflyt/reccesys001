const http = require("http");
const crypto = require("crypto");

// ========== CONFIGURATION ==========
const BACKENDS = [
    { host: "localhost", port: 3001, name: "B-1", weight: 5, connections: 0, alive: true },
    { host: "localhost", port: 3002, name: "B-2", weight: 3, connections: 0, alive: true },
    { host: "localhost", port: 3003, name: "B-3", weight: 2, connections: 0, alive: true }
];

const CONFIG = {
    healthCheckInterval: 5000,
    healthCheckTimeout: 2000,
    requestTimeout: 5000,
    rateLimitWindow: 60000,
    rateLimitMax: 100,
    maxSockets: 50,
    keepAlive: true
};

// ========== CONNECTION POOL ==========
const agent = new http.Agent({
    keepAlive: CONFIG.keepAlive,
    maxSockets: CONFIG.maxSockets,
    timeout: 60000,
    freeSocketTimeout: 30000
});

// ========== HEALTH CHECKS ==========
function checkHealth() {
    BACKENDS.forEach(backend => {
        const req = http.get({
            host: backend.host,
            port: backend.port,
            path: "/health",
            agent: agent
        }, (res) => {
            const wasAlive = backend.alive;
            backend.alive = res.statusCode === 200;
            if (wasAlive !== backend.alive) {
                console.log(`[HEALTH] ${backend.name} is ${backend.alive ? "UP" : "DOWN"}`);
            }
        });
        
        req.on("error", () => {
            if (backend.alive) {
                console.log(`[HEALTH] ${backend.name} is DOWN!`);
                backend.alive = false;
            }
        });
        
        req.setTimeout(CONFIG.healthCheckTimeout, () => req.destroy());
    });
}

setInterval(checkHealth, CONFIG.healthCheckInterval);

// ========== RATE LIMITING ==========
const rateLimitMap = new Map();

function isRateLimited(ip) {
    const now = Date.now();
    const timestamps = rateLimitMap.get(ip) || [];
    const valid = timestamps.filter(t => now - t < CONFIG.rateLimitWindow);
    
    rateLimitMap.set(ip, valid);
    
    if (valid.length >= CONFIG.rateLimitMax) return true;
    valid.push(now);
    return false;
}

// ========== LOAD BALANCING: WEIGHTED LEAST CONNECTIONS ==========
function getBackend(ip, useSticky = false) {
    const alive = BACKENDS.filter(b => b.alalive);
    if (alive.length === 0) return null;
    
    // Sticky session: hash IP to pick backend
    if (useSticky) {
        const hash = parseInt(crypto.createHash("md5").update(ip).digest("hex").substring(0, 8), 16);
        return alive[hash % alive.length];
    }
    
    // Weighted least connections
    return alive.reduce((best, current) => {
        const bestScore = best.connections / best.weight;
        const currentScore = current.connections / current.weight;
        return currentScore < bestScore ? current : best;
    });
}

// ========== PROXY SERVER ==========
http.createServer((clientReq, clientRes) => {
    const clientIP = clientReq.socket.remoteAddress;
    
    // Rate limit check
    if (isRateLimited(clientIP)) {
        clientRes.writeHead(429);
        return clientRes.end("Too many requests");
    }
    
    // Get backend
    const backend = getBackend(clientIP, false);
    if (!backend) {
        clientRes.writeHead(503);
        return clientRes.end("Service unavailable");
    }
    
    console.log(`[PROXY] ${clientReq.method} ${clientReq.url} → ${backend.name} (active: ${backend.connections})`);
    
    backend.connections++;
    
    const options = {
        hostname: backend.host,
        port: backend.port,
        path: clientReq.url,
        method: clientReq.method,
        headers: clientReq.headers,
        agent: agent
    };
    
    const proxyReq = http.request(options, (proxyRes) => {
        clientRes.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(clientRes);
        
        proxyRes.on("end", () => {
            backend.connections--;
            console.log(`[PROXY] ${backend.name} done (active: ${backend.connections})`);
        });
    });
    
    // Timeout
    proxyReq.setTimeout(CONFIG.requestTimeout, () => {
        proxyReq.destroy();
        backend.connections--;
        clientRes.writeHead(504);
        clientRes.end("Gateway timeout");
    });
    
    // Error handling
    proxyReq.on("error", (err) => {
        backend.connections--;
        backend.alive = false;
        console.error(`[PROXY] ${backend.name} error: ${err.message}`);
        clientRes.writeHead(502);
        clientRes.end("Bad gateway");
    });
    
    // Forward request body
    clientReq.pipe(proxyReq);
    
}).listen(8080, () => {
    console.log("Scalable proxy on port 8080");
    checkHealth(); // Initial check
});