const crypto = require("crypto");

function hashIP(ip) {
    return crypto.createHash("md5").update(ip).digest("hex");
}

function getStickyBackend(ip) {
    const hash = parseInt(hashIP(ip).substring(0, 8), 16);
    const index = hash % backends.length;
    return backends[index];
}

// In proxy server:
const backend = getStickyBackend(clientReq.socket.remoteAddress);