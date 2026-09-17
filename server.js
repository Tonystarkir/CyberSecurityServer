const express = require("express");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const sessions = new Map();

app.use(express.static("public"));
app.get("/health", (_req, res) => res.json({ ok: true }));

function id() { return crypto.randomBytes(16).toString("hex"); }
function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

wss.on("connection", (ws) => {
  let sessionId = null, role = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch {
      return send(ws, { type: "error", message: "Invalid message." });
    }

    if (msg.type === "create-session") {
      sessionId = id(); role = "host";
      sessions.set(sessionId, { host: ws, guest: null, createdAt: Date.now() });
      return send(ws, { type: "session-created", sessionId });
    }

    if (msg.type === "join-session") {
      sessionId = String(msg.sessionId || "");
      const s = sessions.get(sessionId);
      if (!s) return send(ws, { type: "error", message: "Session not found or expired." });
      if (s.guest) return send(ws, { type: "error", message: "Session already has a guest." });
      role = "guest"; s.guest = ws;
      send(ws, { type: "joined", sessionId });
      return send(s.host, { type: "guest-joined" });
    }

    const s = sessions.get(sessionId);
    if (!s) return send(ws, { type: "error", message: "Session expired." });

    if (msg.type === "consent-granted" && role === "guest")
      return send(s.host, { type: "consent-granted" });

    if (["offer", "answer", "ice-candidate", "stop-session"].includes(msg.type)) {
      const target = role === "host" ? s.guest : s.host;
      send(target, { ...msg, from: role });
      if (msg.type === "stop-session") sessions.delete(sessionId);
    }
  });

  ws.on("close", () => {
    const s = sessions.get(sessionId);
    if (s) {
      send(role === "host" ? s.guest : s.host, { type: "peer-disconnected" });
      sessions.delete(sessionId);
    }
  });
});

setInterval(() => {
  const cutoff = Date.now() - 15 * 60 * 1000;
  for (const [key, s] of sessions) {
    if (s.createdAt < cutoff) {
      send(s.host, { type: "session-expired" });
      send(s.guest, { type: "session-expired" });
      sessions.delete(key);
    }
  }
}, 60000);

server.listen(process.env.PORT || 3000, () => console.log("Server started"));
