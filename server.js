/* Le Grand Débat — serveur de salles (Node.js + WebSocket)
   Il sert la page du jeu et synchronise les téléphones d’une même salle.
   Aucun compte, aucune base de données : tout est en mémoire. */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const MIN = parseInt(process.env.MIN_PLAYERS || "4", 10);
const MAX = 8;
// Cherche la page du jeu dans public/index.html, ou à défaut à côté de server.js
const PAGE = [path.join(__dirname, "public", "index.html"), path.join(__dirname, "index.html")].find(f => fs.existsSync(f));
if (!PAGE) { console.error("index.html est introuvable : ajoute-le à côté de server.js."); process.exit(1); }
const INDEX = fs.readFileSync(PAGE);

const server = http.createServer((req, res) => {
  const u = (req.url || "/").split("?")[0];
  if (u === "/healthz") { res.writeHead(200, { "Content-Type": "text/plain" }); res.end("ok"); return; }
  if (u === "/" || u === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    res.end(INDEX); return;
  }
  res.writeHead(404, { "Content-Type": "text/plain" }); res.end("Introuvable");
});
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 32 * 1024 });

const rooms = new Map();
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ";
const rid = n => Array.from({ length: n }, () => LETTERS[Math.floor(Math.random() * LETTERS.length)]).join("");
const tok = () => Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
const cleanName = s => String(s == null ? "" : s).replace(/[<>&"]/g, "").replace(/\s+/g, " ").trim().slice(0, 14);
const MINUTES = [5, 10, 15];

function newCode() { let c; do { c = rid(4); } while (rooms.has(c)); return c; }

function publicState(room) {
  return {
    code: room.code, host: room.hostId, phase: room.phase, min: MIN, max: MAX,
    settings: room.settings,
    players: room.players.map(p => ({ id: p.id, name: p.name, cand: p.cand, on: p.on })),
    taken: room.taken, round: room.round, theme: room.theme, used: room.used,
    deb: room.deb,
    voted: Object.keys(room.votes), results: room.results, totals: room.totals,
    now: Date.now()
  };
}
function send(ws, m) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); }
function broadcast(room) {
  room.last = Date.now();
  const m = { t: "state", s: publicState(room) };
  room.players.forEach(p => send(p.ws, m));
}
function err(ws, msg, fatal) { send(ws, { t: "error", msg, fatal: !!fatal }); }
function playerOf(room, pid) { return room.players.find(p => p.id === pid); }

function newRoom(hostName, ws) {
  const room = {
    code: newCode(), hostId: null, phase: "lobby", settings: { rounds: 1, minutes: 10, mode: "random", themes: [] },
    players: [], taken: [], round: 0, theme: null, used: [], deb: null,
    votes: {}, results: [], totals: {}, last: Date.now(), hostTimer: null
  };
  rooms.set(room.code, room);
  const p = addPlayer(room, hostName, ws);
  room.hostId = p.id;
  return { room, p };
}
function addPlayer(room, name, ws) {
  const p = { id: "p" + tok().slice(0, 6), token: tok(), name, cand: null, on: true, ws };
  room.players.push(p); room.totals[p.id] = 0;
  return p;
}
function uniqueName(room, name) {
  let n = name, k = 2;
  while (room.players.some(p => p.name.toLowerCase() === n.toLowerCase())) n = name.slice(0, 11) + " " + (k++);
  return n;
}
function pickNewHost(room) {
  const h = room.players.find(p => p.on);
  if (h) room.hostId = h.id;
}
function allVoted(room) {
  const live = room.players.filter(p => p.on);
  return live.length > 0 && live.every(p => room.votes[p.id]);
}
function finishVote(room) {
  const counts = {};
  room.players.forEach(p => counts[p.id] = 0);
  Object.values(room.votes).forEach(t => { if (counts[t] != null) counts[t]++; });
  room.results.push({ counts });
  Object.keys(counts).forEach(id => room.totals[id] = (room.totals[id] || 0) + counts[id]);
  room.phase = "results";
}
function resetGame(room) {
  room.players.forEach(p => { p.cand = null; room.totals[p.id] = 0; });
  room.taken = []; room.round = 0; room.theme = null; room.used = []; room.deb = null;
  room.votes = {}; room.results = [];
  room.phase = "duels";
}
/* Temps de parole : chaque joueur a une « banque » de secondes, comme à la télé */
function newDeb(room) {
  const sec = room.settings.minutes * 60, banks = {};
  room.players.forEach(p => banks[p.id] = sec);
  return { banks, total: sec, floor: null, running: false, at: Date.now(), prompt: null };
}
function debLeft(D, id, now) { let b = D.banks[id] || 0; if (D.running && D.floor === id) b -= (now - D.at) / 1000; return Math.max(0, b); }
function debSettle(D, now) { if (D.running && D.floor != null) D.banks[D.floor] = debLeft(D, D.floor, now); D.at = now; }
function debSpeak(D, id, now) { debSettle(D, now); D.floor = id; D.running = (D.banks[id] || 0) > 0; D.at = now; }
function debPause(D, now) { debSettle(D, now); D.running = false; }
function debResume(D, now) { debSettle(D, now); D.running = D.floor != null && (D.banks[D.floor] || 0) > 0; D.at = now; }

const HANDLERS = {
  create(ws, m) {
    if (rooms.size > 500) return err(ws, "Le serveur est complet, réessaie dans quelques minutes.");
    const name = cleanName(m.name) || "Hôte";
    const { room, p } = newRoom(name, ws);
    ws.ctx = { code: room.code, pid: p.id };
    send(ws, { t: "joined", code: room.code, pid: p.id, token: p.token });
    broadcast(room);
  },
  join(ws, m) {
    const code = String(m.code || "").toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return err(ws, "Cette salle n’existe pas. Vérifie le code.");
    if (room.phase !== "lobby") return err(ws, "La partie a déjà commencé dans cette salle.");
    if (room.players.length >= MAX) return err(ws, "La salle est pleine (8 joueurs maximum).");
    const name = uniqueName(room, cleanName(m.name) || "Joueur");
    const p = addPlayer(room, name, ws);
    ws.ctx = { code: room.code, pid: p.id };
    send(ws, { t: "joined", code: room.code, pid: p.id, token: p.token });
    broadcast(room);
  },
  rejoin(ws, m) {
    const room = rooms.get(String(m.code || "").toUpperCase());
    const p = room && playerOf(room, m.pid);
    if (!room || !p || p.token !== m.token) return err(ws, "Session expirée.", true);
    if (p.ws && p.ws !== ws) { try { p.ws.ctx = null; p.ws.close(); } catch (e) { } }
    p.ws = ws; p.on = true;
    ws.ctx = { code: room.code, pid: p.id };
    if (room.hostTimer && room.hostId === p.id) { clearTimeout(room.hostTimer); room.hostTimer = null; }
    send(ws, { t: "joined", code: room.code, pid: p.id, token: p.token });
    broadcast(room);
  },
  ping(ws) { send(ws, { t: "pong", now: Date.now() }); }
};

// Actions qui exigent d’être dans une salle (et parfois d’être l’hôte)
const ACTIONS = {
  settings(room, p, m) {
    if (room.phase !== "lobby") return;
    const st = room.settings;
    const r = parseInt(m.rounds, 10); if (r >= 1 && r <= 3) st.rounds = r;
    const mn = parseInt(m.minutes, 10); if (MINUTES.includes(mn)) st.minutes = mn;
    if (m.mode === "random" || m.mode === "theme") st.mode = m.mode;
    if (Array.isArray(m.themes)) {
      const seen = new Set();
      st.themes = m.themes.map(x => String(x).slice(0, 8)).filter(x => x && !seen.has(x) && seen.add(x)).slice(0, 3);
    }
    if (st.mode === "theme") st.rounds = Math.max(1, st.themes.length);
  },
  start(room, p) {
    if (room.phase !== "lobby") return;
    if (room.players.length < MIN) return;
    if (room.settings.mode === "theme" && !room.settings.themes.length) return;
    resetGame(room);
  },
  pick(room, p, m, ws) {
    if (room.phase !== "duels") return;
    const c = String(m.cand || "").slice(0, 24);
    if (!c) return;
    if (room.taken.includes(c) && p.cand !== c) return send(ws, { t: "conflict", taken: room.taken });
    if (p.cand) room.taken = room.taken.filter(x => x !== p.cand);
    p.cand = c; room.taken.push(c);
  },
  startRound(room, p, m, ws) {
    if (room.phase === "duels") {
      room.players = room.players.filter(q => q.cand);
      if (room.players.length < 2) return;
      room.round = 0; room.used = [];
    } else if (room.phase === "results") {
      if (room.round >= room.settings.rounds - 1) return;
      room.round += 1;
    } else return;
    room.theme = String(m.theme || "").slice(0, 8); room.used.push(room.theme);
    room.votes = {};
    room.deb = newDeb(room);
    room.phase = "theme";
  },
  beginDebate(room) { if (room.phase === "theme") room.phase = "debate"; },
  speak(room, p) { if (room.phase === "debate" && room.deb) debSpeak(room.deb, p.id, Date.now()); },
  give(room, p, m) {
    if (room.phase !== "debate" || !room.deb) return;
    if (playerOf(room, m.pid)) debSpeak(room.deb, m.pid, Date.now());
  },
  pause(room, p) {
    if (room.phase !== "debate" || !room.deb) return;
    if (room.hostId === p.id || room.deb.floor === p.id) debPause(room.deb, Date.now());
  },
  resume(room, p) {
    if (room.phase !== "debate" || !room.deb) return;
    if (room.hostId === p.id || room.deb.floor === p.id) debResume(room.deb, Date.now());
  },
  relance(room, p, m) {
    if (room.phase !== "debate" || !room.deb) return;
    const i = Math.max(0, Math.min(50, parseInt(m.i, 10) || 0));
    room.deb.prompt = { i, n: ((room.deb.prompt && room.deb.prompt.n) || 0) + 1 };
  },
  toVote(room) {
    if (room.phase !== "debate") return;
    if (room.deb) debPause(room.deb, Date.now());
    room.phase = "vote"; room.votes = {};
  },
  vote(room, p, m) {
    if (room.phase !== "vote") return;
    const tgt = playerOf(room, m.target);
    if (!tgt || tgt.id === p.id) return;
    room.votes[p.id] = tgt.id;
    if (allVoted(room)) finishVote(room);
  },
  finishVote(room) { if (room.phase === "vote" && Object.keys(room.votes).length >= 1) finishVote(room); },
  final(room) { if (room.phase === "results") room.phase = "final"; },
  rematch(room) { if (room.phase === "results" || room.phase === "final") resetGame(room); },
  leave(room, p, m, ws) {
    if (room.phase === "lobby") room.players = room.players.filter(q => q.id !== p.id);
    else p.on = false;
    p.ws = null; ws.ctx = null;
    if (room.hostId === p.id) pickNewHost(room);
    if (room.phase === "vote" && allVoted(room)) finishVote(room);
  }
};
const HOST_ONLY = new Set(["settings", "start", "startRound", "beginDebate", "give", "relance", "toVote", "finishVote", "final", "rematch"]);

wss.on("connection", ws => {
  ws.isAlive = true; ws.ctx = null; ws.budget = 40; ws.lastRefill = Date.now();
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", raw => {
    const now = Date.now();
    ws.budget = Math.min(40, ws.budget + (now - ws.lastRefill) / 100); ws.lastRefill = now;
    if (--ws.budget < 0) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== "string") return;
    ws.isAlive = true;
    if (HANDLERS[m.t]) return HANDLERS[m.t](ws, m);
    if (!ws.ctx) return;
    const room = rooms.get(ws.ctx.code); if (!room) return;
    const p = playerOf(room, ws.ctx.pid); if (!p) return;
    const act = ACTIONS[m.t]; if (!act) return;
    if (HOST_ONLY.has(m.t) && room.hostId !== p.id) return;
    act(room, p, m, ws);
    broadcast(room);
  });
  ws.on("close", () => {
    const c = ws.ctx; if (!c) return;
    const room = rooms.get(c.code); if (!room) return;
    const p = playerOf(room, c.pid);
    if (!p || p.ws !== ws) return;
    p.on = false; p.ws = null;
    if (room.hostId === p.id && !room.hostTimer) {
      room.hostTimer = setTimeout(() => {
        room.hostTimer = null;
        const h = playerOf(room, room.hostId);
        if (h && !h.on) { pickNewHost(room); broadcast(room); }
      }, 20000);
    }
    if (room.phase === "vote" && allVoted(room)) finishVote(room);
    broadcast(room);
  });
  ws.on("error", () => { });
});

// Garde les connexions vivantes (les hébergeurs coupent les connexions silencieuses)
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { try { ws.terminate(); } catch (e) { } return; }
    ws.isAlive = false; try { ws.ping(); } catch (e) { }
  });
}, 25000);
// Nettoie les salles abandonnées
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room, code) => {
    const alive = room.players.some(p => p.on);
    if ((!alive && now - room.last > 30 * 60 * 1000) || now - room.last > 5 * 3600 * 1000) rooms.delete(code);
  });
}, 10 * 60 * 1000);

server.listen(PORT, () => console.log("Le Grand Débat écoute sur le port " + PORT + " (joueurs min : " + MIN + ")"));
