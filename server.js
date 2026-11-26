// Signaling-сервер: только знакомит игроков и пересылает SDP/ICE. Игровые данные идут напрямую через WebRTC.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;

const ICE = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }];
if (process.env.TURN_URL) ICE.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER, credential: process.env.TURN_PASS });

const srv = http.createServer((q, r) => {
  if (q.url.split('?')[0] === '/healthz') return r.end('ok');
  r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  fs.createReadStream(path.join(__dirname, 'index.html')).pipe(r);
});

const rooms = new Map(); let n = 0;
const newCode = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.random() * 24 | 0]).join(''); } while (rooms.has(c)); return c; };
const send = (s, o) => s.readyState === 1 && s.send(JSON.stringify(o));

const wss = new WebSocketServer({ server: srv });
wss.on('connection', ws => {
  ws.id = 'p' + (++n); ws.room = null;
  ws.alive = true; ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'create') {
      const c = newCode(); rooms.set(c, { host: ws.id, m: new Map([[ws.id, ws]]) }); ws.room = c;
      send(ws, { t: 'created', code: c, id: ws.id, ice: ICE });
    } else if (m.t === 'join') {
      const c = String(m.code || '').toUpperCase().trim(), r = rooms.get(c);
      if (!r) return send(ws, { t: 'err', m: 'Комната не найдена' });
      if (r.m.size >= 10) return send(ws, { t: 'err', m: 'Комната заполнена' });
      const peers = [...r.m.keys()]; r.m.set(ws.id, ws); ws.room = c;
      send(ws, { t: 'joined', id: ws.id, host: r.host, peers, ice: ICE });
    } else if (m.t === 'sig') {
      const r = rooms.get(ws.room), to = r && r.m.get(m.to);
      if (to) send(to, { t: 'sig', from: ws.id, d: m.d });
    }
  });
  ws.on('close', () => {
    const r = rooms.get(ws.room); if (!r) return;
    r.m.delete(ws.id);
    if (!r.m.size) return void rooms.delete(ws.room);
    if (r.host === ws.id) r.host = r.m.keys().next().value;
    r.m.forEach(s => send(s, { t: 'left', id: ws.id, host: r.host }));
  });
});
setInterval(() => wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); }), 25000);
srv.listen(PORT, () => console.log('Frontline Strike: http://localhost:' + PORT));
