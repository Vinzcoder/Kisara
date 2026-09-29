import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import pg from "pg";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: true, max: 5 });
const app = express();
const http = createServer(app);
const io = new Server(http, { cors: { origin: process.env.CORS_ORIGIN || "*" } });

await pool.query(`
  create table if not exists posts(
    id bigserial primary key,
    author text not null,
    body text not null check (char_length(body) between 1 and 280),
    created_at timestamptz not null default now());
  create table if not exists likes(
    post_id bigint references posts(id) on delete cascade,
    client_id text not null,
    primary key (post_id, client_id));`);

const SELECT = `select p.id::text, p.author, p.body, (extract(epoch from p.created_at)*1000)::bigint as ts,
  (select count(*)::int from likes l where l.post_id = p.id) as likes from posts p`;

app.use(express.static("public"));
app.get("/health", (_, res) => res.send("ok"));

io.on("connection", async (s) => {
  const cid = String(s.handshake.auth?.cid || "").slice(0, 64);
  io.emit("presence", { online: io.engine.clientsCount });
  try {
    const { rows } = await pool.query(`${SELECT} order by p.id desc limit 50`);
    const mine = rows.length
      ? (await pool.query("select post_id::text from likes where client_id=$1 and post_id = any($2::bigint[])",
          [cid, rows.map((r) => r.id)])).rows.map((r) => r.post_id)
      : [];
    s.emit("init", { posts: rows, mine });
  } catch (e) { console.error(e); }

  s.on("post", async (d, ack) => {
    const body = String(d?.body || "").trim().slice(0, 280);
    const author = String(d?.author || "").trim().slice(0, 24) || "anon";
    const now = Date.now();
    if (!body) return ack?.({ ok: false, error: "Tulisan kosong." });
    if (now - (s.data.last || 0) < 2000) return ack?.({ ok: false, error: "Terlalu cepat, tunggu sebentar." });
    s.data.last = now;
    try {
      const { rows } = await pool.query(
        `with ins as (insert into posts(author, body) values ($1,$2) returning *)
         select id::text, author, body, (extract(epoch from created_at)*1000)::bigint as ts, 0 as likes from ins`,
        [author, body]);
      io.emit("post:new", rows[0]);
      ack?.({ ok: true });
    } catch (e) { console.error(e); ack?.({ ok: false, error: "Gagal menyimpan." }); }
  });

  s.on("like", async ({ id, on } = {}) => {
    if (!/^\d+$/.test(String(id)) || !cid) return;
    try {
      await pool.query(on
        ? "insert into likes(post_id, client_id) values ($1,$2) on conflict do nothing"
        : "delete from likes where post_id=$1 and client_id=$2", [id, cid]);
      const { rows } = await pool.query("select count(*)::int as n from likes where post_id=$1", [id]);
      io.emit("likes", { id: String(id), likes: rows[0].n });
    } catch (e) { console.error(e); }
  });

  s.on("disconnect", () => io.emit("presence", { online: io.engine.clientsCount }));
});

http.listen(process.env.PORT || 3000, () => console.log("KISAR jalan di port", process.env.PORT || 3000));
