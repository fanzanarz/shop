import { copyFile, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { createHash } from "node:crypto";

const AUTO_FRONTEND = (process.env.AUTO_FRONTEND || "1").trim().toLowerCase();
if (["0", "false", "off"].includes(AUTO_FRONTEND)) process.exit(0);

const TEMPLATE_ROOT = join("templates", "landing");
const publicPath = normalizePath(process.env.PUBLIC_RELAY_PATH || "/api");
const relayPath = normalizePath(process.env.RELAY_PATH || "/api");

const hash = createHash("sha256").update(Date.now().toString()).digest("hex");
const tokens = { "{{BUILD_CODE}}": hash.slice(0, 8).toUpperCase(), "{{PUBLIC_RELAY_PATH}}": publicPath, "{{RELAY_PATH}}": relayPath, "{{GENERATED_AT}}": new Date().toISOString() };

await rm("public", { recursive: true, force: true });
await mkdir("public", { recursive: true });

const templates = await listTemplates(TEMPLATE_ROOT);
if (templates.length > 0) {
  const selected = templates[Math.floor(Math.random() * templates.length)];
  await copyDir(join(TEMPLATE_ROOT, selected), "public");
  await replaceTokens("public", tokens);
} else {
  await writeFile("public/index.html", "<h1>System Online</h1>");
}

async function listTemplates(root) {
  try {
    const entries = await readdir(root, { withFileTypes: true });
    return entries.filter(e => e.isDirectory()).map(e => e.name);
  } catch { return []; }
}

async function copyDir(from, to) {
  await mkdir(to, { recursive: true });
  for (const entry of await readdir(from, { withFileTypes: true })) {
    const s = join(from, entry.name), d = join(to, entry.name);
    entry.isDirectory() ? await copyDir(s, d) : await copyFile(s, d);
  }
}

async function replaceTokens(root, map) {
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const target = join(root, entry.name);
    if (entry.isDirectory()) { await replaceTokens(target, map); continue; }
    if (![".html", ".css", ".js"].includes(extname(entry.name))) continue;
    let content = await readFile(target, "utf8");
    for (const [k, v] of Object.entries(map)) content = content.split(k).join(v);
    await writeFile(target, content, "utf8");
  }
}

function normalizePath(r) { const i = String(r || "").trim(); if (!i) return "/api"; const b = i.startsWith("/") ? i : `/${i}`; return b.length > 1 && b.endsWith("/") ? b.slice(0, -1) : b; }