
const POOLS = {
  service: [
    { id: "svc1", url: "https://1serviceddma.sergiirudniev.com" },
    { id: "svc2", url: "https://2serviceddma.sergiirudniev.com" },
  ],
  admin: [
    { id: "adm1", url: "https://1adminddma.sergiirudniev.com" },
    { id: "adm2", url: "https://2adminddma.sergiirudniev.com" },
  ],
};

function ipHashPick(pool, ip) {
  if (!pool.length) return null;
  if (!ip) return pool[Math.floor(Math.random() * pool.length)];
  let h = 0;
  for (let i = 0; i < ip.length; i++) h = (h * 31 + ip.charCodeAt(i)) >>> 0;
  return pool[h % pool.length];
}

export default {
  async fetch(req, env) {
    const host = req.headers.get("host") || "";
    const isService = host.startsWith("algopraktrob.");
    const isAdmin   = host.startsWith("algopraktrobadmin.");
    if (!isService && !isAdmin) return new Response("Not routed", { status: 404 });

    const poolName  = isService ? "service" : "admin";
    const cookieKey = isService ? "lb_svc"  : "lb_adm";
    const backends  = POOLS[poolName];
    
    const kvKey  = `health:${poolName}`;
    const health = JSON.parse((await env.KV.get(kvKey)) || "{}");
    let healthyPool = backends.filter(b => health[b.id] !== false);
    if (!healthyPool.length) healthyPool = backends;

    const cookie = req.headers.get("cookie") || "";
    const m = new RegExp(`${cookieKey}=([^;]+)`).exec(cookie);
    let target = m ? healthyPool.find(b => b.id === m[1]) : null;

    if (!target) {
      const ip = req.headers.get("cf-connecting-ip") || "";
      target = ipHashPick(healthyPool, ip);
    }

    const upstream = new URL(target.url);
    const url = new URL(req.url);
    url.protocol = upstream.protocol;
    url.hostname = upstream.hostname;
    url.port     = upstream.port;

    const ac = new AbortController();
    const t = setTimeout(() => ac.abort("upstream timeout"), 10_000);

    let resp;
    try {
      const init = new Request(url.toString(), req);
      resp = await fetch(init, { cf: { cacheTtl: 0, cacheEverything: false }, signal: ac.signal });
    } catch {
      clearTimeout(t);
      const fallback = healthyPool.find(b => b.id !== target.id);
      if (!fallback) return new Response("Upstream unavailable", { status: 502 });
      const fUrl = new URL(req.url); const fUp = new URL(fallback.url);
      fUrl.protocol = fUp.protocol; fUrl.hostname = fUp.hostname; fUrl.port = fUp.port;
      resp = await fetch(new Request(fUrl.toString(), req), { cf: { cacheTtl: 0, cacheEverything: false } });
    } finally {
      clearTimeout(t);
    }

    const ttl = Number(env.COOKIE_TTL_SECONDS || "600");
    const out = new Response(resp.body, resp);
    out.headers.append("Set-Cookie", `${cookieKey}=${target.id}; Path=/; Max-Age=${ttl}; SameSite=Lax`);
    out.headers.set("x-lb-backend", target.id);
    return out;
  },

  async scheduled(_event, env) {
    for (const poolName of ["service", "admin"]) {
      const list = POOLS[poolName];
      const results = {};
      await Promise.all(list.map(async (b) => {
        try {
          const r = await fetch(b.url, { method: "GET" });
          results[b.id] = r.ok;
        } catch {
          results[b.id] = false;
        }
      }));
      await env.KV.put(`health:${poolName}`, JSON.stringify(results));
    }
  }
};
