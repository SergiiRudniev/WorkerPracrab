const POOLS = {
  service: [
    { id: "svc1", url: "https://1serviceddma.sergiirudniev.com" },
    { id: "svc2", url: "https://2serviceddma.sergiirudniev.com" },
  ],
  admin: [
    { id: "adm1", url: "https://1adminddma.sergiirudniev.com" },
    { id: "adm2", url: "https://2adminddma.sergiirudnieв.com" },
  ],
};

function ema(prev, cur, alpha = 0.3) {
  return prev == null ? cur : prev * (1 - alpha) + cur * alpha;
}

function ipHashPick(pool, ip) {
  if (!pool.length) return null;
  if (!ip) return pool[Math.floor(Math.random() * pool.length)];
  let h = 0;
  for (let i = 0; i < ip.length; i++) h = (h * 31 + ip.charCodeAt(i)) >>> 0;
  return pool[h % pool.length];
}

async function chooseTarget(req, env, poolName, healthyPool, cookieKey) {
  const stickyEnabled = (env.STICKY_ENABLED || "1") === "1";
  if (stickyEnabled) {
    const cookie = req.headers.get("cookie") || "";
    const stickyId = new RegExp(`${cookieKey}=([^;]+)`).exec(cookie)?.[1];
    const stickyTarget = stickyId ? healthyPool.find(b => b.id === stickyId) : null;
    if (stickyTarget) return stickyTarget;
  }

  const colo = (req.cf && req.cf.colo) || "ZZZ";
  const rttKey = `rtt:${poolName}:${colo}`;
  const rttMap = JSON.parse((await env.KV.get(rttKey)) || "{}");
  if (Object.keys(rttMap).length) {
    const sorted = healthyPool.slice().sort((a, b) => (rttMap[a.id] ?? 1e9) - (rttMap[b.id] ?? 1e9));
    return sorted[0] || healthyPool[0];
  }

  const ip = req.headers.get("cf-connecting-ip") || "";
  return ipHashPick(healthyPool, ip) || healthyPool[0];
}

async function readBodyOnce(req) {
  const m = req.method.toUpperCase();
  if (m === "GET" || m === "HEAD") return null;
  const ab = await req.arrayBuffer();
  return ab;
}

function buildUpstreamRequestFromBuffer(req, targetUrl, bodyBuf) {
  const url = new URL(req.url);
  const upstream = new URL(targetUrl);
  url.protocol = upstream.protocol;
  url.hostname = upstream.hostname;
  url.port     = upstream.port;

  const headers = new Headers(req.headers);
  headers.set("X-Forwarded-Proto", req.headers.get("X-Forwarded-Proto") || (req.url.startsWith("https:") ? "https" : "http"));
  headers.set("X-Forwarded-Host", req.headers.get("host") || "");
  headers.set("X-Real-IP", req.headers.get("cf-connecting-ip") || "");

  const init = {
    method: req.method,
    headers,
    body: bodyBuf ? bodyBuf.slice(0) : null,
    redirect: "manual",
    cf: { cacheTtl: 0, cacheEverything: false },
  };
  return new Request(url.toString(), init);
}

async function proxyOnce(req, env, poolName, target, bodyBuf, measureRtt = true) {
  const colo = (req.cf && req.cf.colo) || "ZZZ";
  const rttKey = `rtt:${poolName}:${colo}`;

  const initReq = buildUpstreamRequestFromBuffer(req, target.url, bodyBuf);

  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort("upstream timeout"), 10_000);

  let resp, dt = null;
  const t0 = measureRtt ? Date.now() : null;

  try {
    resp = await fetch(initReq, { signal: ac.signal });
  } finally {
    clearTimeout(timer);
    if (measureRtt) {
      dt = Date.now() - t0;
      const rttMap = JSON.parse((await env.KV.get(rttKey)) || "{}");
      rttMap[target.id] = ema(rttMap[target.id], dt);
      await env.KV.put(rttKey, JSON.stringify(rttMap), { expirationTtl: 3600 });
    }
  }
  return resp;
}

export default {
  async fetch(req, env) {
    const urlIn = new URL(req.url);
    const host = req.headers.get("host") || urlIn.host;

    if (urlIn.pathname === "/__lb/health") {
      const colo = (req.cf && req.cf.colo) || "ZZZ";
      const svc = JSON.parse((await env.KV.get("health:service")) || "{}");
      const adm = JSON.parse((await env.KV.get("health:admin")) || "{}");
      const rttSvc = JSON.parse((await env.KV.get(`rtt:service:${colo}`)) || "{}");
      const rttAdm = JSON.parse((await env.KV.get(`rtt:admin:${colo}`)) || "{}");
      return new Response(JSON.stringify({ host, colo, svc, adm, rttSvc, rttAdm }), {
        status: 200, headers: { "content-type": "application/json" }
      });
    }

    const isService = host.startsWith("algopraktrob.");
    const isAdmin   = host.startsWith("algopraktrobadmin.");
    if (!isService && !isAdmin) return new Response("Not routed", { status: 404 });

    const poolName  = isService ? "service" : "admin";
    const cookieKey = isService ? "lb_svc"  : "lb_adm";
    const backends  = POOLS[poolName];

    const health = JSON.parse((await env.KV.get(`health:${poolName}`)) || "{}");
    let healthyPool = backends.filter(b => health[b.id] !== false);
    if (!healthyPool.length) healthyPool = backends;

    let target = await chooseTarget(req, env, poolName, healthyPool, cookieKey);

    const bodyBuf = await readBodyOnce(req);

    let resp = await proxyOnce(req, env, poolName, target, bodyBuf, true);

    if (!resp || resp.status >= 502 || resp.status === 0) {
      const colo = (req.cf && req.cf.colo) || "ZZZ";
      const rttMap = JSON.parse((await env.KV.get(`rtt:${poolName}:${colo}`)) || "{}");
      const ordered = healthyPool
        .slice()
        .sort((a, b) => (rttMap[a.id] ?? 1e9) - (rttMap[b.id] ?? 1e9))
        .filter(b => b.id !== target.id);
      const fallback = ordered[0];
      if (fallback) {
        resp = await proxyOnce(req, env, poolName, fallback, bodyBuf, true);
        if (resp) target = fallback;
      }
    }
    if (!resp) return new Response("Upstream unavailable", { status: 502 });

    const out = new Response(resp.body, resp); 
    out.headers.set("x-lb-backend", target.id);

    if ((env.STICKY_ENABLED || "1") === "1") {
      const ttl = Number(env.COOKIE_TTL_SECONDS || "600");
      out.headers.append("Set-Cookie", `${cookieKey}=${target.id}; Path=/; Max-Age=${ttl}; SameSite=Lax`);
    }
    return out;
  },

  async scheduled(_event, env) {
    const pools = [
      { name: "service", list: POOLS.service, path: "/" },
      { name: "admin",   list: POOLS.admin,   path: "/admin" },
    ];

    for (const { name, list, path } of pools) {
      const results = {};
      await Promise.all(list.map(async (b) => {
        try {
          const u = new URL(b.url);
          u.pathname = path;
          const r = await fetch(u.toString(), {
            method: "GET",
            redirect: "follow",
            headers: { "User-Agent": "EdgeLB/1.0", "Accept": "*/*" },
          });
          results[b.id] = r.status < 400;
        } catch {
          results[b.id] = false;
        }
      }));
      await env.KV.put(`health:${name}`, JSON.stringify(results));
    }
  }
};
