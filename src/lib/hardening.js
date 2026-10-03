const cluster = require("cluster");
const os = require("os");
const compression = require("compression");
const helmet = require("helmet");

function mongoConnectOptions() {
  return {
    maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE || 100),
    minPoolSize: Number(process.env.MONGO_MIN_POOL_SIZE || 10),
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    maxIdleTimeMS: 30000,
    retryWrites: true,
    w: "majority",
  };
}

function applySecurityMiddleware(app) {
  app.use(
    helmet({
      contentSecurityPolicy: false,
      crossOriginEmbedderPolicy: false,
    }),
  );
  app.use(compression({ threshold: 1024, level: 6 }));
  app.use((req, res, next) => {
    res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
    if (process.env.NODE_ENV === "production") {
      res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
    }
    next();
  });
}

function createSocketPacketLimiter(maxPerSecond = 50) {
  return (socket, next) => {
    let count = 0;
    let windowStart = Date.now();
    socket.use(([event], packetNext) => {
      const now = Date.now();
      if (now - windowStart >= 1000) {
        count = 0;
        windowStart = now;
      }
      if (count >= maxPerSecond) {
        return packetNext(new Error("Rate limit exceeded"));
      }
      count += 1;
      packetNext();
    });
    next();
  };
}

async function setupSocketRedisAdapter(io, redisUrl) {
  if (!redisUrl) return null;
  const { createAdapter } = require("@socket.io/redis-adapter");
  const { createClient } = require("redis");
  const pub = createClient({
    url: redisUrl,
    socket: { reconnectStrategy: (retries) => Math.min(retries * 100, 3000) },
  });
  const sub = pub.duplicate();
  pub.on("error", (e) => console.warn("Redis pub error:", e.message));
  sub.on("error", (e) => console.warn("Redis sub error:", e.message));
  await Promise.all([pub.connect(), sub.connect()]);
  io.adapter(createAdapter(pub, sub));
  console.log("Socket.IO Redis adapter enabled");
  return { pub, sub };
}

function createRequestGuards() {
  const maxUrl = Number(process.env.MAX_URL_LENGTH || 2048);
  return (req, res, next) => {
    const url = String(req.originalUrl || req.url || "");
    if (url.length > maxUrl) {
      return res.status(414).json({ error: "URI too long" });
    }
    if (req.body && typeof req.body === "object" && hasMongoOperatorKeys(req.body)) {
      return res.status(400).json({ error: "Invalid request payload" });
    }
    next();
  };
}

function hasMongoOperatorKeys(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 6) return false;
  for (const key of Object.keys(value)) {
    if (key.startsWith("$")) return true;
    if (hasMongoOperatorKeys(value[key], depth + 1)) return true;
  }
  return false;
}

function registerHealthRoutes(app, state) {
  app.get("/health", (_req, res) => {
    res.json({ ok: true, pid: process.pid, uptime: process.uptime() });
  });
  app.get("/ready", async (_req, res) => {
    if (state.isShuttingDown?.()) {
      return res.status(503).json({ ok: false, reason: "shutting_down" });
    }
    try {
      const mongo = state.mongoose?.connection?.readyState === 1;
      if (!mongo) {
        return res.status(503).json({ ok: false, reason: "mongo_not_ready" });
      }
      return res.json({ ok: true, mongo: true, redis: Boolean(state.redisOk?.()) });
    } catch {
      return res.status(503).json({ ok: false });
    }
  });
}

function registerGracefulShutdown(server, cleanup, io) {
  let shuttingDown = false;
  const isShuttingDown = () => shuttingDown;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`${signal}: graceful shutdown...`);
    try {
      io?.close?.();
    } catch (e) {
      console.warn("Socket close:", e.message);
    }
    server.close(async () => {
      try {
        await cleanup?.();
      } catch (e) {
        console.error("Cleanup error:", e.message);
      }
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 15000).unref();
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  return { isShuttingDown };
}

function registerGlobalErrorHandlers(app) {
  app.use((req, res) => {
    res.status(404).json({ error: "Not found" });
  });
  app.use((err, req, res, _next) => {
    if (err?.message?.includes("CORS") || err?.message?.includes("origin")) {
      return res.status(403).json({ error: "Origin not allowed" });
    }
    console.error("Unhandled error:", err?.message || err);
    res.status(err?.status || 500).json({
      error: process.env.NODE_ENV === "production" ? "Internal server error" : err?.message || "Error",
    });
  });
}

function runCluster(startWorker) {
  const workers = Number(process.env.CLUSTER_WORKERS || os.cpus().length);
  if (workers <= 1) {
    startWorker();
    return;
  }
  if (cluster.isPrimary) {
    console.log(`Cluster master ${process.pid}, forking ${workers} workers`);
    for (let i = 0; i < workers; i += 1) cluster.fork();
    cluster.on("exit", (worker) => {
      console.warn(`Worker ${worker.process.pid} died, restarting`);
      cluster.fork();
    });
  } else {
    startWorker();
  }
}

module.exports = {
  mongoConnectOptions,
  applySecurityMiddleware,
  createRequestGuards,
  registerHealthRoutes,
  createSocketPacketLimiter,
  setupSocketRedisAdapter,
  registerGracefulShutdown,
  registerGlobalErrorHandlers,
  runCluster,
};
