const TTL_MS = Number(process.env.AUTH_SESSION_CACHE_TTL_MS || 30000);
const MAX_ENTRIES = Number(process.env.AUTH_SESSION_CACHE_MAX || 50000);

const cache = new Map();

function cacheKey(userId, deviceId, tokenVersion) {
  return `${userId}:${deviceId}:${tokenVersion}`;
}

function pruneIfNeeded() {
  if (cache.size <= MAX_ENTRIES) return;
  const drop = Math.ceil(cache.size * 0.1);
  const keys = cache.keys();
  for (let i = 0; i < drop; i += 1) {
    const next = keys.next();
    if (next.done) break;
    cache.delete(next.value);
  }
}

function get(userId, deviceId, tokenVersion) {
  const key = cacheKey(userId, deviceId, tokenVersion);
  const hit = cache.get(key);
  if (!hit) return null;
  if (hit.exp <= Date.now()) {
    cache.delete(key);
    return null;
  }
  return hit;
}

function set(userId, deviceId, tokenVersion, user, device) {
  pruneIfNeeded();
  cache.set(cacheKey(userId, deviceId, tokenVersion), {
    user,
    device,
    exp: Date.now() + TTL_MS,
  });
}

function invalidateUser(userId) {
  for (const key of cache.keys()) {
    if (key.startsWith(`${userId}:`)) cache.delete(key);
  }
}

function invalidateDevice(userId, deviceId) {
  for (const key of cache.keys()) {
    if (key.startsWith(`${userId}:${deviceId}:`)) cache.delete(key);
  }
}

function clear() {
  cache.clear();
}

module.exports = {
  get,
  set,
  invalidateUser,
  invalidateDevice,
  clear,
};
