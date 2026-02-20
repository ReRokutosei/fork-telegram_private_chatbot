var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/do/rate-limit-do.js
import { DurableObject } from "cloudflare:workers";
var RateLimitDO = class extends DurableObject {
  static {
    __name(this, "RateLimitDO");
  }
  constructor(ctx, env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
                CREATE TABLE IF NOT EXISTS rate_limits (
                    key TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                /* expires_at \u7D22\u5F15\u7528\u4E8E\u52A0\u901F\u6309\u8FC7\u671F\u65F6\u95F4\u7684\u6E05\u7406\u4E0E\u7EDF\u8BA1\u67E5\u8BE2 */
                CREATE INDEX IF NOT EXISTS idx_expires_at
                ON rate_limits(expires_at);
            `);
    });
    this.cache = /* @__PURE__ */ new Map();
  }
  /**
   * RPC：检查并更新限流计数。
   *
   * 行为定义：
   * - 若当前窗口期内计数未达 limit，则允许请求并将计数 +1。
   * - 若计数已达 limit，则拒绝请求，不再递增计数。
   * - 若记录不存在或已过期，则创建/重置窗口期并将计数置为 1。
   *
   * 注意：
   * - 本实现依赖 Durable Object 的串行执行模型来避免同一对象实例内的并发竞态。
   * - SQLite 写入通过 exec 提交，写操作在 DO 存储层排队执行。
   */
  async check(key, limit, window) {
    if (!key || !limit || !window) {
      throw new Error("Missing parameters: key, limit, window");
    }
    const now = Date.now();
    const expiresAt = now + window * 1e3;
    const cached = this.cache.get(key);
    if (cached && cached.expiresAt > now) {
      if (cached.count >= limit) {
        return { allowed: false, remaining: 0 };
      }
      this.ctx.storage.sql.exec(
        `INSERT INTO rate_limits (key, count, expires_at, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET
                 count = count + 1,
                 updated_at = ?`,
        key,
        cached.count + 1,
        expiresAt,
        now,
        now,
        now
      );
      cached.count++;
      this.cache.set(key, { count: cached.count, expiresAt });
      return { allowed: true, remaining: limit - cached.count };
    }
    const result = this.ctx.storage.sql.exec(
      `SELECT count, expires_at FROM rate_limits WHERE key = ?`,
      key
    ).one();
    if (result && result.expires_at > now) {
      if (result.count >= limit) {
        this.cache.set(key, {
          count: result.count,
          expiresAt: result.expires_at
        });
        return { allowed: false, remaining: 0 };
      }
      this.ctx.storage.sql.exec(
        `UPDATE rate_limits SET count = count + 1, updated_at = ? WHERE key = ?`,
        now,
        key
      );
      const newCount = result.count + 1;
      this.cache.set(key, { count: newCount, expiresAt: result.expires_at });
      return { allowed: true, remaining: limit - newCount };
    }
    this.ctx.storage.sql.exec(
      `INSERT INTO rate_limits (key, count, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
             count = 1,
             expires_at = ?,
             updated_at = ?`,
      key,
      1,
      expiresAt,
      now,
      now,
      expiresAt,
      now
    );
    this.cache.set(key, { count: 1, expiresAt });
    return { allowed: true, remaining: limit - 1 };
  }
  /**
   * 清理过期限流记录（建议由 alarm 或外部调度周期触发）。
   *
   * 行为：
   * - 删除 SQLite 中 expires_at < now 的记录。
   * - 同步删除缓存中过期项，避免缓存膨胀。
   *
   * 返回：
   * - deleted：本次删除的记录数量（以 SQLite meta.changes 为准）。
   */
  async cleanupExpired() {
    const now = Date.now();
    const result = this.ctx.storage.sql.exec(
      `DELETE FROM rate_limits WHERE expires_at < ?`,
      now
    );
    for (const [key, value] of this.cache.entries()) {
      if (value.expiresAt < now) {
        this.cache.delete(key);
      }
    }
    return { deleted: result.meta.changes };
  }
  /**
   * 获取服务统计信息。
   *
   * 字段说明：
   * - totalRecords：SQLite 表内记录总数（含已过期但未清理的记录）。
   * - activeRecords：未过期记录数（expires_at > now）。
   * - cachedItems：当前内存缓存项数量。
   */
  async getStats() {
    const total = this.ctx.storage.sql.exec(
      `SELECT COUNT(*) as count FROM rate_limits`
    ).one();
    const active = this.ctx.storage.sql.exec(
      `SELECT COUNT(*) as count FROM rate_limits WHERE expires_at > ?`,
      Date.now()
    ).one();
    return {
      totalRecords: total?.count || 0,
      activeRecords: active?.count || 0,
      cachedItems: this.cache.size
    };
  }
  /**
   * 重置指定 key 的限流状态（管理用途）。
   *
   * 行为：
   * - 删除 SQLite 中对应记录。
   * - 删除缓存中的对应项。
   */
  async reset(key) {
    this.ctx.storage.sql.exec(`DELETE FROM rate_limits WHERE key = ?`, key);
    this.cache.delete(key);
    return { success: true };
  }
};

// src/config/constants.js
var CONFIG = {
  VERIFY_ID_LENGTH: 12,
  VERIFY_EXPIRE_SECONDS: 300,
  VERIFIED_EXPIRE_SECONDS: 2592e3,
  MEDIA_GROUP_EXPIRE_SECONDS: 60,
  MEDIA_GROUP_DELAY_MS: 3e3,
  PENDING_MAX_MESSAGES: 10,
  ADMIN_CACHE_TTL_SECONDS: 300,
  NEEDS_REVERIFY_TTL_SECONDS: 600,
  THREAD_HEALTH_TTL_MS: 6e4,
  MESSAGE_MAP_TTL_SECONDS: 86400,
  RATE_LIMIT_MESSAGE: 45,
  RATE_LIMIT_VERIFY: 3,
  RATE_LIMIT_WINDOW: 60,
  BUTTON_COLUMNS: 2,
  MAX_TITLE_LENGTH: 128,
  MAX_NAME_LENGTH: 30,
  API_TIMEOUT_MS: 1e4,
  CLEANUP_BATCH_SIZE: 10,
  MAX_CLEANUP_DISPLAY: 20,
  CLEANUP_LOCK_TTL_SECONDS: 1800,
  MAX_RETRY_ATTEMPTS: 3,
  D1_WRITE_MAX_RETRIES: 3,
  D1_WRITE_BASE_DELAY_MS: 120,
  D1_WRITE_MAX_DELAY_MS: 1200,
  KEYWORD_MAX_LENGTH: 200,
  KEYWORD_MATCH_MAX_TEXT_LENGTH: 4e3
};
var LOCAL_QUESTIONS = [
  { question: "\u51B0\u878D\u5316\u540E\u4F1A\u53D8\u6210\u4EC0\u4E48\uFF1F", correct_answer: "\u6C34", incorrect_answers: ["\u77F3\u5934", "\u6728\u5934", "\u706B"] },
  { question: "\u6B63\u5E38\u4EBA\u6709\u51E0\u53EA\u773C\u775B\uFF1F", correct_answer: "2", incorrect_answers: ["1", "3", "4"] },
  { question: "\u4EE5\u4E0B\u54EA\u4E2A\u5C5E\u4E8E\u6C34\u679C\uFF1F", correct_answer: "\u9999\u8549", incorrect_answers: ["\u767D\u83DC", "\u732A\u8089", "\u5927\u7C73"] },
  { question: "1 \u52A0 2 \u7B49\u4E8E\u51E0\uFF1F", correct_answer: "3", incorrect_answers: ["2", "4", "5"] },
  { question: "5 \u51CF 2 \u7B49\u4E8E\u51E0\uFF1F", correct_answer: "3", incorrect_answers: ["1", "2", "4"] },
  { question: "2 \u4E58\u4EE5 3 \u7B49\u4E8E\u51E0\uFF1F", correct_answer: "6", incorrect_answers: ["4", "5", "7"] },
  { question: "10 \u52A0 5 \u7B49\u4E8E\u51E0\uFF1F", correct_answer: "15", incorrect_answers: ["10", "12", "20"] },
  { question: "8 \u51CF 4 \u7B49\u4E8E\u51E0\uFF1F", correct_answer: "4", incorrect_answers: ["2", "3", "5"] },
  { question: "\u5728\u5929\u4E0A\u98DE\u7684\u4EA4\u901A\u5DE5\u5177\u662F\u4EC0\u4E48\uFF1F", correct_answer: "\u98DE\u673A", incorrect_answers: ["\u6C7D\u8F66", "\u8F6E\u8239", "\u81EA\u884C\u8F66"] },
  { question: "\u661F\u671F\u4E00\u7684\u540E\u9762\u662F\u661F\u671F\u51E0\uFF1F", correct_answer: "\u661F\u671F\u4E8C", incorrect_answers: ["\u661F\u671F\u65E5", "\u661F\u671F\u4E94", "\u661F\u671F\u4E09"] },
  { question: "\u9C7C\u901A\u5E38\u751F\u6D3B\u5728\u54EA\u91CC\uFF1F", correct_answer: "\u6C34\u91CC", incorrect_answers: ["\u6811\u4E0A", "\u571F\u91CC", "\u706B\u91CC"] },
  { question: "\u6211\u4EEC\u7528\u4EC0\u4E48\u5668\u5B98\u6765\u542C\u58F0\u97F3\uFF1F", correct_answer: "\u8033\u6735", incorrect_answers: ["\u773C\u775B", "\u9F3B\u5B50", "\u5634\u5DF4"] },
  { question: "\u6674\u6717\u7684\u5929\u7A7A\u901A\u5E38\u662F\u4EC0\u4E48\u989C\u8272\u7684\uFF1F", correct_answer: "\u84DD\u8272", incorrect_answers: ["\u7EFF\u8272", "\u7EA2\u8272", "\u7D2B\u8272"] },
  { question: "\u592A\u9633\u4ECE\u54EA\u4E2A\u65B9\u5411\u5347\u8D77\uFF1F", correct_answer: "\u4E1C\u65B9", incorrect_answers: ["\u897F\u65B9", "\u5357\u65B9", "\u5317\u65B9"] },
  { question: "\u5C0F\u72D7\u53D1\u51FA\u7684\u53EB\u58F0\u901A\u5E38\u662F\uFF1F", correct_answer: "\u6C6A\u6C6A", incorrect_answers: ["\u55B5\u55B5", "\u54A9\u54A9", "\u5471\u5471"] }
];

// src/core/logger.js
var Logger = {
  info(action, data = {}) {
    const log = {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      level: "INFO",
      action,
      ...data
    };
    console.log(JSON.stringify(log));
  },
  warn(action, data = {}) {
    const log = {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      level: "WARN",
      action,
      ...data
    };
    console.warn(JSON.stringify(log));
  },
  error(action, error, data = {}) {
    const log = {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      level: "ERROR",
      action,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : void 0,
      ...data
    };
    console.error(JSON.stringify(log));
  },
  debug(action, data = {}) {
    const log = {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      level: "DEBUG",
      action,
      ...data
    };
    console.log(JSON.stringify(log));
  }
};

// src/core/random.js
function secureRandomInt(min, max) {
  const range = max - min;
  const bytes = new Uint32Array(1);
  crypto.getRandomValues(bytes);
  return min + bytes[0] % range;
}
__name(secureRandomInt, "secureRandomInt");
function secureRandomId(length = 12) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return Array.from(bytes).map((b) => chars[b % chars.length]).join("");
}
__name(secureRandomId, "secureRandomId");

// src/adapters/telegram.js
async function tgCall(env, method, body, timeout = CONFIG.API_TIMEOUT_MS) {
  let base = env.API_BASE || "https://api.telegram.org";
  if (base.startsWith("http://")) {
    Logger.warn("api_http_upgraded", { originalBase: base });
    base = base.replace("http://", "https://");
  }
  try {
    new URL(`${base}/test`);
  } catch (e) {
    Logger.error("api_base_invalid", e, { base });
    base = "https://api.telegram.org";
  }
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    const resp = await fetch(`${base}/bot${env.BOT_TOKEN}/${method}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    if (!resp.ok && resp.status >= 500) {
      Logger.warn("telegram_api_server_error", {
        method,
        status: resp.status
      });
    }
    let result;
    try {
      result = await resp.json();
    } catch (parseError) {
      Logger.error("telegram_api_json_parse_failed", parseError, { method, status: resp.status });
      return { ok: false, description: "Invalid JSON response from Telegram" };
    }
    if (!result.ok && result.description && result.description.includes("Too Many Requests")) {
      const retryAfter = result.parameters?.retry_after || 5;
      Logger.warn("telegram_api_rate_limit", {
        method,
        retryAfter
      });
    }
    return result;
  } catch (e) {
    clearTimeout(timeoutId);
    if (e.name === "AbortError") {
      Logger.error("telegram_api_timeout", e, { method, timeout });
      return { ok: false, description: "Request timeout" };
    }
    Logger.error("telegram_api_failed", e, { method });
    return { ok: false, description: String(e.message) };
  }
}
__name(tgCall, "tgCall");

// src/services/rate-limit.js
async function checkRateLimit(userId, env, action = "message", limit = 20, window = 60) {
  if (!env.RATE_LIMIT_DO) {
    Logger.warn("rate_limit_do_not_configured", { userId, action });
    const key = `ratelimit:${action}:${userId}`;
    const countStr = await env.TOPIC_MAP.get(key);
    const count = parseInt(countStr || "0");
    if (count >= limit) {
      return { allowed: false, remaining: 0 };
    }
    await env.TOPIC_MAP.put(key, String(count + 1), { expirationTtl: window });
    return { allowed: true, remaining: limit - count - 1 };
  }
  try {
    const stub = env.RATE_LIMIT_DO.getByName(String(userId));
    const result = await stub.check(`${action}:${userId}`, limit, window);
    return { allowed: result.allowed, remaining: result.remaining };
  } catch (e) {
    if (e.retryable) {
      Logger.warn("rate_limit_do_retryable_error", { userId, action, error: e.message });
    } else if (e.overloaded) {
      Logger.warn("rate_limit_do_overloaded", { userId, action });
    } else {
      Logger.error("rate_limit_do_call_failed", e, { userId, action });
    }
    return { allowed: true, remaining: limit };
  }
}
__name(checkRateLimit, "checkRateLimit");

// src/adapters/storage-d1.js
var keywordCache = {
  ts: 0,
  list: []
};
function hasD1(env) {
  return !!env.TG_BOT_DB;
}
__name(hasD1, "hasD1");
function shouldRetryD1Error(error) {
  const message = String(error?.message || error || "");
  const retryable = [
    "Network connection lost",
    "Socket was closed",
    "reset because its code was updated",
    "storage reset because its code was updated"
  ];
  return retryable.some((text) => message.includes(text));
}
__name(shouldRetryD1Error, "shouldRetryD1Error");
async function runD1Write(env, action, fn) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      attempt++;
      const shouldRetry = shouldRetryD1Error(e) && attempt < CONFIG.D1_WRITE_MAX_RETRIES;
      if (!shouldRetry) {
        Logger.error("d1_write_failed", e, { action, attempt });
        throw e;
      }
      const base = CONFIG.D1_WRITE_BASE_DELAY_MS;
      const max = CONFIG.D1_WRITE_MAX_DELAY_MS;
      const delay = Math.min(max, base * 2 ** (attempt - 1) + Math.floor(Math.random() * base));
      Logger.warn("d1_write_retry", { action, attempt, delay });
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}
__name(runD1Write, "runD1Write");
function toDbBool(val) {
  return val ? 1 : 0;
}
__name(toDbBool, "toDbBool");
function normalizeUserRecord(row) {
  if (!row) return null;
  return {
    thread_id: row.thread_id ?? null,
    title: row.title ?? null,
    closed: row.closed ? true : false
  };
}
__name(normalizeUserRecord, "normalizeUserRecord");
async function ensureUserRow(env, userId) {
  if (!hasD1(env)) return;
  const now = Date.now();
  await runD1Write(env, "user_insert", async () => {
    await env.TG_BOT_DB.prepare("INSERT OR IGNORE INTO users (user_id, created_at, updated_at) VALUES (?, ?, ?)").bind(String(userId), now, now).run();
  });
}
__name(ensureUserRow, "ensureUserRow");
async function dbUserGet(env, userId) {
  if (!hasD1(env)) return null;
  const row = await env.TG_BOT_DB.prepare("SELECT user_id, thread_id, title, closed FROM users WHERE user_id = ?").bind(String(userId)).first();
  return normalizeUserRecord(row);
}
__name(dbUserGet, "dbUserGet");
async function dbUserUpdate(env, userId, data = {}) {
  if (!hasD1(env)) return;
  await ensureUserRow(env, userId);
  const fields = [];
  const values = [];
  if ("thread_id" in data) {
    fields.push("thread_id = ?");
    values.push(data.thread_id !== void 0 ? data.thread_id === null ? null : String(data.thread_id) : null);
  }
  if ("title" in data) {
    fields.push("title = ?");
    values.push(data.title ?? null);
  }
  if ("closed" in data) {
    fields.push("closed = ?");
    values.push(toDbBool(!!data.closed));
  }
  if ("verify_state" in data) {
    fields.push("verify_state = ?");
    values.push(data.verify_state ?? null);
  }
  if ("verify_expires_at" in data) {
    fields.push("verify_expires_at = ?");
    values.push(data.verify_expires_at ?? null);
  }
  if ("is_blocked" in data) {
    fields.push("is_blocked = ?");
    values.push(toDbBool(!!data.is_blocked));
  }
  if ("user_info_json" in data) {
    fields.push("user_info_json = ?");
    values.push(data.user_info_json ?? null);
  }
  if (fields.length === 0) return;
  const now = Date.now();
  fields.push("updated_at = ?");
  values.push(now);
  await runD1Write(env, "user_update", async () => {
    await env.TG_BOT_DB.prepare(`UPDATE users SET ${fields.join(", ")} WHERE user_id = ?`).bind(...values, String(userId)).run();
  });
}
__name(dbUserUpdate, "dbUserUpdate");
async function dbGetVerifyState(env, userId) {
  if (!hasD1(env)) return null;
  const row = await env.TG_BOT_DB.prepare("SELECT verify_state, verify_expires_at FROM users WHERE user_id = ?").bind(String(userId)).first();
  if (!row || !row.verify_state) return null;
  if (row.verify_state === "trusted") return "trusted";
  const expiresAt = Number(row.verify_expires_at || 0);
  if (expiresAt && expiresAt < Date.now()) {
    await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
    return null;
  }
  return row.verify_state;
}
__name(dbGetVerifyState, "dbGetVerifyState");
async function dbSetVerifyState(env, userId, state) {
  if (!hasD1(env)) return;
  if (!state) {
    await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
    return;
  }
  const now = Date.now();
  const expiresAt = state === "trusted" ? null : now + CONFIG.VERIFIED_EXPIRE_SECONDS * 1e3;
  await dbUserUpdate(env, userId, { verify_state: state, verify_expires_at: expiresAt });
}
__name(dbSetVerifyState, "dbSetVerifyState");
async function dbIsBanned(env, userId) {
  if (!hasD1(env)) return false;
  const row = await env.TG_BOT_DB.prepare("SELECT is_blocked FROM users WHERE user_id = ?").bind(String(userId)).first();
  return !!(row && row.is_blocked);
}
__name(dbIsBanned, "dbIsBanned");
async function dbSetBanned(env, userId, isBanned) {
  if (!hasD1(env)) return;
  await dbUserUpdate(env, userId, { is_blocked: !!isBanned });
}
__name(dbSetBanned, "dbSetBanned");
async function dbThreadGetUserId(env, threadId) {
  if (!hasD1(env)) return null;
  const row = await env.TG_BOT_DB.prepare("SELECT user_id FROM threads WHERE thread_id = ?").bind(String(threadId)).first();
  if (row?.user_id) return row.user_id;
  const fallback = await env.TG_BOT_DB.prepare("SELECT user_id FROM users WHERE thread_id = ?").bind(String(threadId)).first();
  if (fallback?.user_id) {
    await dbThreadPut(env, threadId, fallback.user_id);
    return fallback.user_id;
  }
  return null;
}
__name(dbThreadGetUserId, "dbThreadGetUserId");
async function dbThreadPut(env, threadId, userId) {
  if (!hasD1(env)) return;
  await runD1Write(env, "thread_put", async () => {
    await env.TG_BOT_DB.prepare("INSERT OR REPLACE INTO threads (thread_id, user_id) VALUES (?, ?)").bind(String(threadId), String(userId)).run();
  });
}
__name(dbThreadPut, "dbThreadPut");
async function dbThreadDelete(env, threadId) {
  if (!hasD1(env)) return;
  await runD1Write(env, "thread_delete", async () => {
    await env.TG_BOT_DB.prepare("DELETE FROM threads WHERE thread_id = ?").bind(String(threadId)).run();
  });
}
__name(dbThreadDelete, "dbThreadDelete");
async function dbMessageMapPut(env, sourceChatId, sourceMsgId, targetChatId, targetMsgId) {
  if (!hasD1(env)) return;
  const now = Date.now();
  await runD1Write(env, "message_map_put", async () => {
    await env.TG_BOT_DB.prepare(`INSERT OR REPLACE INTO messages
                (source_chat_id, source_msg_id, target_chat_id, target_msg_id, created_at)
                VALUES (?, ?, ?, ?, ?)`).bind(String(sourceChatId), String(sourceMsgId), String(targetChatId), String(targetMsgId), now).run();
  });
}
__name(dbMessageMapPut, "dbMessageMapPut");
async function dbMessageMapGet(env, sourceChatId, sourceMsgId) {
  if (!hasD1(env)) return null;
  const row = await env.TG_BOT_DB.prepare(`SELECT target_chat_id, target_msg_id, created_at
                  FROM messages WHERE source_chat_id = ? AND source_msg_id = ?`).bind(String(sourceChatId), String(sourceMsgId)).first();
  if (!row) return null;
  return {
    targetChatId: row.target_chat_id,
    targetMsgId: row.target_msg_id,
    createdAt: row.created_at
  };
}
__name(dbMessageMapGet, "dbMessageMapGet");
async function dbListUsers(env, limit, offset) {
  if (!hasD1(env)) return [];
  const result = await env.TG_BOT_DB.prepare("SELECT user_id, thread_id, title, closed FROM users LIMIT ? OFFSET ?").bind(limit, offset).all();
  return result?.results || [];
}
__name(dbListUsers, "dbListUsers");
async function dbKeywordList(env) {
  if (!hasD1(env)) return [];
  const result = await env.TG_BOT_DB.prepare("SELECT keyword FROM keywords ORDER BY id ASC").all();
  return (result?.results || []).map((row) => String(row.keyword)).filter(Boolean);
}
__name(dbKeywordList, "dbKeywordList");
async function dbKeywordListWithId(env) {
  if (!hasD1(env)) return [];
  const result = await env.TG_BOT_DB.prepare("SELECT id, keyword FROM keywords ORDER BY id ASC").all();
  return (result?.results || []).map((row) => ({ id: Number(row.id), keyword: String(row.keyword) })).filter((row) => row.keyword);
}
__name(dbKeywordListWithId, "dbKeywordListWithId");
async function dbKeywordAdd(env, keyword) {
  if (!hasD1(env)) return;
  await runD1Write(env, "keyword_add", async () => {
    await env.TG_BOT_DB.prepare("INSERT OR IGNORE INTO keywords (keyword, created_at) VALUES (?, ?)").bind(String(keyword), Date.now()).run();
  });
  keywordCache.ts = 0;
}
__name(dbKeywordAdd, "dbKeywordAdd");
async function dbKeywordDelete(env, keyword) {
  if (!hasD1(env)) return 0;
  let changes = 0;
  await runD1Write(env, "keyword_delete", async () => {
    const result = await env.TG_BOT_DB.prepare("DELETE FROM keywords WHERE keyword = ?").bind(String(keyword)).run();
    changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
  });
  keywordCache.ts = 0;
  return changes;
}
__name(dbKeywordDelete, "dbKeywordDelete");
async function dbKeywordDeleteById(env, id) {
  if (!hasD1(env)) return 0;
  let changes = 0;
  await runD1Write(env, "keyword_delete", async () => {
    const result = await env.TG_BOT_DB.prepare("DELETE FROM keywords WHERE id = ?").bind(Number(id)).run();
    changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
  });
  keywordCache.ts = 0;
  return changes;
}
__name(dbKeywordDeleteById, "dbKeywordDeleteById");
async function getKeywordListCached(env) {
  if (!hasD1(env)) return [];
  const now = Date.now();
  if (keywordCache.ts && now - keywordCache.ts < 6e4 && keywordCache.list.length) {
    return keywordCache.list;
  }
  const list = await dbKeywordList(env);
  keywordCache.ts = now;
  keywordCache.list = list;
  return list;
}
__name(getKeywordListCached, "getKeywordListCached");

// src/services/keywords.js
function getFilterText(msg) {
  if (msg.text) return String(msg.text);
  if (msg.caption) return String(msg.caption);
  return "";
}
__name(getFilterText, "getFilterText");
function validateKeywordPattern(raw) {
  const pattern = String(raw || "").trim();
  if (!pattern) return { ok: false, reason: "\u5173\u952E\u8BCD\u4E0D\u80FD\u4E3A\u7A7A" };
  if (pattern.length > CONFIG.KEYWORD_MAX_LENGTH) {
    return { ok: false, reason: `\u5173\u952E\u8BCD\u8FC7\u957F\uFF08\u6700\u5927 ${CONFIG.KEYWORD_MAX_LENGTH} \u5B57\u7B26\uFF09` };
  }
  const dotAnyCount = (pattern.match(/(\.\*|\.\+)/g) || []).length;
  if (dotAnyCount > 2) {
    return { ok: false, reason: "\u5305\u542B\u8FC7\u591A\u4EFB\u610F\u5339\u914D\uFF08.* / .+\uFF09" };
  }
  const nestedQuantifier = /(\([^)]*[+*][^)]*\)[+*?]|\([^)]*\{[^}]+\}[^)]*\)[+*?]|\([^)]*[+*][^)]*\)\{\d*,?\d*\})/;
  if (nestedQuantifier.test(pattern)) {
    return { ok: false, reason: "\u7591\u4F3C\u5D4C\u5957\u91CF\u8BCD" };
  }
  const repeatWithDotAny = /\([^)]*(\.\*|\.\+)[^)]*\)\{\d*,?\d*\}/;
  if (repeatWithDotAny.test(pattern)) {
    return { ok: false, reason: "\u5305\u542B\u9AD8\u98CE\u9669\u7684\u91CD\u590D\u5339\u914D\u7ED3\u6784" };
  }
  return { ok: true, reason: "" };
}
__name(validateKeywordPattern, "validateKeywordPattern");
async function matchKeyword(env, text) {
  if (!text) return null;
  const targetText = String(text).slice(0, CONFIG.KEYWORD_MATCH_MAX_TEXT_LENGTH);
  const list = await getKeywordListCached(env);
  if (!list.length) return null;
  for (const keyword of list) {
    const raw = String(keyword).trim();
    if (!raw) continue;
    const validation = validateKeywordPattern(raw);
    if (!validation.ok) {
      Logger.warn("keyword_pattern_blocked", { keyword: raw, reason: validation.reason });
      continue;
    }
    try {
      const re = new RegExp(raw, "i");
      if (re.test(targetText)) return keyword;
    } catch {
      Logger.warn("keyword_regex_invalid", { keyword: raw });
    }
  }
  return null;
}
__name(matchKeyword, "matchKeyword");

// src/services/admin.js
var adminStatusCache = /* @__PURE__ */ new Map();
function parseAdminIdAllowlist(env) {
  const raw = (env.ADMIN_IDS || "").toString().trim();
  if (!raw) return /* @__PURE__ */ new Set();
  return new Set(
    raw.split(",").map((s) => s.trim()).filter(Boolean).filter((v) => /^\d+$/.test(v))
  );
}
__name(parseAdminIdAllowlist, "parseAdminIdAllowlist");
async function isAdminUser(env, userId) {
  if (!userId) return false;
  const allowlist = parseAdminIdAllowlist(env);
  if (allowlist.has(String(userId))) {
    return true;
  }
  const cacheKey = String(userId);
  const now = Date.now();
  const cached = adminStatusCache.get(cacheKey);
  if (cached && now - cached.ts < CONFIG.ADMIN_CACHE_TTL_SECONDS * 1e3) {
    return cached.isAdmin;
  }
  const kvKey = `admin:${userId}`;
  const kvVal = await env.TOPIC_MAP.get(kvKey);
  if (kvVal === "1" || kvVal === "0") {
    const isAdmin = kvVal === "1";
    adminStatusCache.set(cacheKey, { ts: now, isAdmin });
    return isAdmin;
  }
  try {
    const res = await tgCall(env, "getChatMember", {
      chat_id: env.SUPERGROUP_ID,
      user_id: userId
    });
    const status = res.result?.status;
    const isAdmin = res.ok && (status === "creator" || status === "administrator");
    await env.TOPIC_MAP.put(kvKey, isAdmin ? "1" : "0", { expirationTtl: CONFIG.ADMIN_CACHE_TTL_SECONDS });
    adminStatusCache.set(cacheKey, { ts: now, isAdmin });
    return isAdmin;
  } catch (e) {
    Logger.warn("admin_check_failed", { userId });
    return false;
  }
}
__name(isAdminUser, "isAdminUser");

// src/services/topic-utils.js
function withMessageThreadId(body, threadId) {
  if (threadId === void 0 || threadId === null) return body;
  return { ...body, message_thread_id: threadId };
}
__name(withMessageThreadId, "withMessageThreadId");
function normalizeTgDescription(description) {
  return (description || "").toString().toLowerCase();
}
__name(normalizeTgDescription, "normalizeTgDescription");
function isTopicMissingOrDeleted(description) {
  const desc = normalizeTgDescription(description);
  return desc.includes("thread not found") || desc.includes("topic not found") || desc.includes("message thread not found") || desc.includes("topic deleted") || desc.includes("thread deleted") || desc.includes("forum topic not found") || desc.includes("topic closed permanently");
}
__name(isTopicMissingOrDeleted, "isTopicMissingOrDeleted");
function isTestMessageInvalid(description) {
  const desc = normalizeTgDescription(description);
  return desc.includes("message text is empty") || desc.includes("bad request: message text is empty");
}
__name(isTestMessageInvalid, "isTestMessageInvalid");
async function sendWelcomeCard(env, threadId, userId, userFrom) {
  if (!userFrom) return;
  const firstName = (userFrom.first_name || "").trim();
  const lastName = (userFrom.last_name || "").trim();
  const userNameStr = userFrom.username ? `@${userFrom.username}` : "\u672A\u8BBE\u7F6E\u7528\u6237\u540D";
  const fullName = (firstName + (lastName ? " " + lastName : "")).trim() || "\u533F\u540D\u7528\u6237";
  const cardText = `\u{1F464} <b>\u65B0\u7528\u6237\u63A5\u5165</b>
ID: <code>${userId}</code>
\u540D\u5B57: <a href="tg://user?id=${userId}">${fullName}</a>
\u7528\u6237\u540D: ${userNameStr}
#id${userId}`;
  try {
    await tgCall(env, "sendMessage", {
      chat_id: env.SUPERGROUP_ID,
      message_thread_id: threadId,
      text: cardText,
      parse_mode: "HTML"
    });
    Logger.info("welcome_card_sent", { userId, threadId });
  } catch (e) {
    Logger.warn("welcome_card_send_failed", { userId, threadId, error: e.message });
  }
}
__name(sendWelcomeCard, "sendWelcomeCard");
async function probeForumThread(env, expectedThreadId, { userId, reason, doubleCheckOnMissingThreadId = true } = {}) {
  const attemptOnce = /* @__PURE__ */ __name(async () => {
    const res = await tgCall(env, "sendMessage", {
      chat_id: env.SUPERGROUP_ID,
      message_thread_id: expectedThreadId,
      text: "\u{1F50E}"
      // 在tg发送符号作为探测，该符号会自动加入动效，后续应改为使用纯文本
    });
    const actualThreadId = res.result?.message_thread_id;
    const probeMessageId = res.result?.message_id;
    if (res.ok && probeMessageId) {
      try {
        await tgCall(env, "deleteMessage", {
          chat_id: env.SUPERGROUP_ID,
          message_id: probeMessageId
        });
      } catch {
      }
    }
    if (!res.ok) {
      if (isTopicMissingOrDeleted(res.description)) {
        return { status: "missing", description: res.description };
      }
      if (isTestMessageInvalid(res.description)) {
        return { status: "probe_invalid", description: res.description };
      }
      return { status: "unknown_error", description: res.description };
    }
    if (actualThreadId === void 0 || actualThreadId === null) {
      return { status: "missing_thread_id" };
    }
    if (Number(actualThreadId) !== Number(expectedThreadId)) {
      return { status: "redirected", actualThreadId };
    }
    return { status: "ok" };
  }, "attemptOnce");
  const first = await attemptOnce();
  if (first.status !== "missing_thread_id" || !doubleCheckOnMissingThreadId) {
    return first;
  }
  const second = await attemptOnce();
  if (second.status === "missing_thread_id") {
    Logger.warn("probe_missing_thread_id_confirmed", {
      userId,
      expectedThreadId,
      reason
    });
  }
  return second;
}
__name(probeForumThread, "probeForumThread");

// src/services/verification.js
async function sendVerificationChallengeImpl({
  userId,
  env,
  pendingMsgId,
  safeGetJSON: safeGetJSON2,
  checkRateLimit: checkRateLimit2,
  tgCall: tgCall2,
  Logger: Logger2,
  CONFIG: CONFIG2,
  LOCAL_QUESTIONS: LOCAL_QUESTIONS2,
  secureRandomInt: secureRandomInt2,
  shuffleArray: shuffleArray2,
  secureRandomId: secureRandomId2
}) {
  const existingChallenge = await env.TOPIC_MAP.get(`user_challenge:${userId}`);
  if (existingChallenge) {
    const chalKey = `chal:${existingChallenge}`;
    const state2 = await safeGetJSON2(env, chalKey, null);
    if (!state2 || state2.userId !== userId) {
      await env.TOPIC_MAP.delete(`user_challenge:${userId}`);
    } else {
      if (pendingMsgId) {
        let pendingIds = [];
        if (Array.isArray(state2.pending_ids)) {
          pendingIds = state2.pending_ids.slice();
        } else if (state2.pending) {
          pendingIds = [state2.pending];
        }
        if (!pendingIds.includes(pendingMsgId)) {
          pendingIds.push(pendingMsgId);
          if (pendingIds.length > CONFIG2.PENDING_MAX_MESSAGES) {
            pendingIds = pendingIds.slice(pendingIds.length - CONFIG2.PENDING_MAX_MESSAGES);
          }
          state2.pending_ids = pendingIds;
          delete state2.pending;
          await env.TOPIC_MAP.put(chalKey, JSON.stringify(state2), { expirationTtl: CONFIG2.VERIFY_EXPIRE_SECONDS });
        }
      }
      Logger2.debug("verification_duplicate_skipped", { userId, verifyId: existingChallenge, hasPending: !!pendingMsgId });
      return;
    }
  }
  const verifyLimit = await checkRateLimit2(userId, env, "verify", CONFIG2.RATE_LIMIT_VERIFY, 300);
  if (!verifyLimit.allowed) {
    await tgCall2(env, "sendMessage", {
      chat_id: userId,
      text: "\u26A0\uFE0F \u9A8C\u8BC1\u8BF7\u6C42\u8FC7\u4E8E\u9891\u7E41\uFF0C\u8BF75\u5206\u949F\u540E\u518D\u8BD5\u3002"
    });
    return;
  }
  const q = LOCAL_QUESTIONS2[secureRandomInt2(0, LOCAL_QUESTIONS2.length)];
  const challenge = {
    question: q.question,
    correct: q.correct_answer,
    options: shuffleArray2([...q.incorrect_answers, q.correct_answer])
  };
  const verifyId = secureRandomId2(CONFIG2.VERIFY_ID_LENGTH);
  const answerIndex = challenge.options.indexOf(challenge.correct);
  const state = {
    answerIndex,
    options: challenge.options,
    pending_ids: pendingMsgId ? [pendingMsgId] : [],
    userId
  };
  await env.TOPIC_MAP.put(`chal:${verifyId}`, JSON.stringify(state), { expirationTtl: CONFIG2.VERIFY_EXPIRE_SECONDS });
  await env.TOPIC_MAP.put(`user_challenge:${userId}`, verifyId, { expirationTtl: CONFIG2.VERIFY_EXPIRE_SECONDS });
  Logger2.info("verification_sent", {
    userId,
    verifyId,
    question: q.question,
    pendingCount: state.pending_ids.length
  });
  const buttons = challenge.options.map((opt, idx) => ({
    text: opt,
    callback_data: `verify:${verifyId}:${idx}`
  }));
  const keyboard = [];
  for (let i = 0; i < buttons.length; i += CONFIG2.BUTTON_COLUMNS) {
    keyboard.push(buttons.slice(i, i + CONFIG2.BUTTON_COLUMNS));
  }
  await tgCall2(env, "sendMessage", {
    chat_id: userId,
    text: `\u{1F6E1}\uFE0F **\u4EBA\u673A\u9A8C\u8BC1**

${challenge.question}

\u8BF7\u70B9\u51FB\u4E0B\u65B9\u6309\u94AE\u56DE\u7B54 (\u56DE\u7B54\u6B63\u786E\u540E\u5C06\u81EA\u52A8\u53D1\u9001\u60A8\u521A\u624D\u7684\u6D88\u606F)\u3002`,
    parse_mode: "Markdown",
    reply_markup: { inline_keyboard: keyboard }
  });
}
__name(sendVerificationChallengeImpl, "sendVerificationChallengeImpl");
async function handleCallbackQueryImpl({
  query,
  env,
  ctx,
  tgCall: tgCall2,
  Logger: Logger2,
  hasD1: hasD12,
  dbSetVerifyState: dbSetVerifyState2,
  CONFIG: CONFIG2,
  forwardToTopic: forwardToTopic2
}) {
  try {
    const data = query.data;
    if (!data.startsWith("verify:")) return;
    const parts = data.split(":");
    if (parts.length !== 3) return;
    const verifyId = parts[1];
    const selectedIndex = parseInt(parts[2]);
    const userId = query.from.id;
    const stateStr = await env.TOPIC_MAP.get(`chal:${verifyId}`);
    if (!stateStr) {
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u274C \u9A8C\u8BC1\u5DF2\u8FC7\u671F\uFF0C\u8BF7\u91CD\u53D1\u6D88\u606F",
        show_alert: true
      });
      return;
    }
    let state;
    try {
      state = JSON.parse(stateStr);
    } catch {
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u274C \u6570\u636E\u9519\u8BEF",
        show_alert: true
      });
      return;
    }
    if (state.userId && state.userId !== userId) {
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u274C \u65E0\u6548\u7684\u9A8C\u8BC1",
        show_alert: true
      });
      return;
    }
    if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= state.options.length) {
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u274C \u65E0\u6548\u9009\u9879",
        show_alert: true
      });
      return;
    }
    if (selectedIndex === state.answerIndex) {
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u2705 \u9A8C\u8BC1\u901A\u8FC7"
      });
      Logger2.info("verification_passed", {
        userId,
        verifyId,
        selectedOption: state.options[selectedIndex]
      });
      if (hasD12(env)) {
        await dbSetVerifyState2(env, userId, "1");
      } else {
        await env.TOPIC_MAP.put(`verified:${userId}`, "1", { expirationTtl: CONFIG2.VERIFIED_EXPIRE_SECONDS });
      }
      await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
      await env.TOPIC_MAP.delete(`chal:${verifyId}`);
      await env.TOPIC_MAP.delete(`user_challenge:${userId}`);
      await tgCall2(env, "editMessageText", {
        chat_id: userId,
        message_id: query.message.message_id,
        text: `\u2705 **\u9A8C\u8BC1\u6210\u529F**

\u60A8\u73B0\u5728\u53EF\u4EE5\u81EA\u7531\u5BF9\u8BDD\u4E86\u3002`,
        parse_mode: "Markdown"
      });
      const hasPending = Array.isArray(state.pending_ids) && state.pending_ids.length > 0 || !!state.pending;
      if (hasPending) {
        try {
          let pendingIds = [];
          if (Array.isArray(state.pending_ids)) {
            pendingIds = state.pending_ids.slice();
          } else if (state.pending) {
            pendingIds = [state.pending];
          }
          if (pendingIds.length > CONFIG2.PENDING_MAX_MESSAGES) {
            pendingIds = pendingIds.slice(pendingIds.length - CONFIG2.PENDING_MAX_MESSAGES);
          }
          let forwardedCount = 0;
          for (const pendingId of pendingIds) {
            if (!pendingId) continue;
            const forwardedKey = `forwarded:${userId}:${pendingId}`;
            const alreadyForwarded = await env.TOPIC_MAP.get(forwardedKey);
            if (alreadyForwarded) {
              Logger2.info("message_forward_duplicate_skipped", { userId, messageId: pendingId });
              continue;
            }
            const fakeMsg = {
              message_id: pendingId,
              chat: { id: userId, type: "private" },
              from: query.from
            };
            await forwardToTopic2(fakeMsg, env, ctx);
            await env.TOPIC_MAP.put(forwardedKey, "1", { expirationTtl: 3600 });
            forwardedCount++;
          }
          if (forwardedCount > 0) {
            await tgCall2(env, "sendMessage", {
              chat_id: userId,
              text: `\u{1F4E9} \u521A\u624D\u7684 ${forwardedCount} \u6761\u6D88\u606F\u5DF2\u5E2E\u60A8\u9001\u8FBE\u3002`
            });
          }
        } catch (e) {
          Logger2.error("pending_message_forward_failed", e, { userId });
          await tgCall2(env, "sendMessage", {
            chat_id: userId,
            text: "\u26A0\uFE0F \u81EA\u52A8\u53D1\u9001\u5931\u8D25\uFF0C\u8BF7\u91CD\u65B0\u53D1\u9001\u60A8\u7684\u6D88\u606F\u3002"
          });
        }
      }
    } else {
      Logger2.info("verification_failed", {
        userId,
        verifyId,
        selectedIndex,
        correctIndex: state.answerIndex
      });
      await tgCall2(env, "answerCallbackQuery", {
        callback_query_id: query.id,
        text: "\u274C \u7B54\u6848\u9519\u8BEF",
        show_alert: true
      });
    }
  } catch (e) {
    Logger2.error("callback_query_error", e, {
      userId: query.from?.id,
      callbackData: query.data
    });
    await tgCall2(env, "answerCallbackQuery", {
      callback_query_id: query.id,
      text: "\u26A0\uFE0F \u7CFB\u7EDF\u9519\u8BEF\uFF0C\u8BF7\u91CD\u8BD5",
      show_alert: true
    });
  }
}
__name(handleCallbackQueryImpl, "handleCallbackQueryImpl");

// src/services/media-group.js
async function handleMediaGroupImpl({
  msg,
  env,
  ctx,
  direction,
  targetChat,
  threadId,
  tgCall: tgCall2,
  withMessageThreadId: withMessageThreadId2,
  safeGetJSON: safeGetJSON2,
  delaySend: delaySend2,
  CONFIG: CONFIG2
}) {
  const groupId = msg.media_group_id;
  const key = `mg:${direction}:${groupId}`;
  const item = extractMedia(msg);
  if (!item) {
    await tgCall2(env, "copyMessage", withMessageThreadId2({
      chat_id: targetChat,
      from_chat_id: msg.chat.id,
      message_id: msg.message_id
    }, threadId));
    return;
  }
  let rec = await safeGetJSON2(env, key, null);
  if (!rec) rec = { direction, targetChat, threadId: threadId === null ? void 0 : threadId, items: [], last_ts: Date.now() };
  rec.items.push({ ...item, msg_id: msg.message_id });
  rec.last_ts = Date.now();
  await env.TOPIC_MAP.put(key, JSON.stringify(rec), { expirationTtl: CONFIG2.MEDIA_GROUP_EXPIRE_SECONDS });
  ctx.waitUntil(delaySend2(env, key, rec.last_ts));
}
__name(handleMediaGroupImpl, "handleMediaGroupImpl");
function extractMedia(msg) {
  if (msg.photo && msg.photo.length > 0) {
    const highestResolution = msg.photo[msg.photo.length - 1];
    return {
      type: "photo",
      id: highestResolution.file_id,
      cap: msg.caption || ""
    };
  }
  if (msg.video) {
    return {
      type: "video",
      id: msg.video.file_id,
      cap: msg.caption || ""
    };
  }
  if (msg.document) {
    return {
      type: "document",
      id: msg.document.file_id,
      cap: msg.caption || ""
    };
  }
  if (msg.audio) {
    return {
      type: "audio",
      id: msg.audio.file_id,
      cap: msg.caption || ""
    };
  }
  if (msg.animation) {
    return {
      type: "animation",
      id: msg.animation.file_id,
      cap: msg.caption || ""
    };
  }
  return null;
}
__name(extractMedia, "extractMedia");
async function flushExpiredMediaGroupsImpl({ env, now, getAllKeys: getAllKeys2, safeGetJSON: safeGetJSON2, Logger: Logger2 }) {
  try {
    const prefix = "mg:";
    const allKeys = await getAllKeys2(env, prefix);
    let deletedCount = 0;
    for (const { name } of allKeys) {
      const rec = await safeGetJSON2(env, name, null);
      if (rec && rec.last_ts && now - rec.last_ts > 3e5) {
        await env.TOPIC_MAP.delete(name);
        deletedCount++;
      }
    }
    if (deletedCount > 0) {
      Logger2.info("media_groups_cleaned", { deletedCount });
    }
  } catch (e) {
    Logger2.error("media_group_cleanup_failed", e);
  }
}
__name(flushExpiredMediaGroupsImpl, "flushExpiredMediaGroupsImpl");
async function delaySendImpl({
  env,
  key,
  ts,
  CONFIG: CONFIG2,
  safeGetJSON: safeGetJSON2,
  Logger: Logger2,
  tgCall: tgCall2,
  withMessageThreadId: withMessageThreadId2
}) {
  await new Promise((r) => setTimeout(r, CONFIG2.MEDIA_GROUP_DELAY_MS));
  const rec = await safeGetJSON2(env, key, null);
  if (rec && rec.last_ts === ts) {
    if (!rec.items || rec.items.length === 0) {
      Logger2.warn("media_group_empty", { key });
      await env.TOPIC_MAP.delete(key);
      return;
    }
    const media = rec.items.map((it, i) => {
      if (!it.type || !it.id) {
        Logger2.warn("media_group_invalid_item", { key, item: it });
        return null;
      }
      const caption = i === 0 ? (it.cap || "").substring(0, 1024) : "";
      return {
        type: it.type,
        media: it.id,
        caption
      };
    }).filter(Boolean);
    if (media.length > 0) {
      try {
        const result = await tgCall2(env, "sendMediaGroup", withMessageThreadId2({
          chat_id: rec.targetChat,
          media
        }, rec.threadId));
        if (!result.ok) {
          Logger2.error("media_group_send_failed", result.description, {
            key,
            mediaCount: media.length
          });
        } else {
          Logger2.info("media_group_sent", {
            key,
            mediaCount: media.length,
            targetChat: rec.targetChat
          });
        }
      } catch (e) {
        Logger2.error("media_group_send_exception", e, { key });
      }
    }
    await env.TOPIC_MAP.delete(key);
  }
}
__name(delaySendImpl, "delaySendImpl");

// src/services/cleanup.js
async function handleCleanupCommandImpl({
  threadId,
  env,
  CONFIG: CONFIG2,
  hasD1: hasD12,
  dbListUsers: dbListUsers2,
  probeForumThread: probeForumThread2,
  resetUserVerificationAndRequireReverify: resetUserVerificationAndRequireReverify2,
  Logger: Logger2,
  safeGetJSON: safeGetJSON2,
  deleteBulk: deleteBulk2,
  tgCall: tgCall2,
  withMessageThreadId: withMessageThreadId2
}) {
  const lockKey = "cleanup:lock";
  const locked = await env.TOPIC_MAP.get(lockKey);
  if (locked) {
    await tgCall2(env, "sendMessage", withMessageThreadId2({
      chat_id: env.SUPERGROUP_ID,
      text: "\u23F3 **\u5DF2\u6709\u6E05\u7406\u4EFB\u52A1\u6B63\u5728\u8FD0\u884C\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5\u3002**",
      parse_mode: "Markdown"
    }, threadId));
    return;
  }
  await env.TOPIC_MAP.put(lockKey, "1", { expirationTtl: CONFIG2.CLEANUP_LOCK_TTL_SECONDS });
  await tgCall2(env, "sendMessage", withMessageThreadId2({
    chat_id: env.SUPERGROUP_ID,
    text: "\u{1F504} **\u6B63\u5728\u626B\u63CF\u9700\u8981\u6E05\u7406\u7684\u7528\u6237...**",
    parse_mode: "Markdown"
  }, threadId));
  let cleanedCount = 0;
  let errorCount = 0;
  const cleanedUsers = [];
  let scannedCount = 0;
  try {
    if (hasD12(env)) {
      let offset = 0;
      while (true) {
        const rows = await dbListUsers2(env, CONFIG2.CLEANUP_BATCH_SIZE, offset);
        if (!rows.length) break;
        scannedCount += rows.length;
        const results = await Promise.allSettled(
          rows.map(async (row) => {
            if (!row.thread_id) return null;
            const userId = row.user_id;
            const topicThreadId = row.thread_id;
            const probe = await probeForumThread2(env, topicThreadId, {
              userId,
              reason: "cleanup_check",
              doubleCheckOnMissingThreadId: false
            });
            if (probe.status === "redirected" || probe.status === "missing") {
              await resetUserVerificationAndRequireReverify2(env, {
                userId,
                userKey: null,
                oldThreadId: topicThreadId,
                pendingMsgId: null,
                reason: "cleanup_check"
              });
              return {
                userId,
                threadId: topicThreadId,
                title: row.title || "\u672A\u77E5"
              };
            } else if (probe.status === "probe_invalid") {
              Logger2.warn("cleanup_probe_invalid_message", {
                userId,
                threadId: topicThreadId,
                errorDescription: probe.description
              });
            } else if (probe.status === "unknown_error") {
              Logger2.warn("cleanup_probe_failed_unknown", {
                userId,
                threadId: topicThreadId,
                errorDescription: probe.description
              });
            } else if (probe.status === "missing_thread_id") {
              Logger2.warn("cleanup_probe_missing_thread_id", { userId, threadId: topicThreadId });
            }
            return null;
          })
        );
        results.forEach((result) => {
          if (result.status === "fulfilled" && result.value) {
            cleanedCount++;
            cleanedUsers.push(result.value);
            Logger2.info("cleanup_user", {
              userId: result.value.userId,
              threadId: result.value.threadId
            });
          } else if (result.status === "rejected") {
            errorCount++;
            Logger2.error("cleanup_batch_error", result.reason);
          }
        });
        offset += rows.length;
        await new Promise((r) => setTimeout(r, 200));
      }
    } else {
      const keysToDelete = [];
      let cursor = void 0;
      do {
        const result = await env.TOPIC_MAP.list({ prefix: "user:", cursor });
        const names = (result.keys || []).map((k) => k.name);
        scannedCount += names.length;
        for (let i = 0; i < names.length; i += CONFIG2.CLEANUP_BATCH_SIZE) {
          const batch = names.slice(i, i + CONFIG2.CLEANUP_BATCH_SIZE);
          const results = await Promise.allSettled(
            batch.map(async (name) => {
              const rec = await safeGetJSON2(env, name, null);
              if (!rec || !rec.thread_id) return null;
              const userId = name.slice(5);
              const topicThreadId = rec.thread_id;
              const probe = await probeForumThread2(env, topicThreadId, {
                userId,
                reason: "cleanup_check",
                doubleCheckOnMissingThreadId: false
              });
              if (probe.status === "redirected" || probe.status === "missing") {
                keysToDelete.push(
                  name,
                  `verified:${userId}`,
                  `thread:${topicThreadId}`
                );
                return {
                  userId,
                  threadId: topicThreadId,
                  title: rec.title || "\u672A\u77E5"
                };
              } else if (probe.status === "probe_invalid") {
                Logger2.warn("cleanup_probe_invalid_message", {
                  userId,
                  threadId: topicThreadId,
                  errorDescription: probe.description
                });
              } else if (probe.status === "unknown_error") {
                Logger2.warn("cleanup_probe_failed_unknown", {
                  userId,
                  threadId: topicThreadId,
                  errorDescription: probe.description
                });
              } else if (probe.status === "missing_thread_id") {
                Logger2.warn("cleanup_probe_missing_thread_id", { userId, threadId: topicThreadId });
              }
              return null;
            })
          );
          results.forEach((result2) => {
            if (result2.status === "fulfilled" && result2.value) {
              cleanedCount++;
              cleanedUsers.push(result2.value);
              Logger2.info("cleanup_user", {
                userId: result2.value.userId,
                threadId: result2.value.threadId
              });
            } else if (result2.status === "rejected") {
              errorCount++;
              Logger2.error("cleanup_batch_error", result2.reason);
            }
          });
          if (i + CONFIG2.CLEANUP_BATCH_SIZE < names.length) {
            await new Promise((r) => setTimeout(r, 600));
          }
        }
        cursor = result.list_complete ? void 0 : result.cursor;
        if (cursor) {
          await new Promise((r) => setTimeout(r, 200));
        }
      } while (cursor);
      if (keysToDelete.length > 0) {
        const deletedCount = await deleteBulk2(env, keysToDelete);
        Logger2.info("cleanup_bulk_delete", { deletedKeyCount: deletedCount });
      }
    }
    let reportText = `\u2705 **\u6E05\u7406\u5B8C\u6210**

`;
    reportText += `\u{1F4CA} **\u7EDF\u8BA1\u4FE1\u606F**
`;
    reportText += `- \u626B\u63CF\u7528\u6237\u6570: ${scannedCount}
`;
    reportText += `- \u5DF2\u6E05\u7406\u7528\u6237\u6570: ${cleanedCount}
`;
    reportText += `- \u9519\u8BEF\u6570: ${errorCount}

`;
    if (cleanedCount > 0) {
      reportText += `\u{1F5D1}\uFE0F **\u5DF2\u6E05\u7406\u7684\u7528\u6237** (\u8BDD\u9898\u5DF2\u5220\u9664):
`;
      for (const user of cleanedUsers.slice(0, CONFIG2.MAX_CLEANUP_DISPLAY)) {
        reportText += `- UID: \`${user.userId}\` | \u8BDD\u9898: ${user.title}
`;
      }
      if (cleanedUsers.length > CONFIG2.MAX_CLEANUP_DISPLAY) {
        reportText += `
...(\u8FD8\u6709 ${cleanedUsers.length - CONFIG2.MAX_CLEANUP_DISPLAY} \u4E2A\u7528\u6237)
`;
      }
      reportText += `
\u{1F4A1} \u8FD9\u4E9B\u7528\u6237\u4E0B\u6B21\u53D1\u6D88\u606F\u65F6\u5C06\u91CD\u65B0\u8FDB\u884C\u4EBA\u673A\u9A8C\u8BC1\u5E76\u521B\u5EFA\u65B0\u8BDD\u9898\u3002`;
    } else {
      reportText += `\u2728 \u6CA1\u6709\u53D1\u73B0\u9700\u8981\u6E05\u7406\u7684\u7528\u6237\u8BB0\u5F55\u3002`;
    }
    Logger2.info("cleanup_completed", {
      cleanedCount,
      errorCount,
      totalUsers: scannedCount
    });
    await tgCall2(env, "sendMessage", withMessageThreadId2({
      chat_id: env.SUPERGROUP_ID,
      text: reportText,
      parse_mode: "Markdown"
    }, threadId));
  } catch (e) {
    Logger2.error("cleanup_failed", e, { threadId });
    await tgCall2(env, "sendMessage", withMessageThreadId2({
      chat_id: env.SUPERGROUP_ID,
      text: `\u274C **\u6E05\u7406\u8FC7\u7A0B\u51FA\u9519**

\u9519\u8BEF\u4FE1\u606F: \`${e.message}\``,
      parse_mode: "Markdown"
    }, threadId));
  } finally {
    await env.TOPIC_MAP.delete(lockKey);
  }
}
__name(handleCleanupCommandImpl, "handleCleanupCommandImpl");

// src/services/topic-lifecycle.js
async function getOrCreateUserTopicRecImpl({
  from,
  key,
  env,
  userId,
  hasD1: hasD12,
  dbUserGet: dbUserGet2,
  safeGetJSON: safeGetJSON2,
  createTopic: createTopic2,
  topicCreateInFlight: topicCreateInFlight2
}) {
  if (hasD12(env)) {
    const existing = await dbUserGet2(env, userId);
    if (existing && existing.thread_id) return existing;
  } else {
    const existing = await safeGetJSON2(env, key, null);
    if (existing && existing.thread_id) return existing;
  }
  const inflight = topicCreateInFlight2.get(String(userId));
  if (inflight) return await inflight;
  const p = (async () => {
    if (hasD12(env)) {
      const again = await dbUserGet2(env, userId);
      if (again && again.thread_id) return again;
    } else {
      const again = await safeGetJSON2(env, key, null);
      if (again && again.thread_id) return again;
    }
    return await createTopic2(from, key, env, userId);
  })();
  topicCreateInFlight2.set(String(userId), p);
  try {
    return await p;
  } finally {
    if (topicCreateInFlight2.get(String(userId)) === p) {
      topicCreateInFlight2.delete(String(userId));
    }
  }
}
__name(getOrCreateUserTopicRecImpl, "getOrCreateUserTopicRecImpl");
async function resetUserVerificationAndRequireReverifyImpl({
  env,
  userId,
  userKey,
  oldThreadId,
  pendingMsgId,
  reason,
  hasD1: hasD12,
  dbUserUpdate: dbUserUpdate2,
  dbThreadDelete: dbThreadDelete2,
  CONFIG: CONFIG2,
  threadHealthCache: threadHealthCache2,
  Logger: Logger2,
  sendVerificationChallenge: sendVerificationChallenge2
}) {
  if (hasD12(env)) {
    await dbUserUpdate2(env, userId, { verify_state: null, verify_expires_at: null });
  } else {
    await env.TOPIC_MAP.delete(`verified:${userId}`);
  }
  await env.TOPIC_MAP.put(`needs_verify:${userId}`, "1", { expirationTtl: CONFIG2.NEEDS_REVERIFY_TTL_SECONDS });
  await env.TOPIC_MAP.delete(`retry:${userId}`);
  if (userKey) {
    if (hasD12(env)) {
      await dbUserUpdate2(env, userId, { thread_id: null, title: null, closed: false });
    } else {
      await env.TOPIC_MAP.delete(userKey);
    }
  }
  if (oldThreadId !== void 0 && oldThreadId !== null) {
    if (hasD12(env)) {
      await dbThreadDelete2(env, oldThreadId);
    } else {
      await env.TOPIC_MAP.delete(`thread:${oldThreadId}`);
    }
    await env.TOPIC_MAP.delete(`thread_ok:${oldThreadId}`);
    threadHealthCache2.delete(oldThreadId);
  }
  Logger2.info("verification_reset_due_to_topic_loss", {
    userId,
    oldThreadId,
    pendingMsgId,
    reason
  });
  await sendVerificationChallenge2(userId, env, pendingMsgId || null);
}
__name(resetUserVerificationAndRequireReverifyImpl, "resetUserVerificationAndRequireReverifyImpl");
function buildTopicTitleImpl(from, CONFIG2) {
  const firstName = (from.first_name || "").trim().substring(0, CONFIG2.MAX_NAME_LENGTH);
  const lastName = (from.last_name || "").trim().substring(0, CONFIG2.MAX_NAME_LENGTH);
  let username = "";
  if (from.username) {
    username = from.username.replace(/[^\w]/g, "").substring(0, 20);
  }
  const cleanName = (firstName + " " + lastName).replace(/[\u0000-\u001F\u007F-\u009F]/g, "").replace(/\s+/g, " ").trim();
  const name = cleanName || "User";
  const usernameStr = username ? ` @${username}` : "";
  const title = (name + usernameStr).substring(0, CONFIG2.MAX_TITLE_LENGTH);
  return title;
}
__name(buildTopicTitleImpl, "buildTopicTitleImpl");
async function createTopicImpl({
  from,
  key,
  env,
  userId,
  CONFIG: CONFIG2,
  tgCall: tgCall2,
  hasD1: hasD12,
  dbUserUpdate: dbUserUpdate2,
  dbThreadPut: dbThreadPut2,
  putWithMetadata: putWithMetadata2,
  buildTopicTitle: buildTopicTitle2
}) {
  const title = buildTopicTitle2(from);
  if (!env.SUPERGROUP_ID.toString().startsWith("-100")) throw new Error("SUPERGROUP_ID\u5FC5\u987B\u4EE5-100\u5F00\u5934");
  const res = await tgCall2(env, "createForumTopic", { chat_id: env.SUPERGROUP_ID, name: title });
  if (!res.ok) throw new Error(`\u521B\u5EFA\u8BDD\u9898\u5931\u8D25: ${res.description}`);
  const rec = { thread_id: res.result.message_thread_id, title, closed: false };
  if (hasD12(env)) {
    await dbUserUpdate2(env, userId, {
      thread_id: rec.thread_id,
      title: rec.title,
      closed: false
    });
    if (userId) {
      await dbThreadPut2(env, rec.thread_id, userId);
    }
  } else {
    await putWithMetadata2(env, key, rec, {
      expirationTtl: null,
      metadata: {
        userId: String(userId),
        threadId: res.result.message_thread_id
      }
    });
    if (userId) {
      await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
    }
  }
  return rec;
}
__name(createTopicImpl, "createTopicImpl");
async function updateThreadStatusImpl({
  threadId,
  isClosed,
  env,
  hasD1: hasD12,
  dbThreadGetUserId: dbThreadGetUserId2,
  dbUserGet: dbUserGet2,
  dbUserUpdate: dbUserUpdate2,
  dbThreadDelete: dbThreadDelete2,
  safeGetJSON: safeGetJSON2,
  getAllKeys: getAllKeys2,
  Logger: Logger2
}) {
  try {
    if (hasD12(env)) {
      const mappedUser2 = await dbThreadGetUserId2(env, threadId);
      if (mappedUser2) {
        const rec = await dbUserGet2(env, mappedUser2);
        if (rec && Number(rec.thread_id) === Number(threadId)) {
          await dbUserUpdate2(env, mappedUser2, { closed: isClosed });
          Logger2.info("thread_status_updated", { threadId, isClosed, updatedCount: 1 });
          return;
        }
        await dbThreadDelete2(env, threadId);
      }
      const result = await env.TG_BOT_DB.prepare("SELECT user_id FROM users WHERE thread_id = ?").bind(String(threadId)).all();
      const rows = result?.results || [];
      for (const row of rows) {
        await dbUserUpdate2(env, row.user_id, { closed: isClosed });
      }
      Logger2.info("thread_status_updated", { threadId, isClosed, updatedCount: rows.length });
      return;
    }
    const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
    if (mappedUser) {
      const userKey = `user:${mappedUser}`;
      const rec = await safeGetJSON2(env, userKey, null);
      if (rec && Number(rec.thread_id) === Number(threadId)) {
        rec.closed = isClosed;
        await env.TOPIC_MAP.put(userKey, JSON.stringify(rec));
        Logger2.info("thread_status_updated", { threadId, isClosed, updatedCount: 1 });
        return;
      }
      await env.TOPIC_MAP.delete(`thread:${threadId}`);
    }
    const allKeys = await getAllKeys2(env, "user:");
    const updates = [];
    for (const { name } of allKeys) {
      const rec = await safeGetJSON2(env, name, null);
      if (rec && Number(rec.thread_id) === Number(threadId)) {
        rec.closed = isClosed;
        updates.push(env.TOPIC_MAP.put(name, JSON.stringify(rec)));
      }
    }
    await Promise.all(updates);
    Logger2.info("thread_status_updated", { threadId, isClosed, updatedCount: updates.length });
  } catch (e) {
    Logger2.error("thread_status_update_failed", e, { threadId, isClosed });
    throw e;
  }
}
__name(updateThreadStatusImpl, "updateThreadStatusImpl");

// src/services/edit-sync.js
async function handleEditedMessageImpl({
  msg,
  env,
  hasD1: hasD12,
  dbMessageMapGet: dbMessageMapGet2,
  safeGetJSON: safeGetJSON2,
  dbUserGet: dbUserGet2,
  tgCall: tgCall2,
  Logger: Logger2
}) {
  if (msg.chat?.id == env.SUPERGROUP_ID) {
    const sourceChatId = msg.chat.id;
    const sourceMsgId = msg.message_id;
    const targetInfo = hasD12(env) ? await dbMessageMapGet2(env, sourceChatId, sourceMsgId) : await safeGetJSON2(env, `msg_map:${String(sourceChatId)}:${sourceMsgId}`, null);
    if (targetInfo) {
      const { targetChatId, targetMsgId } = targetInfo;
      try {
        if (msg.text) {
          await tgCall2(env, "editMessageText", {
            chat_id: targetChatId,
            message_id: targetMsgId,
            text: msg.text,
            entities: msg.entities,
            parse_mode: msg.parse_mode
          });
        } else if (msg.caption) {
          await tgCall2(env, "editMessageCaption", {
            chat_id: targetChatId,
            message_id: targetMsgId,
            caption: msg.caption,
            caption_entities: msg.caption_entities,
            parse_mode: msg.parse_mode
          });
        }
      } catch (error) {
        Logger2.warn("edit_message_forward_failed", {
          sourceChatId,
          sourceMsgId,
          targetChatId,
          targetMsgId,
          error: error.message
        });
      }
    }
  } else {
    const userId = msg.chat.id;
    const sourceMsgId = msg.message_id;
    const userRec = hasD12(env) ? await dbUserGet2(env, userId) : await safeGetJSON2(env, `user:${userId}`, null);
    if (!userRec || !userRec.thread_id) {
      return;
    }
    const targetInfo = hasD12(env) ? await dbMessageMapGet2(env, userId, sourceMsgId) : await safeGetJSON2(env, `msg_map:${String(userId)}:${sourceMsgId}`, null);
    if (targetInfo) {
      const { targetChatId, targetMsgId } = targetInfo;
      try {
        if (msg.text) {
          await tgCall2(env, "editMessageText", {
            chat_id: env.SUPERGROUP_ID,
            message_id: targetMsgId,
            message_thread_id: userRec.thread_id,
            text: msg.text,
            entities: msg.entities,
            parse_mode: msg.parse_mode
          });
        } else if (msg.caption) {
          await tgCall2(env, "editMessageCaption", {
            chat_id: env.SUPERGROUP_ID,
            message_id: targetMsgId,
            message_thread_id: userRec.thread_id,
            caption: msg.caption,
            caption_entities: msg.caption_entities,
            parse_mode: msg.parse_mode
          });
        }
      } catch (error) {
        Logger2.warn("edit_message_forward_failed", {
          sourceChatId: userId,
          sourceMsgId,
          targetChatId,
          targetMsgId,
          error: error.message
        });
      }
    }
  }
}
__name(handleEditedMessageImpl, "handleEditedMessageImpl");

// src/services/admin-reply.js
async function handleAdminReplyImpl(msg, env, ctx, deps) {
  const { isAdminUser: isAdminUser2, hasD1: hasD12, dbKeywordListWithId: dbKeywordListWithId2, tgCall: tgCall2, dbSetBanned: dbSetBanned2, dbThreadGetUserId: dbThreadGetUserId2, dbThreadPut: dbThreadPut2, getAllKeys: getAllKeys2, safeGetJSON: safeGetJSON2, dbKeywordAdd: dbKeywordAdd2, dbKeywordDelete: dbKeywordDelete2, dbKeywordDeleteById: dbKeywordDeleteById2, validateKeywordPattern: validateKeywordPattern2, CONFIG: CONFIG2, dbUserUpdate: dbUserUpdate2, dbSetVerifyState: dbSetVerifyState2, dbUserGet: dbUserGet2, dbGetVerifyState: dbGetVerifyState2, dbIsBanned: dbIsBanned2, handleMediaGroup: handleMediaGroup2, dbMessageMapPut: dbMessageMapPut2, handleCleanupCommand: handleCleanupCommand2 } = deps;
  const threadId = msg.message_thread_id;
  const text = (msg.text || "").trim();
  const senderId = msg.from?.id;
  const parts = text.split(/\s+/).filter(Boolean);
  const baseCmd = parts[0] || "";
  if (!senderId || !await isAdminUser2(env, senderId)) {
    return;
  }
  if (text === "/cleanup") {
    ctx.waitUntil(handleCleanupCommand2(threadId, env));
    return;
  }
  if (text === "/help") {
    const helpText = [
      "\u{1F6E0}\uFE0F **\u7BA1\u7406\u5458\u6307\u4EE4**",
      "",
      "/info - \u663E\u793A\u5F53\u524D\u7528\u6237\u4FE1\u606F",
      "/close - \u5173\u95ED\u5BF9\u8BDD",
      "/open - \u91CD\u65B0\u5F00\u542F\u5BF9\u8BDD",
      "/ban - \u5C01\u7981\u7528\u6237",
      "/unban - \u89E3\u5C01\u7528\u6237",
      "/trust - \u8BBE\u4E3A\u6C38\u4E45\u4FE1\u4EFB",
      "/reset - \u91CD\u7F6E\u9A8C\u8BC1\u72B6\u6001",
      "/cleanup - \u6E05\u7406\u5DF2\u5220\u9664\u8BDD\u9898\u6570\u636E",
      "/kw help - \u5173\u952E\u8BCD\u7BA1\u7406\u5E2E\u52A9"
    ].join("\n");
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: helpText, parse_mode: "Markdown" });
    return;
  }
  if (baseCmd === "/kw" && parts[1] === "list") {
    if (!hasD12(env)) {
      const warnText = "\u26A0\uFE0F \u5173\u952E\u8BCD\u529F\u80FD\u9700\u8981\u7ED1\u5B9A D1 \u6570\u636E\u5E93\u3002";
      const payload = { chat_id: env.SUPERGROUP_ID, text: warnText, parse_mode: "Markdown" };
      if (threadId) payload.message_thread_id = threadId;
      await tgCall2(env, "sendMessage", payload);
      return;
    }
    const list = await dbKeywordListWithId2(env);
    if (!list.length) {
      const payload = { chat_id: env.SUPERGROUP_ID, text: "\u5F53\u524D\u6682\u65E0\u5173\u952E\u8BCD\u3002" };
      if (threadId) payload.message_thread_id = threadId;
      await tgCall2(env, "sendMessage", payload);
      return;
    }
    const items = list.slice(0, 50).map((k, i) => `${i + 1}. [id=${k.id}] ${k.keyword}`);
    const header = "\u{1F4CC} \u5173\u952E\u8BCD\u5217\u8868";
    const maxLen = 3800;
    let buffer = `${header}

`;
    for (const line of items) {
      if (buffer.length + line.length + 1 > maxLen) {
        const payload = { chat_id: env.SUPERGROUP_ID, text: buffer.trimEnd() };
        if (threadId) payload.message_thread_id = threadId;
        await tgCall2(env, "sendMessage", payload);
        buffer = "";
      }
      buffer += (buffer ? "\n" : "") + line;
    }
    if (buffer.trim()) {
      const payload = { chat_id: env.SUPERGROUP_ID, text: buffer.trimEnd() };
      if (threadId) payload.message_thread_id = threadId;
      await tgCall2(env, "sendMessage", payload);
    }
    return;
  }
  if (baseCmd === "/ban" && parts[1] && /^\d+$/.test(parts[1])) {
    const targetUserId = Number(parts[1]);
    if (hasD12(env)) {
      await dbSetBanned2(env, targetUserId, true);
    } else {
      await env.TOPIC_MAP.put(`banned:${targetUserId}`, "1");
    }
    const payload = {
      chat_id: env.SUPERGROUP_ID,
      text: `\u{1F6AB} **\u7528\u6237\u5DF2\u5C01\u7981**
UID: \`${targetUserId}\``,
      parse_mode: "Markdown"
    };
    if (threadId) payload.message_thread_id = threadId;
    await tgCall2(env, "sendMessage", payload);
    return;
  }
  if (baseCmd === "/unban" && parts[1] && /^\d+$/.test(parts[1])) {
    const targetUserId = Number(parts[1]);
    if (hasD12(env)) {
      await dbSetBanned2(env, targetUserId, false);
    } else {
      await env.TOPIC_MAP.delete(`banned:${targetUserId}`);
    }
    const payload = {
      chat_id: env.SUPERGROUP_ID,
      text: `\u2705 **\u7528\u6237\u5DF2\u89E3\u5C01**
UID: \`${targetUserId}\``,
      parse_mode: "Markdown"
    };
    if (threadId) payload.message_thread_id = threadId;
    await tgCall2(env, "sendMessage", payload);
    return;
  }
  let userId = null;
  if (hasD12(env)) {
    const mappedUser = await dbThreadGetUserId2(env, threadId);
    if (mappedUser) {
      userId = Number(mappedUser);
    } else {
      const result = await env.TG_BOT_DB.prepare("SELECT user_id FROM users WHERE thread_id = ?").bind(String(threadId)).first();
      if (result?.user_id) {
        userId = Number(result.user_id);
        await dbThreadPut2(env, threadId, userId);
      }
    }
  } else {
    const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
    if (mappedUser) {
      userId = Number(mappedUser);
    } else {
      const allKeys = await getAllKeys2(env, "user:");
      for (const { name } of allKeys) {
        const rec = await safeGetJSON2(env, name, null);
        if (rec && Number(rec.thread_id) === Number(threadId)) {
          userId = Number(name.slice(5));
          break;
        }
      }
    }
  }
  if (!userId) return;
  if (text.startsWith("/kw")) {
    if (!hasD12(env)) {
      await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u26A0\uFE0F \u5173\u952E\u8BCD\u529F\u80FD\u9700\u8981\u7ED1\u5B9A D1 \u6570\u636E\u5E93\u3002", parse_mode: "Markdown" });
      return;
    }
    const parts2 = text.split(" ").filter(Boolean);
    const action = parts2[1] || "help";
    const subAction = parts2[2] || "";
    const restText = parts2.slice(2).join(" ").trim();
    if (action === "add") {
      if (!restText) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u7528\u6CD5\uFF1A`/kw add \u5173\u952E\u8BCD`", parse_mode: "Markdown" });
        return;
      }
      const validation = validateKeywordPattern2(restText);
      if (!validation.ok) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u274C \u5173\u952E\u8BCD\u89C4\u5219\u88AB\u62D2\u7EDD\uFF1A${validation.reason}`, parse_mode: "Markdown" });
        return;
      }
      await dbKeywordAdd2(env, restText);
      await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u2705 \u5DF2\u6DFB\u52A0\u5173\u952E\u8BCD\uFF1A\`${restText}\``, parse_mode: "Markdown" });
      return;
    }
    if (action === "del") {
      if (!restText) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u7528\u6CD5\uFF1A`/kw del \u5173\u952E\u8BCD` \u6216 `/kw del id <id>`", parse_mode: "Markdown" });
        return;
      }
      if (subAction === "id") {
        const idText = parts2[3];
        if (!idText || !/^\d+$/.test(idText)) {
          await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u7528\u6CD5\uFF1A`/kw del id <id>`", parse_mode: "Markdown" });
          return;
        }
        const changes2 = await dbKeywordDeleteById2(env, Number(idText));
        if (changes2 > 0) {
          await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u2705 \u5DF2\u5220\u9664\u5173\u952E\u8BCD\uFF08ID\uFF09\uFF1A\`${idText}\``, parse_mode: "Markdown" });
        } else {
          await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u274C \u672A\u627E\u5230\u5173\u952E\u8BCD\uFF08ID\uFF09\uFF1A\`${idText}\``, parse_mode: "Markdown" });
        }
        return;
      }
      const changes = await dbKeywordDelete2(env, restText);
      if (changes > 0) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u2705 \u5DF2\u5220\u9664\u5173\u952E\u8BCD\uFF1A\`${restText}\``, parse_mode: "Markdown" });
      } else {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u274C \u672A\u627E\u5230\u5173\u952E\u8BCD\uFF1A\`${restText}\``, parse_mode: "Markdown" });
      }
      return;
    }
    if (action === "list") {
      const list = await dbKeywordListWithId2(env);
      if (!list.length) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u5F53\u524D\u6682\u65E0\u5173\u952E\u8BCD\u3002" });
        return;
      }
      const items = list.slice(0, 50).map((k, i) => `${i + 1}. [id=${k.id}] ${k.keyword}`);
      const header = "\u{1F4CC} \u5173\u952E\u8BCD\u5217\u8868";
      const maxLen = 3800;
      let buffer = `${header}

`;
      for (const line of items) {
        if (buffer.length + line.length + 1 > maxLen) {
          await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: buffer.trimEnd() });
          buffer = "";
        }
        buffer += (buffer ? "\n" : "") + line;
      }
      if (buffer.trim()) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: buffer.trimEnd() });
      }
      return;
    }
    if (action === "test") {
      const rest = text.replace(/^\/kw\s+test\s+/i, "");
      const [pattern, ...textParts] = rest.split(" ");
      const sample = textParts.join(" ").trim();
      if (!pattern || !sample) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u7528\u6CD5\uFF1A`/kw test <\u8868\u8FBE\u5F0F> <\u6587\u672C>`", parse_mode: "Markdown" });
        return;
      }
      const validation = validateKeywordPattern2(pattern);
      if (!validation.ok) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u274C \u5173\u952E\u8BCD\u89C4\u5219\u88AB\u62D2\u7EDD\uFF1A${validation.reason}`, parse_mode: "Markdown" });
        return;
      }
      try {
        const re = new RegExp(pattern, "i");
        const matched = re.test(sample);
        const resultText = matched ? "\u2705 \u5339\u914D\u6210\u529F" : "\u274C \u672A\u547D\u4E2D";
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `${resultText}
\u8868\u8FBE\u5F0F\uFF1A\`${pattern}\`
\u6587\u672C\uFF1A\`${sample}\``, parse_mode: "Markdown" });
      } catch (e) {
        await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `\u274C \u6B63\u5219\u8BED\u6CD5\u9519\u8BEF\uFF1A\`${e.message}\``, parse_mode: "Markdown" });
      }
      return;
    }
    if (action === "help") {
      const helpText = [
        "\u{1F50E} **\u5173\u952E\u8BCD\u7BA1\u7406**",
        "",
        "/kw add \u5173\u952E\u8BCD - \u6DFB\u52A0\u5173\u952E\u8BCD",
        "/kw del \u5173\u952E\u8BCD - \u5220\u9664\u5173\u952E\u8BCD",
        "/kw del id <id> - \u6309 ID \u5220\u9664\u5173\u952E\u8BCD",
        "/kw list - \u67E5\u770B\u5173\u952E\u8BCD\u5217\u8868",
        "/kw test <\u8868\u8FBE\u5F0F> <\u6587\u672C> - \u6D4B\u8BD5\u6B63\u5219\u662F\u5426\u547D\u4E2D",
        "",
        "\u89C4\u5219\u9650\u5236\uFF1A",
        `1) \u5173\u952E\u8BCD\u957F\u5EA6\u4E0A\u9650 ${CONFIG2.KEYWORD_MAX_LENGTH} \u5B57\u7B26`,
        `2) \u8FC7\u6EE4\u4EC5\u5339\u914D\u524D ${CONFIG2.KEYWORD_MATCH_MAX_TEXT_LENGTH} \u5B57\u7B26`,
        "3) \u6B63\u5219\u9650\u5236\uFF1A",
        "- `.*` / `.+` \u51FA\u73B0\u8D85\u8FC7 2 \u6B21\u4F1A\u88AB\u62D2\u7EDD",
        "- \u5D4C\u5957\u91CF\u8BCD\u4F1A\u88AB\u62D2\u7EDD\uFF08\u5982 `(a+)+`\u3001`(.+)+`\u3001`(.+)*`\u3001`(.*)+`\uFF09",
        "- \u5F62\u5982 `(.*){2,}`\u3001`(.+){1,}` \u7684\u91CD\u590D\u7ED3\u6784\u4F1A\u88AB\u62D2\u7EDD"
      ].join("\n");
      await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: helpText, parse_mode: "Markdown" });
      return;
    }
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u7528\u6CD5\uFF1A`/kw add \u5173\u952E\u8BCD` / `/kw del \u5173\u952E\u8BCD` / `/kw del id <id>` / `/kw list` / `/kw test <\u8868\u8FBE\u5F0F> <\u6587\u672C>` / `/kw help`", parse_mode: "Markdown" });
    return;
  }
  if (text === "/close") {
    if (hasD12(env)) {
      await dbUserUpdate2(env, userId, { closed: true });
    } else {
      const key = `user:${userId}`;
      let rec = await safeGetJSON2(env, key, null);
      if (rec) {
        rec.closed = true;
        await env.TOPIC_MAP.put(key, JSON.stringify(rec));
      }
    }
    await tgCall2(env, "closeForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId });
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u{1F6AB} **\u5BF9\u8BDD\u5DF2\u5F3A\u5236\u5173\u95ED**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/open") {
    if (hasD12(env)) {
      await dbUserUpdate2(env, userId, { closed: false });
    } else {
      const key = `user:${userId}`;
      let rec = await safeGetJSON2(env, key, null);
      if (rec) {
        rec.closed = false;
        await env.TOPIC_MAP.put(key, JSON.stringify(rec));
      }
    }
    await tgCall2(env, "reopenForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId });
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u2705 **\u5BF9\u8BDD\u5DF2\u6062\u590D**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/reset") {
    if (hasD12(env)) {
      await dbSetVerifyState2(env, userId, null);
    } else {
      await env.TOPIC_MAP.delete(`verified:${userId}`);
    }
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u{1F504} **\u9A8C\u8BC1\u91CD\u7F6E**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/trust") {
    if (hasD12(env)) {
      await dbSetVerifyState2(env, userId, "trusted");
    } else {
      await env.TOPIC_MAP.put(`verified:${userId}`, "trusted");
    }
    await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u{1F31F} **\u5DF2\u8BBE\u7F6E\u6C38\u4E45\u4FE1\u4EFB**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/ban") {
    if (hasD12(env)) {
      await dbSetBanned2(env, userId, true);
    } else {
      await env.TOPIC_MAP.put(`banned:${userId}`, "1");
    }
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u{1F6AB} **\u7528\u6237\u5DF2\u5C01\u7981**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/unban") {
    if (hasD12(env)) {
      await dbSetBanned2(env, userId, false);
    } else {
      await env.TOPIC_MAP.delete(`banned:${userId}`);
    }
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "\u2705 **\u7528\u6237\u5DF2\u89E3\u5C01**", parse_mode: "Markdown" });
    return;
  }
  if (text === "/info") {
    const userRec = hasD12(env) ? await dbUserGet2(env, userId) : await safeGetJSON2(env, `user:${userId}`, null);
    const verifyStatus = hasD12(env) ? await dbGetVerifyState2(env, userId) : await env.TOPIC_MAP.get(`verified:${userId}`);
    const banStatus = hasD12(env) ? await dbIsBanned2(env, userId) : await env.TOPIC_MAP.get(`banned:${userId}`);
    const info = `\u{1F464} **\u7528\u6237\u4FE1\u606F**
UID: \`${userId}\`
Topic ID: \`${threadId}\`
\u8BDD\u9898\u6807\u9898: ${userRec?.title || "\u672A\u77E5"}
\u9A8C\u8BC1\u72B6\u6001: ${verifyStatus ? verifyStatus === "trusted" ? "\u{1F31F} \u6C38\u4E45\u4FE1\u4EFB" : "\u2705 \u5DF2\u9A8C\u8BC1" : "\u274C \u672A\u9A8C\u8BC1"}
\u5C01\u7981\u72B6\u6001: ${banStatus ? "\u{1F6AB} \u5DF2\u5C01\u7981" : "\u2705 \u6B63\u5E38"}
Link: [\u70B9\u51FB\u79C1\u804A](tg://user?id=${userId})`;
    await tgCall2(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: info, parse_mode: "Markdown" });
    return;
  }
  if (msg.media_group_id) {
    await handleMediaGroup2(msg, env, ctx, { direction: "t2p", targetChat: userId, threadId: void 0 });
    return;
  }
  const copyResult = await tgCall2(env, "copyMessage", {
    chat_id: userId,
    from_chat_id: env.SUPERGROUP_ID,
    message_id: msg.message_id
  });
  if (copyResult.ok) {
    if (hasD12(env)) {
      await dbMessageMapPut2(env, env.SUPERGROUP_ID, msg.message_id, userId, copyResult.result.message_id);
    } else {
      const mapKey = `msg_map:${String(env.SUPERGROUP_ID)}:${msg.message_id}`;
      const mapValue = JSON.stringify({
        targetChatId: String(userId),
        targetMsgId: copyResult.result.message_id,
        createdAt: Date.now()
      });
      await env.TOPIC_MAP.put(mapKey, mapValue, {
        expirationTtl: CONFIG2.MESSAGE_MAP_TTL_SECONDS
      });
    }
  }
}
__name(handleAdminReplyImpl, "handleAdminReplyImpl");

// src/services/message-flow.js
async function handlePrivateMessageImpl(msg, env, ctx, deps) {
  const { forwardToTopic: forwardToTopic2 } = deps;
  await forwardToTopic2(msg, env, ctx);
}
__name(handlePrivateMessageImpl, "handlePrivateMessageImpl");
async function forwardToTopicImpl(msg, env, ctx, deps) {
  const {
    checkRateLimit: checkRateLimit2,
    CONFIG: CONFIG2,
    tgCall: tgCall2,
    hasD1: hasD12,
    dbIsBanned: dbIsBanned2,
    dbGetVerifyState: dbGetVerifyState2,
    sendVerificationChallenge: sendVerificationChallenge2,
    getFilterText: getFilterText2,
    matchKeyword: matchKeyword2,
    Logger: Logger2,
    dbUserGet: dbUserGet2,
    safeGetJSON: safeGetJSON2,
    getOrCreateUserTopicRec: getOrCreateUserTopicRec2,
    sendWelcomeCard: sendWelcomeCard2,
    dbThreadGetUserId: dbThreadGetUserId2,
    dbThreadPut: dbThreadPut2,
    threadHealthCache: threadHealthCache2,
    probeForumThread: probeForumThread2,
    resetUserVerificationAndRequireReverify: resetUserVerificationAndRequireReverify2,
    handleMediaGroup: handleMediaGroup2,
    normalizeTgDescription: normalizeTgDescription2,
    isTopicMissingOrDeleted: isTopicMissingOrDeleted2,
    dbMessageMapPut: dbMessageMapPut2
  } = deps;
  const userId = msg.chat.id;
  const key = `user:${userId}`;
  const rateLimit = await checkRateLimit2(userId, env, "message", CONFIG2.RATE_LIMIT_MESSAGE, CONFIG2.RATE_LIMIT_WINDOW);
  if (!rateLimit.allowed) {
    await tgCall2(env, "sendMessage", {
      chat_id: userId,
      text: "\u26A0\uFE0F \u53D1\u9001\u8FC7\u4E8E\u9891\u7E41\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5\u3002"
    });
    return;
  }
  if (msg.text && msg.text.startsWith("/") && msg.text.trim() !== "/start") {
    return;
  }
  const isBanned = hasD12(env) ? await dbIsBanned2(env, userId) : await env.TOPIC_MAP.get(`banned:${userId}`);
  if (isBanned) return;
  const verified = hasD12(env) ? await dbGetVerifyState2(env, userId) : await env.TOPIC_MAP.get(`verified:${userId}`);
  if (!verified) {
    const isStart = msg.text && msg.text.trim() === "/start";
    const pendingMsgId = isStart ? null : msg.message_id;
    await sendVerificationChallenge2(userId, env, pendingMsgId);
    return;
  }
  const needsVerify = await env.TOPIC_MAP.get(`needs_verify:${userId}`);
  if (needsVerify) {
    await sendVerificationChallenge2(userId, env, msg.message_id || null);
    return;
  }
  const filterText = getFilterText2(msg);
  if (filterText) {
    const hitKeyword = await matchKeyword2(env, filterText);
    if (hitKeyword) {
      await tgCall2(env, "sendMessage", {
        chat_id: userId,
        text: "\u26A0\uFE0F \u8BE5\u6D88\u606F\u89E6\u53D1\u8FC7\u6EE4\u6761\u4EF6\uFF0C\u5DF2\u88AB\u62E6\u622A\u3002"
      });
      Logger2.info("keyword_blocked", { userId, keyword: hitKeyword });
      return;
    }
  }
  let rec = hasD12(env) ? await dbUserGet2(env, userId) : await safeGetJSON2(env, key, null);
  if (rec && rec.closed) {
    await tgCall2(env, "sendMessage", { chat_id: userId, text: "\u{1F6AB} \u5F53\u524D\u5BF9\u8BDD\u5DF2\u88AB\u7BA1\u7406\u5458\u5173\u95ED\u3002" });
    return;
  }
  const retryKey = `retry:${userId}`;
  let retryCount = parseInt(await env.TOPIC_MAP.get(retryKey) || "0");
  if (retryCount > CONFIG2.MAX_RETRY_ATTEMPTS) {
    await tgCall2(env, "sendMessage", {
      chat_id: userId,
      text: "\u274C \u7CFB\u7EDF\u7E41\u5FD9\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5\u3002"
    });
    await env.TOPIC_MAP.delete(retryKey);
    return;
  }
  if (!rec || !rec.thread_id) {
    rec = await getOrCreateUserTopicRec2(msg.from, key, env, userId);
    if (!rec || !rec.thread_id) {
      throw new Error("\u521B\u5EFA\u8BDD\u9898\u5931\u8D25");
    }
    await sendWelcomeCard2(env, rec.thread_id, userId, msg.from);
  }
  if (rec && rec.thread_id) {
    if (hasD12(env)) {
      const mappedUser = await dbThreadGetUserId2(env, rec.thread_id);
      if (!mappedUser) {
        await dbThreadPut2(env, rec.thread_id, userId);
      }
    } else {
      const mappedUser = await env.TOPIC_MAP.get(`thread:${rec.thread_id}`);
      if (!mappedUser) {
        await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
      }
    }
  }
  if (rec && rec.thread_id) {
    const cacheKey = rec.thread_id;
    const now = Date.now();
    const cached = threadHealthCache2.get(cacheKey);
    const withinTTL = cached && now - cached.ts < CONFIG2.THREAD_HEALTH_TTL_MS;
    if (!withinTTL) {
      const kvHealthKey = `thread_ok:${rec.thread_id}`;
      const kvHealthOk = await env.TOPIC_MAP.get(kvHealthKey);
      if (kvHealthOk === "1") {
        threadHealthCache2.set(cacheKey, { ts: now, ok: true });
      } else {
        const probe = await probeForumThread2(env, rec.thread_id, { userId, reason: "health_check" });
        if (probe.status === "redirected" || probe.status === "missing" || probe.status === "missing_thread_id") {
          await resetUserVerificationAndRequireReverify2(env, {
            userId,
            userKey: key,
            oldThreadId: rec.thread_id,
            pendingMsgId: msg.message_id,
            reason: `health_check:${probe.status}`,
            userFrom: msg.from
          });
          return;
        } else if (probe.status === "probe_invalid") {
          Logger2.warn("topic_health_probe_invalid_message", {
            userId,
            threadId: rec.thread_id,
            errorDescription: probe.description
          });
          threadHealthCache2.set(cacheKey, { ts: now, ok: true });
          await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG2.THREAD_HEALTH_TTL_MS / 1e3) });
        } else if (probe.status === "unknown_error") {
          Logger2.warn("topic_test_failed_unknown", {
            userId,
            threadId: rec.thread_id,
            errorDescription: probe.description
          });
        } else {
          await env.TOPIC_MAP.delete(retryKey);
          threadHealthCache2.set(cacheKey, { ts: now, ok: true });
          await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG2.THREAD_HEALTH_TTL_MS / 1e3) });
        }
      }
    }
  }
  if (msg.media_group_id) {
    await handleMediaGroup2(msg, env, ctx, {
      direction: "p2t",
      targetChat: env.SUPERGROUP_ID,
      threadId: rec.thread_id
    });
    return;
  }
  const copyResult = await tgCall2(env, "copyMessage", {
    chat_id: env.SUPERGROUP_ID,
    from_chat_id: userId,
    message_id: msg.message_id,
    message_thread_id: rec.thread_id
  });
  const resThreadId = copyResult.result?.message_thread_id;
  if (copyResult.ok && resThreadId !== void 0 && resThreadId !== null && Number(resThreadId) !== Number(rec.thread_id)) {
    Logger2.warn("forward_redirected_to_general", {
      userId,
      expectedThreadId: rec.thread_id,
      actualThreadId: resThreadId
    });
    if (copyResult.result?.message_id) {
      try {
        await tgCall2(env, "deleteMessage", {
          chat_id: env.SUPERGROUP_ID,
          message_id: copyResult.result.message_id
        });
      } catch (e) {
      }
    }
    await resetUserVerificationAndRequireReverify2(env, {
      userId,
      userKey: key,
      oldThreadId: rec.thread_id,
      pendingMsgId: msg.message_id,
      reason: "forward_redirected_to_general",
      userFrom: msg.from
    });
    return;
  }
  if (copyResult.ok && (resThreadId === void 0 || resThreadId === null)) {
    const probe = await probeForumThread2(env, rec.thread_id, { userId, reason: "forward_result_missing_thread_id" });
    if (probe.status !== "ok") {
      Logger2.warn("forward_suspected_redirect_or_missing", {
        userId,
        expectedThreadId: rec.thread_id,
        probeStatus: probe.status,
        probeDescription: probe.description
      });
      if (copyResult.result?.message_id) {
        try {
          await tgCall2(env, "deleteMessage", {
            chat_id: env.SUPERGROUP_ID,
            message_id: copyResult.result.message_id
          });
        } catch (e) {
        }
      }
      await resetUserVerificationAndRequireReverify2(env, {
        userId,
        userKey: key,
        oldThreadId: rec.thread_id,
        pendingMsgId: msg.message_id,
        reason: `forward_missing_thread_id:${probe.status}`,
        userFrom: msg.from
      });
      return;
    }
  }
  if (!copyResult.ok) {
    const desc = normalizeTgDescription2(copyResult.description);
    if (isTopicMissingOrDeleted2(desc)) {
      Logger2.warn("forward_failed_topic_missing", {
        userId,
        threadId: rec.thread_id,
        errorDescription: copyResult.description
      });
      await resetUserVerificationAndRequireReverify2(env, {
        userId,
        userKey: key,
        oldThreadId: rec.thread_id,
        pendingMsgId: msg.message_id,
        reason: "forward_failed_topic_missing",
        userFrom: msg.from
      });
      return;
    }
    if (desc.includes("chat not found")) throw new Error(`\u7FA4\u7EC4ID\u9519\u8BEF: ${env.SUPERGROUP_ID}`);
    if (desc.includes("not enough rights")) throw new Error("\u673A\u5668\u4EBA\u6743\u9650\u4E0D\u8DB3 (\u9700 Manage Topics)");
    await tgCall2(env, "sendMessage", {
      chat_id: userId,
      text: "\u274C \u6D88\u606F\u53D1\u9001\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5\u3002"
    });
    return;
  }
  if (hasD12(env)) {
    await dbMessageMapPut2(env, userId, msg.message_id, env.SUPERGROUP_ID, copyResult.result.message_id);
  } else {
    const mapKey = `msg_map:${String(userId)}:${msg.message_id}`;
    const mapValue = JSON.stringify({
      targetChatId: String(env.SUPERGROUP_ID),
      targetMsgId: copyResult.result.message_id,
      createdAt: Date.now()
    });
    await env.TOPIC_MAP.put(mapKey, mapValue, {
      expirationTtl: CONFIG2.MESSAGE_MAP_TTL_SECONDS
    });
  }
}
__name(forwardToTopicImpl, "forwardToTopicImpl");

// src/adapters/storage-kv.js
async function safeGetJSON(env, key, defaultValue = null) {
  try {
    const data = await env.TOPIC_MAP.get(key, { type: "json" });
    if (data === null || data === void 0) {
      return defaultValue;
    }
    if (typeof data !== "object") {
      Logger.warn("kv_invalid_type", { key, type: typeof data });
      return defaultValue;
    }
    return data;
  } catch (e) {
    Logger.error("kv_parse_failed", e, { key });
    return defaultValue;
  }
}
__name(safeGetJSON, "safeGetJSON");
async function getAllKeys(env, prefix = "", limit = null) {
  const allKeys = [];
  let cursor = void 0;
  let count = 0;
  do {
    const result = await env.TOPIC_MAP.list({ prefix, cursor });
    for (const key of result.keys) {
      if (limit && count >= limit) break;
      allKeys.push(key);
      count++;
    }
    if (limit && count >= limit) break;
    cursor = result.list_complete ? void 0 : result.cursor;
  } while (cursor);
  return allKeys;
}
__name(getAllKeys, "getAllKeys");
async function putWithMetadata(env, key, value, options = {}) {
  const {
    expirationTtl = null,
    metadata = {}
  } = options;
  const finalMetadata = {
    updatedAt: Date.now(),
    ...metadata,
    createdAt: metadata.createdAt || Date.now()
  };
  const putOptions = {
    metadata: finalMetadata
  };
  if (expirationTtl) putOptions.expirationTtl = expirationTtl;
  try {
    await env.TOPIC_MAP.put(key, JSON.stringify(value), putOptions);
  } catch (e) {
    Logger.error("kv_put_with_metadata_failed", e, { key });
    throw e;
  }
}
__name(putWithMetadata, "putWithMetadata");
async function deleteBulk(env, keys) {
  if (!keys || keys.length === 0) return 0;
  try {
    const deletePromises = keys.map(
      (key) => env.TOPIC_MAP.delete(key).catch((e) => {
        Logger.warn("kv_delete_failed", { key, error: e.message });
      })
    );
    await Promise.all(deletePromises);
    return keys.length;
  } catch (e) {
    Logger.error("kv_bulk_delete_failed", e, { keyCount: keys.length });
    return 0;
  }
}
__name(deleteBulk, "deleteBulk");

// src/handlers/webhook.js
function createWebhookFetchHandler({
  Logger: Logger2,
  tgCall: tgCall2,
  flushExpiredMediaGroups: flushExpiredMediaGroups2,
  handleEditedMessage: handleEditedMessage2,
  handleCallbackQuery: handleCallbackQuery2,
  handlePrivateMessage: handlePrivateMessage2,
  updateThreadStatus: updateThreadStatus2,
  handleAdminReply: handleAdminReply2
}) {
  return /* @__PURE__ */ __name(async function fetch2(request, env, ctx) {
    if (!env.TOPIC_MAP) return new Response("Error: KV 'TOPIC_MAP' not bound.");
    if (!env.TG_BOT_DB) return new Response("Error: D1 'TG_BOT_DB' not bound.");
    if (!env.BOT_TOKEN) return new Response("Error: BOT_TOKEN not set.");
    if (!env.SUPERGROUP_ID) return new Response("Error: SUPERGROUP_ID not set.");
    const normalizedEnv = {
      ...env,
      SUPERGROUP_ID: String(env.SUPERGROUP_ID),
      BOT_TOKEN: String(env.BOT_TOKEN)
    };
    if (!normalizedEnv.SUPERGROUP_ID.startsWith("-100")) {
      return new Response("Error: SUPERGROUP_ID must start with -100");
    }
    if (request.method !== "POST") return new Response("OK");
    const contentType = request.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
      Logger2.warn("invalid_content_type", { contentType });
      return new Response("OK");
    }
    let update;
    try {
      update = await request.json();
      if (!update || typeof update !== "object") {
        Logger2.warn("invalid_json_structure", { update: typeof update });
        return new Response("OK");
      }
    } catch (e) {
      Logger2.error("json_parse_failed", e);
      return new Response("OK");
    }
    if (update.edited_message) {
      await handleEditedMessage2(update.edited_message, normalizedEnv, ctx);
      return new Response("OK");
    }
    if (update.callback_query) {
      await handleCallbackQuery2(update.callback_query, normalizedEnv, ctx);
      return new Response("OK");
    }
    const msg = update.message;
    if (!msg) return new Response("OK");
    ctx.waitUntil(flushExpiredMediaGroups2(normalizedEnv, Date.now()));
    if (msg.chat && msg.chat.type === "private") {
      try {
        await handlePrivateMessage2(msg, normalizedEnv, ctx);
      } catch (e) {
        const errText = "\u26A0\uFE0F \u7CFB\u7EDF\u7E41\u5FD9\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5\u3002";
        await tgCall2(normalizedEnv, "sendMessage", { chat_id: msg.chat.id, text: errText });
        Logger2.error("private_message_failed", e, { userId: msg.chat.id });
      }
      return new Response("OK");
    }
    if (msg.chat && String(msg.chat.id) === normalizedEnv.SUPERGROUP_ID) {
      if (msg.forum_topic_closed && msg.message_thread_id) {
        await updateThreadStatus2(msg.message_thread_id, true, normalizedEnv);
        return new Response("OK");
      }
      if (msg.forum_topic_reopened && msg.message_thread_id) {
        await updateThreadStatus2(msg.message_thread_id, false, normalizedEnv);
        return new Response("OK");
      }
      const text = (msg.text || "").trim();
      const isCommand = !!text && text.startsWith("/");
      if (msg.message_thread_id || isCommand) {
        await handleAdminReply2(msg, normalizedEnv, ctx);
        return new Response("OK");
      }
    }
    return new Response("OK");
  }, "fetch");
}
__name(createWebhookFetchHandler, "createWebhookFetchHandler");

// src/app.js
var threadHealthCache = /* @__PURE__ */ new Map();
var topicCreateInFlight = /* @__PURE__ */ new Map();
async function getOrCreateUserTopicRec(from, key, env, userId) {
  return getOrCreateUserTopicRecImpl({
    from,
    key,
    env,
    userId,
    hasD1,
    dbUserGet,
    safeGetJSON,
    createTopic,
    topicCreateInFlight
  });
}
__name(getOrCreateUserTopicRec, "getOrCreateUserTopicRec");
async function resetUserVerificationAndRequireReverify(env, { userId, userKey, oldThreadId, pendingMsgId, reason, userFrom = null }) {
  return resetUserVerificationAndRequireReverifyImpl({
    env,
    userId,
    userKey,
    oldThreadId,
    pendingMsgId,
    reason,
    hasD1,
    dbUserUpdate,
    dbThreadDelete,
    CONFIG,
    threadHealthCache,
    Logger,
    sendVerificationChallenge
  });
}
__name(resetUserVerificationAndRequireReverify, "resetUserVerificationAndRequireReverify");
function shuffleArray(arr) {
  const array = [...arr];
  for (let i = array.length - 1; i > 0; i--) {
    const j = secureRandomInt(0, i + 1);
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}
__name(shuffleArray, "shuffleArray");
var fetchHandler = createWebhookFetchHandler({
  Logger,
  tgCall,
  flushExpiredMediaGroups,
  handleEditedMessage,
  handleCallbackQuery,
  handlePrivateMessage,
  updateThreadStatus,
  handleAdminReply
});
var app_default = {
  fetch: fetchHandler
};
async function handlePrivateMessage(msg, env, ctx) {
  return handlePrivateMessageImpl(msg, env, ctx, {
    forwardToTopic
  });
}
__name(handlePrivateMessage, "handlePrivateMessage");
async function forwardToTopic(msg, env, ctx) {
  return forwardToTopicImpl(msg, env, ctx, {
    checkRateLimit,
    CONFIG,
    tgCall,
    hasD1,
    dbIsBanned,
    dbGetVerifyState,
    sendVerificationChallenge,
    getFilterText,
    matchKeyword,
    Logger,
    dbUserGet,
    safeGetJSON,
    getOrCreateUserTopicRec,
    sendWelcomeCard,
    dbThreadGetUserId,
    dbThreadPut,
    threadHealthCache,
    probeForumThread,
    resetUserVerificationAndRequireReverify,
    handleMediaGroup,
    normalizeTgDescription,
    isTopicMissingOrDeleted,
    dbMessageMapPut
  });
}
__name(forwardToTopic, "forwardToTopic");
async function handleAdminReply(msg, env, ctx) {
  return handleAdminReplyImpl(msg, env, ctx, {
    isAdminUser,
    hasD1,
    dbKeywordListWithId,
    tgCall,
    dbSetBanned,
    dbThreadGetUserId,
    dbThreadPut,
    getAllKeys,
    safeGetJSON,
    dbKeywordAdd,
    dbKeywordDelete,
    dbKeywordDeleteById,
    validateKeywordPattern,
    CONFIG,
    dbUserUpdate,
    dbSetVerifyState,
    dbUserGet,
    dbGetVerifyState,
    dbIsBanned,
    handleMediaGroup,
    dbMessageMapPut,
    handleCleanupCommand
  });
}
__name(handleAdminReply, "handleAdminReply");
async function sendVerificationChallenge(userId, env, pendingMsgId) {
  return sendVerificationChallengeImpl({
    userId,
    env,
    pendingMsgId,
    safeGetJSON,
    checkRateLimit,
    tgCall,
    Logger,
    CONFIG,
    LOCAL_QUESTIONS,
    secureRandomInt,
    shuffleArray,
    secureRandomId
  });
}
__name(sendVerificationChallenge, "sendVerificationChallenge");
async function handleCallbackQuery(query, env, ctx) {
  return handleCallbackQueryImpl({
    query,
    env,
    ctx,
    tgCall,
    Logger,
    hasD1,
    dbSetVerifyState,
    CONFIG,
    forwardToTopic
  });
}
__name(handleCallbackQuery, "handleCallbackQuery");
async function handleCleanupCommand(threadId, env) {
  return handleCleanupCommandImpl({
    threadId,
    env,
    CONFIG,
    hasD1,
    dbListUsers,
    probeForumThread,
    resetUserVerificationAndRequireReverify,
    Logger,
    safeGetJSON,
    deleteBulk,
    tgCall,
    withMessageThreadId
  });
}
__name(handleCleanupCommand, "handleCleanupCommand");
async function createTopic(from, key, env, userId) {
  return createTopicImpl({
    from,
    key,
    env,
    userId,
    CONFIG,
    tgCall,
    hasD1,
    dbUserUpdate,
    dbThreadPut,
    putWithMetadata,
    buildTopicTitle
  });
}
__name(createTopic, "createTopic");
async function updateThreadStatus(threadId, isClosed, env) {
  return updateThreadStatusImpl({
    threadId,
    isClosed,
    env,
    hasD1,
    dbThreadGetUserId,
    dbUserGet,
    dbUserUpdate,
    dbThreadDelete,
    safeGetJSON,
    getAllKeys,
    Logger
  });
}
__name(updateThreadStatus, "updateThreadStatus");
function buildTopicTitle(from) {
  return buildTopicTitleImpl(from, CONFIG);
}
__name(buildTopicTitle, "buildTopicTitle");
async function handleMediaGroup(msg, env, ctx, { direction, targetChat, threadId }) {
  return handleMediaGroupImpl({
    msg,
    env,
    ctx,
    direction,
    targetChat,
    threadId,
    tgCall,
    withMessageThreadId,
    safeGetJSON,
    delaySend,
    CONFIG
  });
}
__name(handleMediaGroup, "handleMediaGroup");
async function flushExpiredMediaGroups(env, now) {
  return flushExpiredMediaGroupsImpl({ env, now, getAllKeys, safeGetJSON, Logger });
}
__name(flushExpiredMediaGroups, "flushExpiredMediaGroups");
async function delaySend(env, key, ts) {
  return delaySendImpl({
    env,
    key,
    ts,
    CONFIG,
    safeGetJSON,
    Logger,
    tgCall,
    withMessageThreadId
  });
}
__name(delaySend, "delaySend");
async function handleEditedMessage(msg, env, ctx) {
  return handleEditedMessageImpl({
    msg,
    env,
    hasD1,
    dbMessageMapGet,
    safeGetJSON,
    dbUserGet,
    tgCall,
    Logger
  });
}
__name(handleEditedMessage, "handleEditedMessage");

// main.js
var main_default = app_default;
export {
  RateLimitDO,
  main_default as default
};
//# sourceMappingURL=main.js.map
