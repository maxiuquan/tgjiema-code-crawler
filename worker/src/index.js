/**
 * Cloudflare Worker — Bot 覆盖规则 API
 * 提供 RESTful CRUD 接口，数据存储在 Cloudflare D1
 *
 * 部署步骤:
 *   1. wrangler d1 create bot-overrides-db
 *      将输出的 database_id 填入 wrangler.toml
 *   2. wrangler d1 execute bot-overrides-db --file=schema.sql
 *   3. wrangler secret put AUTH_TOKEN
 *   4. wrangler deploy
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS 预检
    if (request.method === "OPTIONS") {
      return corsResponse(new Response(null, { status: 204 }), env);
    }

    // 鉴权 (Bearer Token) — 除了健康检查外的所有接口
    if (path !== "/" && path !== "/health") {
      const auth = request.headers.get("Authorization") || "";
      const token = auth.replace(/^Bearer\s+/i, "");
      const expected = env.AUTH_TOKEN || "";
      if (!expected || token !== expected) {
        return corsResponse(
          new Response(JSON.stringify({ error: "unauthorized" }), {
            status: 401,
            headers: { "Content-Type": "application/json" },
          }),
          env
        );
      }
    }

    // 路由
    try {
      switch (true) {
        // GET / — 健康检查
        case path === "/" || path === "/health":
          return corsResponse(
            new Response(JSON.stringify({ status: "ok", service: "bot-override-api" }), {
              status: 200,
              headers: { "Content-Type": "application/json" },
            }),
            env
          );

        // GET /api/overrides — 列表
        case path === "/api/overrides" && request.method === "GET":
          return handleList(env);

        // POST /api/overrides — 添加/更新
        case path === "/api/overrides" && request.method === "POST":
          return handleAdd(env, request);

        // DELETE /api/overrides?prefix=xxx — 删除
        case path === "/api/overrides" && request.method === "DELETE":
          return handleRemove(env, url);

        // PATCH /api/overrides/toggle?prefix=xxx — 开关
        case path === "/api/overrides/toggle" && request.method === "PATCH":
          return handleToggle(env, url);

        // GET /api/overrides/match?code=xxx — 匹配查询
        case path === "/api/overrides/match" && request.method === "GET":
          return handleMatch(env, url);

        default:
          return corsResponse(
            new Response(JSON.stringify({ error: "not_found" }), {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }),
            env
          );
      }
    } catch (e) {
      return corsResponse(
        new Response(JSON.stringify({ error: "internal_error", detail: e.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }),
        env
      );
    }
  },
};

// ─── CORS ────────────────────────────────────────

function corsResponse(response, env) {
  const origin = env.ALLOWED_ORIGIN || "*";
  response.headers.set("Access-Control-Allow-Origin", origin);
  response.headers.set("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS");
  response.headers.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  response.headers.set("Access-Control-Max-Age", "86400");
  return response;
}

// ─── 列表 ────────────────────────────────────────

async function handleList(env) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM bot_overrides ORDER BY is_active DESC, created_at DESC"
  ).all();
  return corsResponse(
    new Response(JSON.stringify({ overrides: results }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
    env
  );
}

// ─── 添加/更新 ────────────────────────────────────

async function handleAdd(env, request) {
  const body = await request.json();
  const { code_prefix, override_bot_username, note = "" } = body || {};

  if (!code_prefix || !override_bot_username) {
    return corsResponse(
      new Response(JSON.stringify({ error: "code_prefix and override_bot_username are required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
      env
    );
  }

  await env.DB.prepare(
    `INSERT INTO bot_overrides (code_prefix, override_bot_username, is_active, created_at, note)
     VALUES (?1, ?2, 1, datetime('now'), ?3)
     ON CONFLICT(code_prefix) DO UPDATE SET
       override_bot_username = excluded.override_bot_username,
       is_active = 1,
       updated_at = datetime('now'),
       note = excluded.note`
  )
    .bind(code_prefix, override_bot_username, note)
    .run();

  return corsResponse(
    new Response(JSON.stringify({ ok: true, code_prefix, override_bot_username }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
    env
  );
}

// ─── 删除 ────────────────────────────────────────

async function handleRemove(env, url) {
  const prefix = url.searchParams.get("prefix");
  if (!prefix) {
    return corsResponse(
      new Response(JSON.stringify({ error: "prefix is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
      env
    );
  }

  const { meta } = await env.DB.prepare(
    "DELETE FROM bot_overrides WHERE code_prefix = ?1"
  )
    .bind(prefix)
    .run();

  return corsResponse(
    new Response(JSON.stringify({ ok: true, deleted: meta.changes > 0 }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
    env
  );
}

// ─── 开关 ────────────────────────────────────────

async function handleToggle(env, url) {
  const prefix = url.searchParams.get("prefix");
  if (!prefix) {
    return corsResponse(
      new Response(JSON.stringify({ error: "prefix is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
      env
    );
  }

  const { meta } = await env.DB.prepare(
    `UPDATE bot_overrides
     SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END,
         updated_at = datetime('now')
     WHERE code_prefix = ?1`
  )
    .bind(prefix)
    .run();

  if (meta.changes === 0) {
    return corsResponse(
      new Response(JSON.stringify({ error: "not_found" }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      }),
      env
    );
  }

  const row = await env.DB.prepare(
    "SELECT is_active FROM bot_overrides WHERE code_prefix = ?1"
  )
    .bind(prefix)
    .first();

  return corsResponse(
    new Response(JSON.stringify({ ok: true, code_prefix: prefix, is_active: row.is_active }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
    env
  );
}

// ─── 前缀匹配 ─────────────────────────────────────

async function handleMatch(env, url) {
  const code = url.searchParams.get("code");
  if (!code) {
    return corsResponse(
      new Response(JSON.stringify({ error: "code is required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      }),
      env
    );
  }

  // 最长前缀匹配
  const { results } = await env.DB.prepare(
    `SELECT * FROM bot_overrides
     WHERE is_active = 1 AND ?1 LIKE (code_prefix || '%')
     ORDER BY LENGTH(code_prefix) DESC
     LIMIT 1`
  )
    .bind(code)
    .all();

  return corsResponse(
    new Response(JSON.stringify({ override: results[0] || null }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
    env
  );
}