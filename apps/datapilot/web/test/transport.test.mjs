import assert from "node:assert/strict";
import test from "node:test";
import { accessDenied, ApiClient, ApiError } from "../src/api.ts";

const user = { user_id: "user-unit", username: "unit", role: "analyst_east", effective_scope: "east",
  authz_version: 1, capabilities: ["queries:read_self"] };
const json = (body, status = 200) => new Response(JSON.stringify({ request_id: "unit-request", ...body }), {
  status, headers: { "Content-Type": "application/json" },
});
function fetchDouble(context, implementation) {
  const previous = globalThis.fetch;
  context.after(() => { globalThis.fetch = previous; });
  globalThis.fetch = implementation;
}

test("COM-T002 login rotates CSRF and mutation uses same-origin cookies without hidden retries", async (t) => {
  let mutationCalls = 0;
  fetchDouble(t, async (path, options) => {
    assert.equal(options.credentials, "same-origin");
    if (path === "/api/v1/auth/csrf") return json({ csrf_token: "anonymous-test-only" });
    if (path === "/api/v1/auth/login") {
      assert.equal(options.headers["X-CSRF-Token"], "anonymous-test-only");
      return json({ user, csrf_token: "authenticated-test-only" });
    }
    assert.equal(path, "/api/v1/queries");
    mutationCalls += 1;
    assert.equal(options.headers["X-CSRF-Token"], "authenticated-test-only");
    assert.equal(options.headers["Idempotency-Key"], "retained-command-key");
    return json({ code: "QUEUE_FULL", message: "Queue full", retryable: true, details: {} }, 429);
  });
  const api = new ApiClient(() => assert.fail("429 is not a sign-out"));
  await api.login("unit", "synthetic-test-password");
  await assert.rejects(api.post("/api/v1/queries", { question: "unit-only" }, "retained-command-key"),
    (error) => error instanceof ApiError && error.status === 429 && error.requestId === "unit-request");
  assert.equal(mutationCalls, 1);
});

test("COM-T042 a response from a previous session cannot repopulate the cleared cache", async (t) => {
  let release;
  fetchDouble(t, () => new Promise((resolve) => { release = resolve; }));
  const api = new ApiClient(() => undefined);
  const pending = api.get("/api/v1/queries/old-owner-query");
  api.reset();
  release(json({ private_result: "must-not-return" }));
  await assert.rejects(pending, (error) => error instanceof ApiError && error.code === "SESSION_CHANGED");
});

test("COM-T042 current /me authorization change fences already pending resources", async (t) => {
  let meCalls = 0, release;
  fetchDouble(t, (path) => {
    if (path === "/api/v1/me") return Promise.resolve(json({ user: { ...user, authz_version: ++meCalls } }));
    return new Promise((resolve) => { release = resolve; });
  });
  const api = new ApiClient(() => undefined);
  await api.me();
  const pending = api.get("/api/v1/catalog/metrics");
  await api.me();
  release(json({ private_definition: "must-not-return" }));
  await assert.rejects(pending, (error) => error instanceof ApiError && error.code === "SESSION_CHANGED");
});

test("COM-T045 non-JSON dependency errors do not expose debug response bodies", async (t) => {
  fetchDouble(t, async () => new Response("INTERNAL-DEBUG-PATH-MUST-NOT-DISPLAY", { status: 503 }));
  const api = new ApiClient(() => undefined);
  await assert.rejects(api.get("/api/v1/history"), (error) => {
    assert.equal(error.code, "HTTP_503");
    assert.doesNotMatch(error.message, /INTERNAL-DEBUG/);
    return true;
  });
});

test("COM-T042 forbidden capability refreshes authorization while scope validation preserves identity", async (t) => {
  let revoked = 0, refreshed = 0, calls = 0;
  fetchDouble(t, async () => json({ code: calls++ === 0 ? "FORBIDDEN" : "FORBIDDEN_SCOPE",
    message: "Forbidden", retryable: false, details: {} }, 403));
  const api = new ApiClient(() => { revoked += 1; }, () => { refreshed += 1; });
  await assert.rejects(api.get("/api/v1/catalog/metrics"), (error) => accessDenied(error));
  await assert.rejects(api.get("/api/v1/catalog/metrics"), (error) => accessDenied(error));
  assert.equal(refreshed, 1);
  assert.equal(revoked, 0);
});

test("COM-T004 an actual current-session 401 invalidates identity once", async (t) => {
  let revoked = 0;
  fetchDouble(t, async () => json({ code: "AUTH_REQUIRED", message: "Session ended", retryable: false, details: {} }, 401));
  const api = new ApiClient(() => { revoked += 1; });
  await assert.rejects(api.get("/api/v1/history"), (error) => accessDenied(error));
  assert.equal(revoked, 1);
});
