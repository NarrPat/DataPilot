/** Run the actual bundled transport source, with only its import URL resolved.
 * This matches Vite's TS loading without introducing a product test hook.
 */
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import ts from "typescript";

function compiled(source) {
  return ts.transpileModule(source, { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext,
  } }).outputText;
}
const uri = (source) => `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
const state = uri(compiled(await readFile(new URL("../src/state.ts", import.meta.url), "utf8")));
const source = compiled(await readFile(new URL("../src/events.ts", import.meta.url), "utf8"));
assert.ok(source.includes('from "./state"'));
const { subscribeEvents } = await import(uri(source.replace('from "./state"', `from "${state}"`)));

const queryId = "a0be84c4-34be-40ab-9766-e68922a50103";
const initial = { seq: 3, statusVersion: 3, specVersion: 1 };
const snapshot = (overrides = {}) => ({ query_id: queryId, status: "parsing",
  last_event_seq: 8, status_version: 5, spec_version: 1, ...overrides });
function event(major = "1") {
  return new Response(`id: 4\nevent: query.status\ndata: ${JSON.stringify({
    schema_version: major, app: "datapilot", query_id: queryId,
    seq: 4, status_version: 4, spec_version: 1, type: "query.status", payload: {},
  })}\n\n`, { headers: { "Content-Type": "text/event-stream" } });
}
function setup(t, fetcher) {
  const previous = { fetch: globalThis.fetch, window: globalThis.window };
  const controller = new AbortController();
  globalThis.fetch = fetcher;
  globalThis.window = globalThis;
  t.after(() => { controller.abort(); Object.assign(globalThis, previous); });
  return controller;
}
async function bounded(controller, callbacks) {
  let timer;
  try {
    return await Promise.race([
      subscribeEvents(queryId, initial, callbacks, controller.signal).then(() => "returned"),
      new Promise((resolve) => { timer = setTimeout(() => { controller.abort(); resolve("deadline"); }, 120); }),
    ]);
  } finally { clearTimeout(timer); }
}

test("COM-T035 unknown major stops even when its snapshot refresh fails", async (t) => {
  let fetches = 0, reads = 0;
  const controller = setup(t, async () => { fetches += 1; return event("2"); });
  const states = [];
  assert.equal(await bounded(controller, { status: (v) => states.push(v), revoked: assert.fail,
    refresh: async () => { reads += 1; throw new Error("realistic HTTP failure"); } }), "returned");
  assert.equal(fetches, 1);
  assert.equal(reads, 1);
  assert.equal(states.at(-1), "unsupported");
  assert.ok(!states.includes("reconnecting"));
});

test("COM-T035 reset to a terminal snapshot closes without opening another stream", async (t) => {
  let reads = 0;
  const controller = setup(t, async () => new Response('event: stream.reset\ndata: {"code":"CURSOR_EXPIRED"}\n\n', {
    headers: { "Content-Type": "text/event-stream" },
  }));
  const states = [];
  assert.equal(await bounded(controller, { status: (v) => states.push(v), revoked: assert.fail,
    refresh: async () => { reads += 1; return snapshot({ status: "completed" }); } }), "returned");
  assert.equal(reads, 1);
  assert.equal(states.at(-1), "closed");
});

test("COM-T044 a refresh finishing after page abort cannot publish transport state", async (t) => {
  const states = [];
  const controller = setup(t, async () => event("2"));
  assert.equal(await bounded(controller, { status: (v) => states.push(v), revoked: assert.fail,
    refresh: async () => { controller.abort(); return snapshot(); } }), "returned");
  assert.deepEqual(states, ["connecting", "connected"]);
});

test("COM-T035 HTTP 410 adopts an authoritative snapshot cursor before replay", async (t) => {
  const cursors = [];
  const controller = setup(t, async (_url, options) => {
    cursors.push(options.headers["Last-Event-ID"]);
    if (cursors.length === 1) return new Response('{"code":"CURSOR_EXPIRED"}', { status: 410 });
    controller.abort();
    return event();
  });
  // Avoid a real one-second reconnect delay in this component test only.
  const nativeTimer = globalThis.setTimeout;
  globalThis.window = { setTimeout: (fn) => nativeTimer(fn, 0), clearTimeout };
  await subscribeEvents(queryId, initial, { status: () => undefined, revoked: assert.fail,
    refresh: async () => snapshot({ last_event_seq: 14, status_version: 8, spec_version: 2 }),
  }, controller.signal);
  assert.deepEqual(cursors, ["3", "14"]);
});

test("COM-T035 HTTP 409 with a completed snapshot needs no further SSE", async (t) => {
  let fetches = 0;
  const controller = setup(t, async () => { fetches += 1; return new Response("{}", { status: 409 }); });
  const states = [];
  assert.equal(await bounded(controller, { status: (v) => states.push(v), revoked: assert.fail,
    refresh: async () => snapshot({ status: "completed" }), }), "returned");
  assert.equal(fetches, 1);
  assert.equal(states.at(-1), "closed");
});
