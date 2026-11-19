import assert from "node:assert/strict";
import test from "node:test";
import {
  acceptSnapshot,
  inspectEvent,
  isTerminal,
  makeRequest,
  parseFrames,
  ratioAsPercent,
} from "../src/state.ts";

const snapshot = { query_id: "query-b", status_version: 7, spec_version: 2, last_event_seq: 12, result: "new" };
const cursor = { seq: 12, statusVersion: 7, specVersion: 2 };
const event = { schema_version: "1", app: "datapilot", query_id: "query-b", seq: 13,
  status_version: 8, spec_version: 2, type: "query.completed", payload: { status: "completed" } };

test("COM-T043 delayed HTTP cannot replace newer status/spec/event or another object", () => {
  for (const old of [
    { ...snapshot, status_version: 6 },
    { ...snapshot, spec_version: 1 },
    { ...snapshot, last_event_seq: 11 },
    { ...snapshot, query_id: "query-a" },
  ]) assert.equal(acceptSnapshot(snapshot, old), snapshot);
  const newer = { ...snapshot, status_version: 8, last_event_seq: 13 };
  assert.equal(acceptSnapshot(snapshot, newer), newer);
  assert.equal(acceptSnapshot(undefined, snapshot), snapshot);
});

test("DP-T058 duplicate and stale version events never publish payloads as results", () => {
  assert.equal(inspectEvent(cursor, { ...event, seq: 12 }, "query-b").action, "ignore");
  assert.equal(inspectEvent(cursor, { ...event, query_id: "query-a" }, "query-b").action, "ignore");
  assert.equal(inspectEvent(cursor, { ...event, status_version: 6 }, "query-b").action, "refresh");
  const next = inspectEvent(cursor, event, "query-b");
  assert.deepEqual(next, { action: "refresh", cursor: { seq: 13, statusVersion: 8, specVersion: 2 } });
  assert.equal("result" in next, false);
});

test("COM-T035 unknown major or malformed envelope stops application and requests snapshot", () => {
  for (const value of [{ ...event, schema_version: "2" }, { ...event, seq: -1 }, null, {}])
    assert.equal(inspectEvent(cursor, value, "query-b").action, "unsupported");
});

test("COM-T045 idempotency key survives retry and changes only with the command", () => {
  let calls = 0;
  const id = () => `key-${++calls}`;
  const first = makeRequest(null, { question: "金额" }, id);
  assert.equal(makeRequest(first, { question: "金额" }, id), first);
  assert.notEqual(makeRequest(first, { question: "订单数" }, id).key, first.key);
  assert.equal(calls, 2);
});

test("COM-T033 streamed frames accept CRLF, multiline data, heartbeats and split buffers", () => {
  const first = parseFrames(': heartbeat\r\n\r\nid: 13\r\nevent: query.updated\r\ndata: {"seq":\r\n');
  assert.deepEqual(first.frames, []);
  const second = parseFrames(first.remainder + 'data: 13}\r\n\r\nid: 14\nevent: query.completed\ndata: {}\n\n');
  assert.deepEqual(second.frames, [
    { id: "13", event: "query.updated", data: '{"seq":\n13}' },
    { id: "14", event: "query.completed", data: "{}" },
  ]);
  assert.equal(second.remainder, "");
});

test("DP-T045 percent display preserves exact large values and explicit null", () => {
  assert.equal(ratioAsPercent("0.123456"), "12.35%");
  assert.equal(ratioAsPercent("-0.000001"), "0.00%");
  assert.equal(ratioAsPercent("12345678901234567890.123456"), "1234567890123456789012.35%");
  assert.equal(ratioAsPercent(null), "—");
  assert.equal(ratioAsPercent("invalid"), "—");
});

test("DP-D07 percentages use two decimal half-up rounding, including negative ties", () => {
  for (const [ratio, expected] of [
    ["0.123456", "12.35%"], ["0.000050", "0.01%"], ["-0.000050", "-0.01%"],
    ["-0.000001", "0.00%"], ["0.999999", "100.00%"],
  ]) assert.equal(ratioAsPercent(ratio), expected);
});

test("DP-T061 cancel_requested remains nonterminal and completed stays completed", () => {
  assert.equal(isTerminal("executing"), false);
  assert.equal(isTerminal("cancel_requested"), false);
  assert.equal(isTerminal("completed"), true);
  assert.equal(isTerminal("canceled"), true);
});
