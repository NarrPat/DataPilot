/** UI ordering rules: events invalidate snapshots; they never invent results. */
export interface SnapshotVersion {
  query_id: string;
  status_version: number;
  spec_version: number;
  last_event_seq: number;
}

export function acceptSnapshot<T extends SnapshotVersion>(current: T | undefined, incoming: T): T {
  if (!current) return incoming;
  if (current.query_id !== incoming.query_id || incoming.status_version < current.status_version ||
      incoming.spec_version < current.spec_version || incoming.last_event_seq < current.last_event_seq)
    return current;
  return incoming;
}

export interface EventCursor { seq: number; statusVersion: number; specVersion: number }
export type EventDecision = { action: "ignore" | "refresh" | "unsupported"; cursor: EventCursor };

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function integer(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;
}

export function inspectEvent(cursor: EventCursor, value: unknown, queryId: string): EventDecision {
  if (!object(value) || value.schema_version !== "1" || value.app !== "datapilot" ||
      !integer(value.seq, 1) || !integer(value.status_version, 1) || !integer(value.spec_version, 0) ||
      typeof value.query_id !== "string" || typeof value.type !== "string")
    return { action: "unsupported", cursor };
  if (value.query_id !== queryId || value.seq <= cursor.seq) return { action: "ignore", cursor };
  return { action: "refresh", cursor: {
    seq: value.seq,
    statusVersion: Math.max(cursor.statusVersion, value.status_version),
    specVersion: Math.max(cursor.specVersion, value.spec_version),
  } };
}

export interface CommandRequest { key: string; serialized: string }
export function makeRequest(previous: CommandRequest | null, body: object,
  createKey: () => string = () => crypto.randomUUID()): CommandRequest {
  const serialized = JSON.stringify(body);
  return previous?.serialized === serialized ? previous : { key: createKey(), serialized };
}

export interface StreamFrame { id: string | null; event: string; data: string }
export function parseFrames(buffer: string): { frames: StreamFrame[]; remainder: string } {
  if (buffer.length > 262144) throw new Error("EVENT_FRAME_TOO_LARGE");
  const normalized = buffer.replace(/\r\n/g, "\n");
  const blocks = normalized.split("\n\n");
  const remainder = blocks.pop() ?? "";
  const frames: StreamFrame[] = [];
  for (const block of blocks) {
    let id: string | null = null;
    let event = "message";
    const data: string[] = [];
    for (const line of block.split("\n")) {
      if (line.startsWith(":")) continue;
      const separator = line.indexOf(":");
      const field = separator < 0 ? line : line.slice(0, separator);
      const raw = separator < 0 ? "" : line.slice(separator + 1);
      const value = raw.startsWith(" ") ? raw.slice(1) : raw;
      if (field === "id" && !value.includes("\0")) id = value;
      if (field === "event") event = value;
      if (field === "data") data.push(value);
    }
    if (data.length) frames.push({ id, event, data: data.join("\n") });
  }
  return { frames, remainder };
}

export function ratioAsPercent(value: string | null | undefined): string {
  if (!value || !/^-?[0-9]+\.[0-9]{6}$/.test(value)) return "—";
  const negative = value.startsWith("-");
  const [whole, fraction] = value.replace(/^-/, "").split(".");
  const units = BigInt(whole) * 1000000n + BigInt(fraction);
  // DP-D07: six-place ratio -> two-place percentage, ROUND_HALF_UP.
  // Work on the absolute integer, then restore the sign without negative zero.
  const hundredths = (units + 50n) / 100n;
  const text = `${hundredths / 100n}.${(hundredths % 100n).toString().padStart(2, "0")}`;
  return `${negative && hundredths !== 0n ? "-" : ""}${text}%`;
}

export function isTerminal(status: string): boolean {
  return ["completed", "rejected", "needs_refine", "failed", "timed_out", "canceled"].includes(status);
}
