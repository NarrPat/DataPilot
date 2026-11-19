import { inspectEvent, isTerminal, parseFrames } from "./state";
import type { EventCursor, SnapshotVersion } from "./state";

export type StreamState = "connecting" | "connected" | "reconnecting" | "resetting" | "unsupported" | "closed";
interface StreamSnapshot extends SnapshotVersion { status: string }
interface StreamCallbacks {
  refresh: () => Promise<StreamSnapshot | undefined>;
  status: (value: StreamState) => void;
  revoked: () => void;
}

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = window.setTimeout(done, milliseconds);
    function done() {
      window.clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

/** Same-origin credentialed fetch lets us inspect 401/410 and send Last-Event-ID.
 * Closing the reader only releases the subscription. It never cancels a query.
 */
export async function subscribeEvents(queryId: string, initial: EventCursor,
  callbacks: StreamCallbacks, signal: AbortSignal): Promise<void> {
  let cursor = initial;
  let backoff = 1000;
  const status = (value: StreamState) => { if (!signal.aborted) callbacks.status(value); };
  const refresh = async (reset = false) => {
    const snapshot = await callbacks.refresh();
    if (signal.aborted || snapshot?.query_id !== queryId) return undefined;
    cursor = reset ? { seq: snapshot.last_event_seq, statusVersion: snapshot.status_version,
      specVersion: snapshot.spec_version } : {
        seq: Math.max(cursor.seq, snapshot.last_event_seq),
        statusVersion: Math.max(cursor.statusVersion, snapshot.status_version),
        specVersion: Math.max(cursor.specVersion, snapshot.spec_version) };
    return snapshot;
  };
  while (!signal.aborted) {
    status("connecting");
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const response = await fetch(`/api/v1/queries/${encodeURIComponent(queryId)}/events`, {
        credentials: "same-origin", headers: { "Last-Event-ID": String(cursor.seq) },
        signal, cache: "no-store",
      });
      if (signal.aborted) return;
      if ([401, 403, 404].includes(response.status)) { callbacks.revoked(); return; }
      if (response.status === 409 || response.status === 410) {
        status("resetting");
        const snapshot = await refresh(true);
        if (snapshot && isTerminal(snapshot.status)) { status("closed"); return; }
        await wait(1000, signal);
        continue;
      }
      if (!response.ok || !response.headers.get("content-type")?.startsWith("text/event-stream") || !response.body)
        throw new Error("EVENT_STREAM_UNAVAILABLE");
      reader = response.body.getReader();
      status("connected");
      backoff = 1000;
      const decoder = new TextDecoder();
      let buffer = "";
      let reconnect = false;
      while (!signal.aborted) {
        const chunk = await reader.read();
        if (chunk.done) break;
        const parsed = parseFrames(buffer + decoder.decode(chunk.value, { stream: true }));
        buffer = parsed.remainder;
        for (const frame of parsed.frames) {
          if (signal.aborted) return;
          if (frame.event === "access.revoked") { callbacks.revoked(); return; }
          if (frame.event === "stream.reset" || frame.event === "stream.error") {
            status("resetting");
            const snapshot = await refresh(true);
            if (snapshot && isTerminal(snapshot.status)) { status("closed"); return; }
            reconnect = true;
            break;
          }
          let data: unknown;
          try { data = JSON.parse(frame.data); } catch { data = null; }
          const decision = inspectEvent(cursor, data, queryId);
          if (decision.action === "unsupported") {
            // Unsupported protocol is a stop condition even when GET fails.
            // The query view owns HTTP errors and preserves its last snapshot.
            try { await refresh(); } catch { /* Manual GET retry remains available. */ }
            status("unsupported");
            return;
          }
          cursor = decision.cursor;
          if (decision.action === "refresh") {
            const snapshot = await refresh();
            if (snapshot && isTerminal(snapshot.status)) { status("closed"); return; }
          }
        }
        if (reconnect) break;
      }
    } catch {
      if (signal.aborted) return;
      status("reconnecting");
      try {
        const snapshot = await refresh();
        if (snapshot && isTerminal(snapshot.status)) { status("closed"); return; }
      } catch { /* Preserve the current object; the query view displays its safe HTTP error. */ }
    } finally {
      await reader?.cancel().catch(() => undefined);
    }
    if (!signal.aborted) {
      status("reconnecting");
      await wait(backoff, signal);
      backoff = Math.min(backoff * 2, 10000);
    }
  }
}
