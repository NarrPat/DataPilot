"""Short-transaction outbox publisher; sent is explicitly not claimed.

COM-F06/COM-T021/022 components. A successful but unclaimed delivery is eligible
again after five seconds. A broker timeout may have published before failing;
the worker must deduplicate by fixed run lease/generation, not by task ID.

This is not the recovery/expiry scanner. It never generates a recovery or
changes run state, admission or counters. The caller must perform control
expiry independently; waiting and closed rows are never automatically sent.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from datapilot.dispatch.contracts import DispatchMessage
from datapilot.tasks.service import Connect

Publisher = Callable[[DispatchMessage], None]
SUCCESS_REDELIVERY_DELAY = timedelta(seconds=5)


def retry_delay(attempt: int) -> timedelta:
    if type(attempt) is not int or attempt < 1:
        raise ValueError("POSITIVE_DISPATCH_ATTEMPT_REQUIRED")
    return timedelta(seconds=min(30, 2 ** min(attempt - 1, 5)))


@dataclass(frozen=True)
class DispatchReport:
    selected: int
    published: int
    failed: int
    recorded: int


class Dispatcher:
    def __init__(
        self,
        connect: Connect,
        publisher: Publisher,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.connect, self.publisher, self.clock = connect, publisher, clock

    def now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("AWARE_DISPATCH_CLOCK_REQUIRED")
        return value.astimezone(UTC)

    def dispatch_once(self, *, limit: int = 10) -> DispatchReport:
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ValueError("DISPATCH_LIMIT_OUTSIDE_CONTRACT")
        due = self.now()
        with self.connect() as connection:
            selected = connection.execute(
                "SELECT o.outbox_id,o.query_id,o.execution_generation,o.kind "
                "FROM app.query_outbox o JOIN app.query_runs q USING(query_id) "
                "WHERE o.claimed_at IS NULL AND o.canceled_at IS NULL "
                "AND COALESCE(o.next_attempt_at,o.created_at)<=%s "
                "AND o.execution_generation=q.execution_generation "
                "AND ((o.kind IN ('create','resume') AND q.status='queued' "
                "AND q.admission_state='queued') OR "
                "(o.kind='recover_fragment' AND q.admission_state='active' "
                "AND q.status IN ('parsing','generating','guarding','planning',"
                "'executing','validating','repairing'))) "
                "ORDER BY CASE WHEN o.kind='recover_fragment' THEN 0 ELSE 1 END,"
                "COALESCE(o.next_attempt_at,o.created_at),o.created_at,o.outbox_id LIMIT %s",
                (due, limit),
            ).fetchall()
        published = failed = recorded = 0
        for row in selected:
            message = DispatchMessage.model_validate(
                {name: row[name] for name in ("query_id", "execution_generation", "kind")}
            )
            # No app connection or row lock survives the broker call. A process
            # stopping here leaves the same outbox eligible for another delivery.
            try:
                self.publisher(message)
            except Exception:
                failed += 1
                # Persist a stable class only. Broker exception text may contain
                # connection strings or provider values and is never copied.
                recorded += self._record(row["outbox_id"], message, success=False)
            else:
                published += 1
                recorded += self._record(row["outbox_id"], message, success=True)
        return DispatchReport(len(selected), published, failed, recorded)

    def _record(self, outbox_id: UUID, message: DispatchMessage, *, success: bool) -> int:
        now = self.now()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT dispatch_attempts FROM app.query_outbox "
                "WHERE outbox_id=%s AND query_id=%s AND execution_generation=%s AND kind=%s "
                "AND claimed_at IS NULL AND canceled_at IS NULL FOR UPDATE",
                (outbox_id, message.query_id, message.execution_generation, message.kind),
            ).fetchone()
            if row is None:
                return 0
            attempt = row["dispatch_attempts"] + 1
            delay = SUCCESS_REDELIVERY_DELAY if success else retry_delay(attempt)
            connection.execute(
                "UPDATE app.query_outbox SET dispatch_attempts=%s,next_attempt_at=%s,"
                "published_at=CASE WHEN %s THEN %s ELSE published_at END,last_dispatch_code=%s "
                "WHERE outbox_id=%s",
                (
                    attempt,
                    now + delay,
                    success,
                    now,
                    None if success else "BROKER_UNAVAILABLE",
                    outbox_id,
                ),
            )
            return 1
