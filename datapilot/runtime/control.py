"""Bounded metadata scan then outbox publication; signal-aware between calls."""

from __future__ import annotations

import argparse
import os
import signal
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event
from typing import Any, NoReturn, Protocol

from datapilot.dispatch.celery import CeleryPublisher, create_celery
from datapilot.dispatch.service import Dispatcher, DispatchReport
from datapilot.runtime.config import RuntimeConfig
from datapilot.runtime.worker import (
    TraceSink,
    app_connection,
    configure_runtime_logging,
    emit_runtime_trace,
    task_service,
)


class Scanner(Protocol):
    def scan_once(self, *, limit: int) -> Any: ...


class Dispatch(Protocol):
    def dispatch_once(self, *, limit: int) -> DispatchReport: ...


class TimingSweep(Protocol):
    def sweep(self, *, limit: int) -> dict[str, int]: ...


class ControlLoopError(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


SCAN_COUNTERS = (
    "queued_expired",
    "waiting_expired",
    "inspected",
    "blocked",
    "projected",
    "scheduled",
    "pending",
    "closed",
    "errors",
)
DISPATCH_COUNTERS = ("selected", "published", "failed", "recorded")


def safe_counters(source: Any, names: tuple[str, ...]) -> dict[str, int | None]:
    result = {}
    for name in names:
        value = source.get(name) if isinstance(source, dict) else getattr(source, name, None)
        result[name] = value if type(value) is int and 0 <= value <= 2**63 - 1 else None
    return result


class ControlArguments(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("CONTROL_ARGUMENT_INVALID")


@dataclass(frozen=True)
class CycleReport:
    scan: Any
    dispatch: DispatchReport


class ControlLoop:
    def __init__(
        self,
        scanner: Scanner,
        dispatcher: Dispatch,
        *,
        stop: Event | None = None,
        interval: float = 1,
        limit: int = 10,
        trace_sink: TraceSink | None = None,
        timing: TimingSweep | None = None,
    ) -> None:
        # Reuse exactly the same primitive bounds as CLI configuration.
        RuntimeConfig(interval_seconds=interval, batch_limit=limit)
        self.scanner, self.dispatcher = scanner, dispatcher
        self.stop, self.interval, self.limit = stop or Event(), interval, limit
        self.trace_sink = trace_sink
        self.timing = timing

    def step(self) -> CycleReport:
        if self.stop.is_set():
            return CycleReport(None, DispatchReport(0, 0, 0, 0))
        try:
            scanned = self.scanner.scan_once(limit=self.limit)
        except Exception:
            emit_runtime_trace(
                "control.error", {"code": "CONTROL_SCAN_FAILED"}, sink=self.trace_sink
            )
            raise ControlLoopError("CONTROL_SCAN_FAILED") from None
        total = DispatchReport(0, 0, 0, 0)
        if self.timing is not None and not self.stop.is_set():
            try:
                timed = self.timing.sweep(limit=self.limit)
                emit_runtime_trace(
                    "timing.cycle",
                    safe_counters(timed, ("checked", "verified", "invalid", "blocked", "errors")),
                    sink=self.trace_sink,
                )
            except Exception:
                emit_runtime_trace(
                    "timing.error", {"code": "TIMING_SWEEP_DEFERRED"}, sink=self.trace_sink
                )
        for _ in range(min(self.limit, 50)):
            if self.stop.is_set():
                break
            try:
                report = self.dispatcher.dispatch_once(limit=1)
            except Exception:
                emit_runtime_trace(
                    "control.error", {"code": "CONTROL_DISPATCH_FAILED"}, sink=self.trace_sink
                )
                raise ControlLoopError("CONTROL_DISPATCH_FAILED") from None
            total = DispatchReport(
                total.selected + report.selected,
                total.published + report.published,
                total.failed + report.failed,
                total.recorded + report.recorded,
            )
            if report.selected == 0:
                break
        emit_runtime_trace(
            "control.cycle",
            {
                "scan": safe_counters(scanned, SCAN_COUNTERS),
                "dispatch": safe_counters(total, DISPATCH_COUNTERS),
                "projected_semantics": (
                    "app_projection_including_waiting_terminal_refine_not_success_count"
                ),
            },
            sink=self.trace_sink,
        )
        return CycleReport(scanned, total)

    def run(
        self,
        *,
        max_cycles: int | None = None,
        on_cycle: Callable[[CycleReport], None] | None = None,
    ) -> int:
        if max_cycles is not None and (type(max_cycles) is not int or max_cycles < 1):
            raise ValueError("POSITIVE_CONTROL_CYCLE_LIMIT_REQUIRED")
        cycles = 0
        while not self.stop.is_set() and (max_cycles is None or cycles < max_cycles):
            report = self.step()
            cycles += 1
            if on_cycle:
                on_cycle(report)
            if max_cycles is not None and cycles >= max_cycles:
                break
            self.stop.wait(self.interval)
        emit_runtime_trace(
            "control.stopped",
            {
                "cycles": cycles,
                "code": "CONTROL_STOPPED" if self.stop.is_set() else "CONTROL_CYCLE_LIMIT_REACHED",
            },
            sink=self.trace_sink,
        )
        return cycles


def create_control_loop(
    config: RuntimeConfig, *, stop: Event | None = None
) -> tuple[ControlLoop, Any]:
    config.validate_environment(os.environ)
    # Importing the entry does not require initialized recovery infrastructure.
    from datapilot.tasks.recovery import RecoveryScanner
    from datapilot.tasks.timing import ServerTimingVerifier
    from datapilot.worker.checkpoints import CheckpointInspector

    tasks = task_service(config)
    inspector = CheckpointInspector(tasks)
    scanner = RecoveryScanner(
        tasks, inspector, lambda: app_connection(config, autocommit=True), stale_after_seconds=5
    )
    app = create_celery(namespace=config.namespace)
    publisher = CeleryPublisher(app, queue_name=config.queue_name)
    return ControlLoop(
        scanner,
        Dispatcher(tasks.connect, publisher),
        stop=stop,
        interval=config.interval_seconds,
        limit=config.batch_limit,
        timing=ServerTimingVerifier(tasks, lambda: app_connection(config, autocommit=True)),
    ), app


def main() -> int:
    stop = Event()
    previous = {}
    app = None

    def request_stop(_number: int, _frame: Any) -> None:
        stop.set()

    try:
        parser = ControlArguments(description=__doc__)
        parser.add_argument("--max-cycles", type=int)
        args = parser.parse_args()
        config = RuntimeConfig.from_environment()
        configure_runtime_logging()
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, request_stop)
        loop, app = create_control_loop(config, stop=stop)
        loop.run(max_cycles=args.max_cycles)
        return 0
    except ControlLoopError as exc:
        emit_runtime_trace("control.error", {"code": exc.code})
        return 2
    except Exception:
        emit_runtime_trace("control.error", {"code": "CONTROL_RUNTIME_FAILED"})
        return 2
    finally:
        if app is not None:
            app.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
