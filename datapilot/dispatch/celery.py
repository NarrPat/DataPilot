"""An inert factory and fixed-route gateway for this project's Redis broker.

No worker or connection starts on import/factory creation. The application must
bind a generation/lease-aware fragment handler before starting a consumer.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from celery import Celery  # type: ignore[import-untyped]
from kombu.entity import Exchange, Queue
from pydantic import ValidationError

from datapilot.dispatch.contracts import DispatchMessage

QUEUE_NAME = "datapilot.fragments"
TASK_NAME = "datapilot.run_fragment"
BROKER_URL = "redis://redis:6379/0"
EVAL_NAMESPACE = re.compile(
    r"dp_eval_(i009|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)_([a-z0-9_]{1,24})\Z"
)
EVAL_QUEUE = re.compile(
    r"datapilot\.(?:i009|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)\.[a-z0-9_]{1,24}\.fragments\Z"
)


@dataclass(frozen=True)
class BrokerRoute:
    url: str
    queue: str
    key_prefix: str | None


def registered_route(namespace: str | None = None) -> BrokerRoute:
    if namespace is None:
        return BrokerRoute(BROKER_URL, QUEUE_NAME, None)
    matched = EVAL_NAMESPACE.fullmatch(namespace)
    if matched is None:
        raise ValueError("REGISTERED_RUNTIME_NAMESPACE_REQUIRED")
    iteration, suffix = matched.groups()
    return BrokerRoute(
        "redis://redis:6379/9",
        f"datapilot.{iteration}.{suffix}.fragments",
        f"{iteration}:{suffix}:",
    )


def create_celery(*, namespace: str | None = None) -> Any:
    route = registered_route(namespace)
    for name in ("CELERY_BROKER_URL", "CELERY_BROKER_READ_URL", "CELERY_BROKER_WRITE_URL"):
        if os.environ.get(name) not in (None, "", route.url):
            raise ValueError("UNREGISTERED_BROKER_OVERRIDE")
    if os.environ.get("CELERY_RESULT_BACKEND"):
        raise ValueError("RESULT_BACKEND_FORBIDDEN")
    app = Celery("datapilot", broker=route.url, backend=None)
    app.conf.update(
        task_default_queue=route.queue,
        task_default_exchange=route.queue,
        task_default_routing_key=route.queue,
        task_queues=(
            Queue(  # type: ignore[no-untyped-call]
                route.queue,
                Exchange(route.queue, type="direct"),  # type: ignore[no-untyped-call]
                routing_key=route.queue,
            ),
        ),
        task_routes={TASK_NAME: {"queue": route.queue, "routing_key": route.queue}},
        task_create_missing_queues=False,
        accept_content=["json"],
        result_accept_content=["json"],
        task_serializer="json",
        result_serializer="json",
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        worker_concurrency=2,
        worker_pool="prefork",
        worker_enable_remote_control=False,
        worker_cancel_long_running_tasks_on_connection_loss=True,
        task_publish_retry=False,
        broker_connection_timeout=2,
        broker_connection_retry_on_startup=False,
        broker_connection_max_retries=3,
        broker_pool_limit=1,
        broker_transport_options={
            "socket_timeout": 2,
            "socket_connect_timeout": 2,
            "max_retries": 0,
            "visibility_timeout": 180,
            **({"global_keyprefix": route.key_prefix} if route.key_prefix else {}),
        },
        task_ignore_result=True,
        task_store_errors_even_if_ignored=False,
        task_remote_tracebacks=False,
        task_track_started=False,
        task_send_sent_event=False,
        worker_send_task_events=False,
        worker_hijack_root_logger=False,
        worker_redirect_stdouts=False,
        task_soft_time_limit=125,
        task_time_limit=130,
        enable_utc=True,
        timezone="UTC",
    )
    return app


class CeleryPublisher:
    def __init__(self, app: Any, *, queue_name: str = QUEUE_NAME) -> None:
        if queue_name != QUEUE_NAME and EVAL_QUEUE.fullmatch(queue_name) is None:
            raise ValueError("REGISTERED_QUEUE_REQUIRED")
        if hasattr(app, "conf") and app.conf.task_default_queue != queue_name:
            raise ValueError("CELERY_ROUTE_MISMATCH")
        self.app, self.queue_name = app, queue_name

    def __call__(self, message: DispatchMessage) -> None:
        self.app.send_task(
            TASK_NAME,
            kwargs={"message": message.model_dump(mode="json")},
            queue=self.queue_name,
            routing_key=self.queue_name,
            serializer="json",
            retry=False,
            task_id=message.delivery_id,
        )


def register_fragment_task(app: Any, handler: Callable[[DispatchMessage], Any]) -> Any:
    """Bind once during worker boot; the handler must enforce the DB execution gate."""
    if TASK_NAME in app.tasks:
        raise ValueError("FRAGMENT_HANDLER_ALREADY_REGISTERED")

    def run_fragment(message: object) -> Any:
        try:
            parsed = DispatchMessage.model_validate(message)
        except ValidationError:
            raise ValueError("DISPATCH_MESSAGE_INVALID") from None
        try:
            return handler(parsed)
        except Exception:
            # Celery may log task failures. Never expose arbitrary exception text.
            raise RuntimeError("FRAGMENT_HANDLER_FAILED") from None

    return app.task(name=TASK_NAME, shared=False)(run_fragment)
