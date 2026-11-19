"""Closed transport vocabulary: no hostnames, exception messages or headers."""

from typing import Literal, get_args

TransportPhase = Literal[
    "connect_tcp", "tls", "request_headers", "request_body", "response_headers", "response_body"
]
TransportFailure = Literal[
    "ConnectError",
    "ConnectTimeout",
    "ReadError",
    "ReadTimeout",
    "WriteError",
    "WriteTimeout",
    "RemoteProtocolError",
    "LocalProtocolError",
    "PoolTimeout",
    "other_transport_error",
]

_MESSAGE_PHASES: dict[str, TransportPhase] = {
    "send_request_headers": "request_headers",
    "send_request_body": "request_body",
    "receive_response_headers": "response_headers",
    "receive_response_body": "response_body",
}
TRACE_PHASES: dict[str, TransportPhase] = {
    "connection.connect_tcp": "connect_tcp",
    "connection.start_tls": "tls",
    **{
        f"{protocol}.{name}": phase
        for protocol in ("http11", "http2")
        for name, phase in _MESSAGE_PHASES.items()
    },
}
FAILURE_TYPES = frozenset(get_args(TransportFailure))
