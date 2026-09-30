"""Subscriber for Bitcoin Core's ZMQ `sequence` topic (bitcoin.conf:
`zmqpubsequence=tcp://<addr>:<port>`), used by mempool-watch.

Each message is three frames: [b"sequence", body, 4-byte LE message
counter]. body is a 32-byte hash (already in RPC/display byte order) plus a
one-byte label:
  A  tx added to the mempool       (+ 8-byte LE mempool sequence)
  R  tx removed from the mempool for any reason OTHER than block inclusion
     (replaced, evicted, expired, conflicted)   (+ 8-byte LE mempool sequence)
  C  block connected
  D  block disconnected (reorg)
See https://github.com/bitcoin/bitcoin/blob/master/doc/zmq.md.

ZMQ is fire-and-forget: a SUB socket never errors on a wrong/unreachable
endpoint (it just silently keeps retrying the connection), and messages
can be dropped under load. Both are surfaced rather than hidden - a gap in
the per-topic message counter sets gap_detected (mempool-watch then runs a
full reconcile), and last_message_at lets the caller warn when nothing has
arrived for a suspiciously long time.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

TOPIC = b"sequence"


@dataclass(frozen=True)
class SequenceEvent:
    label: str  # "A", "R", "C" or "D"
    hash_hex: str
    mempool_sequence: int | None


def parse_sequence_body(body: bytes) -> SequenceEvent:
    if len(body) < 33:
        raise ValueError(f"sequence message too short ({len(body)} bytes)")
    label = chr(body[32])
    mempool_sequence = None
    if label in ("A", "R") and len(body) >= 41:
        mempool_sequence = int.from_bytes(body[33:41], "little")
    return SequenceEvent(label=label, hash_hex=body[:32].hex(), mempool_sequence=mempool_sequence)


class ZmqSequenceSubscriber:
    def __init__(self, endpoint: str) -> None:
        # Imported here, not at module level, so every other command keeps
        # working on a host where pyzmq isn't installed.
        import zmq

        self._zmq = zmq
        self.endpoint = endpoint
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        # 0 = unbounded receive queue: during the (slow) startup scan the
        # service isn't reading the socket at all, and the node's own send
        # buffer (zmqpubsequencehwm) is the only other limit. Each message
        # is ~45 bytes, so even hours of buffered events are negligible.
        self._socket.setsockopt(zmq.RCVHWM, 0)
        self._socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self._socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)
        self._socket.setsockopt(zmq.SUBSCRIBE, TOPIC)
        self._socket.connect(endpoint)

        self._last_counter: int | None = None
        self.gap_detected = False
        self.last_message_at = time.monotonic()
        logger.info("Subscribed to ZMQ sequence topic at %s.", endpoint)

    def receive_batch(self, timeout_seconds: float, max_events: int = 5000) -> list[SequenceEvent]:
        """Wait up to timeout_seconds for the first message, then drain
        whatever else is already queued (up to max_events) without
        blocking."""
        zmq = self._zmq
        events: list[SequenceEvent] = []
        if not self._socket.poll(int(timeout_seconds * 1000)):
            return events

        while len(events) < max_events:
            try:
                frames = self._socket.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                break
            if len(frames) < 2 or frames[0] != TOPIC:
                continue
            self.last_message_at = time.monotonic()
            self._check_counter(frames[2] if len(frames) > 2 else None)
            try:
                events.append(parse_sequence_body(frames[1]))
            except ValueError as exc:
                logger.warning("Ignoring malformed ZMQ sequence message: %s", exc)
        return events

    def _check_counter(self, raw: bytes | None) -> None:
        if raw is None or len(raw) != 4:
            return
        counter = int.from_bytes(raw, "little")
        if self._last_counter is not None and counter != (self._last_counter + 1) & 0xFFFFFFFF:
            # Dropped messages, or the node restarted (counter resets to 0).
            logger.warning(
                "ZMQ sequence counter jumped %d -> %d - messages were lost or the node restarted.",
                self._last_counter,
                counter,
            )
            self.gap_detected = True
        self._last_counter = counter

    def take_gap(self) -> bool:
        gap, self.gap_detected = self.gap_detected, False
        return gap

    def close(self) -> None:
        self._socket.close(linger=0)
        self._context.term()
