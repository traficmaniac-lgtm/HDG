import enum
import heapq
import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

import psutil

try:
    import pydivert
except ImportError:  # pragma: no cover
    pydivert = None


class Mode(str, enum.Enum):
    FREEZE = "FREEZE"
    LAG = "LAG"
    THROTTLE = "THROTTLE"
    LOSS = "LOSS"


@dataclass
class EmulatorConfig:
    remote_ip: str = ""
    remote_port: int = 0
    mode: Mode = Mode.FREEZE
    lag_ms: int = 500
    limit_kbps: int = 64
    loss_percent: float = 10.0
    process_name: str = ""
    interface_index: int = 0
    freeze_hard: bool = True


class InboundTrafficEmulator:
    def __init__(self) -> None:
        self._config = EmulatorConfig()
        self._handle = None
        self._capture_thread: Optional[threading.Thread] = None
        self._send_thread: Optional[threading.Thread] = None
        self._running = threading.Event()

        self._freeze_buffer: queue.Queue = queue.Queue()
        self._sched_lock = threading.Lock()
        self._scheduled_packets = []

        self._tokens = 0.0
        self._last_token_ts = time.monotonic()

        self._stats_lock = threading.Lock()
        self._intercepted = 0
        self._forwarded = 0
        self._dropped = 0

    def update_config(self, config: EmulatorConfig) -> None:
        self._config = config

    def start(self) -> None:
        if pydivert is None:
            raise RuntimeError("pydivert не установлен. Установите зависимости.")
        if self._running.is_set():
            return

        wdfilter = self._build_windivert_filter(self._config)
        self._handle = pydivert.WinDivert(wdfilter)
        self._handle.open()

        self._running.set()
        self._capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._capture_thread.start()
        self._send_thread.start()

    def stop(self) -> None:
        if not self._running.is_set():
            return

        self._running.clear()

        if self._capture_thread:
            self._capture_thread.join(timeout=2.0)
        if self._send_thread:
            self._send_thread.join(timeout=2.0)

        self._flush_all_queued()

        if self._handle is not None:
            try:
                self._handle.close()
            except OSError:
                pass
            self._handle = None

    def stats(self) -> dict:
        with self._stats_lock:
            return {
                "intercepted": self._intercepted,
                "forwarded": self._forwarded,
                "dropped": self._dropped,
                "buffered": self._freeze_buffer.qsize() + len(self._scheduled_packets),
            }

    @staticmethod
    def _build_windivert_filter(config: EmulatorConfig) -> str:
        clauses = ["tcp", "inbound"]
        if config.remote_ip:
            clauses.append(f"ip.SrcAddr == {config.remote_ip}")
        if config.remote_port:
            clauses.append(f"tcp.SrcPort == {config.remote_port}")
        return " and ".join(clauses)

    def _capture_loop(self) -> None:
        while self._running.is_set() and self._handle is not None:
            try:
                packet = self._handle.recv()
            except OSError:
                break

            if not self._packet_matches_extra_filters(packet):
                self._send_packet(packet)
                continue

            with self._stats_lock:
                self._intercepted += 1

            if self._is_ack_without_payload(packet):
                self._send_packet(packet)
                continue

            mode = self._config.mode
            if mode == Mode.FREEZE:
                if self._config.freeze_hard:
                    self._freeze_buffer.put(packet)
                else:
                    release_at = time.monotonic() + max(0, self._config.lag_ms) / 1000.0
                    self._schedule_packet(release_at, packet)
            elif mode == Mode.LAG:
                release_at = time.monotonic() + max(0, self._config.lag_ms) / 1000.0
                self._schedule_packet(release_at, packet)
            elif mode == Mode.THROTTLE:
                self._freeze_buffer.put(packet)
            elif mode == Mode.LOSS:
                if random.uniform(0, 100) < self._config.loss_percent:
                    with self._stats_lock:
                        self._dropped += 1
                else:
                    self._send_packet(packet)

    def _send_loop(self) -> None:
        while self._running.is_set():
            now = time.monotonic()
            self._release_scheduled(now)

            if self._config.mode == Mode.THROTTLE:
                self._throttle_release(now)

            time.sleep(0.005)

    def _throttle_release(self, now: float) -> None:
        limit_bps = max(1, self._config.limit_kbps) * 1024
        elapsed = now - self._last_token_ts
        self._last_token_ts = now
        self._tokens = min(limit_bps * 2, self._tokens + elapsed * limit_bps)

        while not self._freeze_buffer.empty():
            packet = self._freeze_buffer.queue[0]
            packet_size = max(1, len(packet.raw))
            if self._tokens < packet_size:
                break
            self._tokens -= packet_size
            packet = self._freeze_buffer.get_nowait()
            self._send_packet(packet)

    def _flush_all_queued(self) -> None:
        while not self._freeze_buffer.empty():
            try:
                packet = self._freeze_buffer.get_nowait()
            except queue.Empty:
                break
            self._send_packet(packet)

        with self._sched_lock:
            pending = [item[1] for item in self._scheduled_packets]
            self._scheduled_packets.clear()

        for packet in pending:
            self._send_packet(packet)

    def _release_scheduled(self, now: float) -> None:
        to_send = []
        with self._sched_lock:
            while self._scheduled_packets and self._scheduled_packets[0][0] <= now:
                _, packet = heapq.heappop(self._scheduled_packets)
                to_send.append(packet)
        for packet in to_send:
            self._send_packet(packet)

    def _schedule_packet(self, release_at: float, packet) -> None:
        with self._sched_lock:
            heapq.heappush(self._scheduled_packets, (release_at, packet))

    def _send_packet(self, packet) -> None:
        if self._handle is None:
            return
        try:
            self._handle.send(packet)
            with self._stats_lock:
                self._forwarded += 1
        except OSError:
            pass

    def _packet_matches_extra_filters(self, packet) -> bool:
        cfg = self._config

        if cfg.interface_index and getattr(packet, "interface", None):
            if packet.interface[0] != cfg.interface_index:
                return False

        if cfg.process_name:
            pid = self._find_pid_by_local_port(packet.dst_port)
            if pid is None:
                return False
            try:
                proc_name = psutil.Process(pid).name().lower()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return False
            if cfg.process_name.lower() not in proc_name:
                return False

        return True

    @staticmethod
    def _find_pid_by_local_port(port: int) -> Optional[int]:
        try:
            for conn in psutil.net_connections(kind="tcp"):
                if not conn.laddr:
                    continue
                if conn.laddr.port == port and conn.pid:
                    return conn.pid
        except psutil.Error:
            return None
        return None

    @staticmethod
    def _is_ack_without_payload(packet) -> bool:
        tcp = getattr(packet, "tcp", None)
        if tcp is None:
            return False
        return bool(tcp.ack) and len(packet.payload) == 0
