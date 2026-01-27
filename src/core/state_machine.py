from __future__ import annotations

from enum import Enum
from typing import Optional


class BotState(str, Enum):
    IDLE = "IDLE"
    ARMED = "ARMED"
    ENTERING = "ENTERING"
    DETECTING = "DETECTING"
    CUTTING = "CUTTING"
    RIDING = "RIDING"
    EXITING = "EXITING"
    CONTROLLED_FLATTEN = "CONTROLLED_FLATTEN"
    COOLDOWN = "COOLDOWN"
    ERROR = "ERROR"


class BotStateMachine:
    def __init__(self) -> None:
        self.state = BotState.IDLE
        self.active_cycle = False
        self.cycle_id = 0
        self.last_error: Optional[str] = None

    def connect_ok(self) -> None:
        if self.state == BotState.IDLE:
            self.state = BotState.IDLE

    def disconnect(self) -> None:
        self.state = BotState.IDLE
        self.active_cycle = False

    def arm(self) -> None:
        if self.state in {BotState.IDLE, BotState.ERROR, BotState.COOLDOWN}:
            self.state = BotState.ARMED

    def start_cycle(self) -> bool:
        if self.state == BotState.ARMED:
            self.state = BotState.ENTERING
            self.active_cycle = True
            self.cycle_id += 1
            return True
        return False

    def stop(self) -> None:
        if self.state in {
            BotState.ENTERING,
            BotState.DETECTING,
            BotState.CUTTING,
            BotState.RIDING,
            BotState.EXITING,
            BotState.CONTROLLED_FLATTEN,
            BotState.COOLDOWN,
        }:
            self.state = BotState.IDLE
            self.active_cycle = False

    def finish_cycle(self) -> None:
        if self.state in {
            BotState.ENTERING,
            BotState.DETECTING,
            BotState.CUTTING,
            BotState.RIDING,
            BotState.EXITING,
            BotState.CONTROLLED_FLATTEN,
            BotState.ERROR,
        }:
            self.state = BotState.COOLDOWN

    def end_cooldown(self) -> None:
        if self.state == BotState.COOLDOWN:
            self.state = BotState.ARMED
            self.active_cycle = False

    def set_error(self, message: str) -> None:
        self.state = BotState.ERROR
        self.active_cycle = False
        self.last_error = message
