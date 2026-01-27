from __future__ import annotations

from enum import Enum
from typing import Optional


class BotState(str, Enum):
    IDLE = "IDLE"
    READY = "READY"
    ENTERING = "ENTERING"
    DETECTING = "DETECTING"
    CUT_LOSER = "CUT_LOSER"
    HOLD_WINNER = "HOLD_WINNER"
    EXIT_WINNER = "EXIT_WINNER"
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
            self.state = BotState.READY

    def disconnect(self) -> None:
        self.state = BotState.IDLE
        self.active_cycle = False

    def start_cycle(self) -> bool:
        if self.state == BotState.READY:
            self.state = BotState.ENTERING
            self.active_cycle = True
            self.cycle_id += 1
            return True
        return False

    def stop(self) -> None:
        if self.state in {
            BotState.ENTERING,
            BotState.DETECTING,
            BotState.CUT_LOSER,
            BotState.HOLD_WINNER,
            BotState.EXIT_WINNER,
            BotState.COOLDOWN,
        }:
            self.state = BotState.READY
            self.active_cycle = False

    def finish_cycle(self) -> None:
        if self.state in {
            BotState.ENTERING,
            BotState.DETECTING,
            BotState.CUT_LOSER,
            BotState.HOLD_WINNER,
            BotState.EXIT_WINNER,
        }:
            self.state = BotState.COOLDOWN
            self.active_cycle = False

    def end_cooldown(self) -> None:
        if self.state == BotState.COOLDOWN:
            self.state = BotState.READY

    def set_error(self, message: str) -> None:
        self.state = BotState.ERROR
        self.active_cycle = False
        self.last_error = message
