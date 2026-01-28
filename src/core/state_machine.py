from __future__ import annotations

from enum import Enum
from typing import Optional


class BotState(str, Enum):
    IDLE = "IDLE"
    DETECT = "DETECT"
    ENTERED_LONG = "ENTERED_LONG"
    ENTERED_SHORT = "ENTERED_SHORT"
    WAIT_WINNER = "WAIT_WINNER"
    EXIT = "EXIT"
    FLATTEN = "FLATTEN"
    COOLDOWN = "COOLDOWN"
    ERROR = "ERROR"


class BotStateMachine:
    def __init__(self) -> None:
        self.state = BotState.IDLE
        self.active_cycle = False
        self.cycle_id = 0
        self.last_error: Optional[str] = None
        self._allowed_transitions = {
            BotState.IDLE: {BotState.DETECT, BotState.FLATTEN},
            BotState.DETECT: {
                BotState.ENTERED_LONG,
                BotState.ENTERED_SHORT,
                BotState.FLATTEN,
                BotState.COOLDOWN,
                BotState.IDLE,
            },
            BotState.ENTERED_LONG: {
                BotState.ENTERED_SHORT,
                BotState.WAIT_WINNER,
                BotState.FLATTEN,
                BotState.COOLDOWN,
            },
            BotState.ENTERED_SHORT: {
                BotState.ENTERED_LONG,
                BotState.WAIT_WINNER,
                BotState.FLATTEN,
                BotState.COOLDOWN,
            },
            BotState.WAIT_WINNER: {BotState.EXIT, BotState.FLATTEN, BotState.COOLDOWN},
            BotState.EXIT: {BotState.COOLDOWN, BotState.FLATTEN},
            BotState.FLATTEN: {BotState.COOLDOWN},
            BotState.COOLDOWN: {BotState.IDLE},
            BotState.ERROR: {BotState.FLATTEN, BotState.COOLDOWN, BotState.IDLE},
        }

    def connect_ok(self) -> None:
        if self.state == BotState.IDLE:
            self.state = BotState.IDLE

    def disconnect(self) -> None:
        self.state = BotState.IDLE
        self.active_cycle = False

    def arm(self) -> None:
        if self.state in {BotState.IDLE, BotState.ERROR, BotState.COOLDOWN}:
            self.state = BotState.DETECT

    def start_cycle(self) -> bool:
        if self.state == BotState.DETECT:
            self.active_cycle = True
            self.cycle_id += 1
            return True
        return False

    def stop(self) -> None:
        if self.state in {
            BotState.DETECT,
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
            BotState.WAIT_WINNER,
            BotState.EXIT,
            BotState.FLATTEN,
            BotState.COOLDOWN,
        }:
            self.state = BotState.IDLE
            self.active_cycle = False

    def finish_cycle(self) -> None:
        if self.state in {
            BotState.DETECT,
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
            BotState.WAIT_WINNER,
            BotState.EXIT,
            BotState.FLATTEN,
            BotState.ERROR,
        }:
            self.state = BotState.COOLDOWN
            self.active_cycle = False

    def end_cooldown(self) -> None:
        if self.state == BotState.COOLDOWN:
            self.state = BotState.IDLE
            self.active_cycle = False

    def set_error(self, message: str) -> None:
        self.state = BotState.ERROR
        self.active_cycle = False
        self.last_error = message

    def transition(self, target: BotState) -> bool:
        if target == self.state:
            return True
        allowed = self._allowed_transitions.get(self.state, set())
        if target not in allowed:
            return False
        self.state = target
        if target in {
            BotState.ENTERED_LONG,
            BotState.ENTERED_SHORT,
            BotState.WAIT_WINNER,
            BotState.EXIT,
            BotState.FLATTEN,
        }:
            self.active_cycle = True
        if target in {BotState.COOLDOWN, BotState.IDLE}:
            self.active_cycle = False
        return True
