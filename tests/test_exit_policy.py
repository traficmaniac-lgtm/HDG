from __future__ import annotations

from src.services.trade_executor import TradeExecutor


def test_sl_policy_crossing() -> None:
    bid = 1.1965
    ask = 1.1967
    tick = 0.0001
    price, _, _, policy = TradeExecutor._compute_exit_sell_price(
        intent="SL",
        bid=bid,
        ask=ask,
        tick_size=tick,
    )
    assert policy == "SL_CROSS"
    assert price is not None
    assert price <= bid


def test_tp_policy_maker() -> None:
    bid = 1.1965
    ask = 1.1967
    tick = 0.0001
    price, _, _, policy = TradeExecutor._compute_exit_sell_price(
        intent="TP",
        bid=bid,
        ask=ask,
        tick_size=tick,
    )
    assert policy == "TP_MAKER"
    assert price is not None
    assert price == ask


def test_tick_rounding() -> None:
    bid = 1.1966
    ask = 1.19674
    tick = 0.0001
    price, _, _, _ = TradeExecutor._compute_exit_sell_price(
        intent="TP",
        bid=bid,
        ask=ask,
        tick_size=tick,
    )
    assert price == 1.1967
