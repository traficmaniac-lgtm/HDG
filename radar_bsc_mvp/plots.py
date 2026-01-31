from typing import Optional

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure


def build_figure() -> Figure:
    fig = Figure(figsize=(7, 4), tight_layout=True)
    return fig


def update_summary_plot(fig: Figure, row: Optional[dict]) -> None:
    fig.clear()
    axes = fig.subplots(2, 2)
    if row is None:
        fig.canvas.draw_idle()
        return

    price = row.get("priceUsd", 0) or 0
    volume1h = row.get("volume1h", 0) or 0
    volume5m = row.get("volume5m", 0) or 0
    liquidity = row.get("liquidityUsd", 0) or 0
    tx_count = row.get("on_tx_count", 0) or 0
    senders = row.get("on_unique_senders", 0) or 0
    receivers = row.get("on_unique_receivers", 0) or 0
    ratio = row.get("onchain_to_dex_ratio", 0) or 0

    ax_price = axes[0, 0]
    ax_price.bar(["price", "liq"], [price, liquidity], color=["#4c72b0", "#55a868"])
    ax_price.set_title("Price & Liquidity")

    ax_volume = axes[0, 1]
    ax_volume.bar(["vol5m", "vol1h"], [volume5m, volume1h], color="#8172b2")
    ax_volume.set_title("Dex Volume")

    ax_tx = axes[1, 0]
    ax_tx.bar(["tx", "senders", "receivers"], [tx_count, senders, receivers], color="#c44e52")
    ax_tx.set_title("Onchain Activity")

    ax_ratio = axes[1, 1]
    ax_ratio.bar(["ratio"], [ratio], color="#ccb974")
    ax_ratio.set_title("Onchain/Dex Ratio")

    for ax in axes.flatten():
        ax.tick_params(axis="x", rotation=20)

    fig.canvas.draw_idle()


class PlotCanvas(FigureCanvas):
    def __init__(self) -> None:
        self.figure = build_figure()
        super().__init__(self.figure)

    def update_plot(self, row: Optional[dict]) -> None:
        update_summary_plot(self.figure, row)
