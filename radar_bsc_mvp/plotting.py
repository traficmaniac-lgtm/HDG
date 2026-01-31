import os
from typing import Optional

NO_PLOT = os.environ.get("NO_PLOT", "0") == "1"

if NO_PLOT:

    class PlotCanvas:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def update_plot(self, *args, **kwargs) -> None:
            pass

else:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure

    def build_figure() -> Figure:
        fig = Figure(figsize=(7, 6), tight_layout=True)
        return fig

    def _plot_snapshot(ax, row: dict) -> None:
        price = row.get("priceUsd", 0) or 0
        liquidity = row.get("liquidityUsd", 0) or 0
        volume1h = row.get("volume1h", 0) or 0
        chg1h = row.get("chg1h", 0) or 0

        ax.bar(["price", "liq", "vol1h"], [price, liquidity, volume1h], color=["#4c72b0", "#55a868", "#8172b2"])
        ax.set_title("DexScreener snapshot")
        ax.text(0.02, 0.95, f"1h change: {chg1h:.2f}%", transform=ax.transAxes, va="top")
        ax.text(
            0.02,
            0.85,
            "Snapshot-only mode (no candles)",
            transform=ax.transAxes,
            fontsize=8,
            va="top",
        )

    def _plot_bucket(ax, title: str, buckets: list[int], color: str) -> None:
        if not buckets:
            ax.text(0.5, 0.5, "No onchain data", ha="center", va="center")
            ax.set_title(title)
            return
        ax.plot(list(range(len(buckets))), buckets, color=color, marker="o")
        ax.set_title(title)
        ax.set_xlabel("Buckets (approx minutes)")
        ax.set_ylabel("Count")

    def _plot_whale_sum(ax, buckets: list[int]) -> None:
        if not buckets:
            ax.text(0.5, 0.5, "No onchain data", ha="center", va="center")
            ax.set_title("Whale transfer sum (raw)")
            return
        ax.bar(list(range(len(buckets))), buckets, color="#ccb974")
        ax.set_title("Whale transfer sum (raw)")
        ax.set_xlabel("Buckets (approx minutes)")
        ax.set_ylabel("Raw amount")

    def update_summary_plot(fig: Figure, row: Optional[dict]) -> None:
        fig.clear()
        axes = fig.subplots(3, 1)
        if row is None:
            try:
                fig.canvas.draw_idle()
            except Exception:
                pass
            return

        _plot_snapshot(axes[0], row)
        _plot_bucket(axes[1], "Transfers per bucket", row.get("on_transfer_buckets") or [], "#c44e52")
        _plot_bucket(axes[2], "Whale transfers per bucket", row.get("on_whale_buckets") or [], "#dd8452")
        if row.get("on_whale_sum_buckets"):
            extra_ax = axes[2].twinx()
            _plot_whale_sum(extra_ax, row.get("on_whale_sum_buckets") or [])
            extra_ax.set_ylabel("Raw sum")

        for ax in axes:
            ax.grid(alpha=0.2)

        try:
            fig.canvas.draw_idle()
        except Exception:
            pass

    class PlotCanvas(FigureCanvas):
        def __init__(self) -> None:
            self.figure = build_figure()
            super().__init__(self.figure)

        def update_plot(self, row: Optional[dict]) -> None:
            update_summary_plot(self.figure, row)
