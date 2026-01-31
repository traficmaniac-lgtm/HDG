import json
import sys
from typing import Optional

import pandas as pd
import requests
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from bsc_rpc import BscRpcClient
from dexscreener_client import build_universe
from onchain_metrics import enrich_candidates
from plots import PlotCanvas
from signals import apply_signals, select_candidates

DEFAULT_MIN_LIQ = 50_000
DEFAULT_MIN_VOL_1H = 20_000
DEFAULT_UNIVERSE_TARGET = 400
DEFAULT_TOP_N = 25
DEFAULT_LOOKBACK_MIN = 120
DEFAULT_RPC = "https://bsc-dataseed.binance.org"
DEFAULT_CHUNK_BLOCKS = 1500
DEFAULT_MAX_LOGS = 12000


class RadarWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.session = requests.Session()
        self.universe_df = pd.DataFrame()
        self.candidates_df = pd.DataFrame()
        self.results_df = pd.DataFrame()

        self.setWindowTitle("BSC Radar MVP")
        self.resize(1300, 800)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        self.table = QTableWidget()
        self.table.setSortingEnabled(False)
        self.table.itemSelectionChanged.connect(self.handle_selection)

        self.plot_canvas = PlotCanvas()

        settings_group = QGroupBox("Settings")
        settings_form = QFormLayout()
        self.min_liq_input = QLineEdit(str(DEFAULT_MIN_LIQ))
        self.min_vol_input = QLineEdit(str(DEFAULT_MIN_VOL_1H))
        self.universe_target_input = QLineEdit(str(DEFAULT_UNIVERSE_TARGET))
        self.top_n_input = QLineEdit(str(DEFAULT_TOP_N))
        self.lookback_input = QLineEdit(str(DEFAULT_LOOKBACK_MIN))
        self.rpc_input = QLineEdit(DEFAULT_RPC)
        self.exclude_stable_checkbox = QCheckBox("Exclude stable/stable")

        settings_form.addRow("Min liquidity USD", self.min_liq_input)
        settings_form.addRow("Min volume 1h USD", self.min_vol_input)
        settings_form.addRow("Universe target", self.universe_target_input)
        settings_form.addRow("Top N", self.top_n_input)
        settings_form.addRow("Lookback minutes", self.lookback_input)
        settings_form.addRow("RPC URL", self.rpc_input)
        settings_form.addRow("", self.exclude_stable_checkbox)
        settings_group.setLayout(settings_form)

        self.build_button = QPushButton("Build Universe")
        self.select_button = QPushButton("Select Candidates")
        self.enrich_button = QPushButton("Enrich Onchain")
        self.save_csv_button = QPushButton("Save CSV")
        self.save_jsonl_button = QPushButton("Save JSONL")

        self.build_button.clicked.connect(self.handle_build_universe)
        self.select_button.clicked.connect(self.handle_select_candidates)
        self.enrich_button.clicked.connect(self.handle_enrich)
        self.save_csv_button.clicked.connect(self.handle_save_csv)
        self.save_jsonl_button.clicked.connect(self.handle_save_jsonl)

        button_row = QHBoxLayout()
        button_row.addWidget(self.build_button)
        button_row.addWidget(self.select_button)
        button_row.addWidget(self.enrich_button)
        button_row.addWidget(self.save_csv_button)
        button_row.addWidget(self.save_jsonl_button)
        button_row.addStretch()

        top_layout = QHBoxLayout()
        top_layout.addWidget(settings_group)
        top_layout.addStretch()

        lower_layout = QHBoxLayout()
        lower_layout.addWidget(self.table, 3)
        lower_layout.addWidget(self.plot_canvas, 2)

        layout = QVBoxLayout()
        layout.addLayout(top_layout)
        layout.addLayout(button_row)
        layout.addWidget(QLabel("Results"))
        layout.addLayout(lower_layout)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def update_status(self, message: str) -> None:
        self.status_bar.showMessage(message)
        QApplication.processEvents()

    def _get_int(self, field: QLineEdit, default: int) -> int:
        try:
            return int(field.text())
        except ValueError:
            return default

    def _get_float(self, field: QLineEdit, default: float) -> float:
        try:
            return float(field.text())
        except ValueError:
            return default

    def _current_df(self) -> pd.DataFrame:
        if not self.results_df.empty:
            return self.results_df
        if not self.candidates_df.empty:
            return self.candidates_df
        return self.universe_df

    def show_dataframe(self, frame: pd.DataFrame) -> None:
        self.table.clear()
        if frame.empty:
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            return

        self.table.setRowCount(len(frame))
        self.table.setColumnCount(len(frame.columns))
        self.table.setHorizontalHeaderLabels(list(frame.columns))

        for row_idx, (_, row) in enumerate(frame.iterrows()):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() ^ Qt.ItemIsEditable)
                self.table.setItem(row_idx, col_idx, item)

        self._apply_row_colors(frame)
        self.table.resizeColumnsToContents()

    def _apply_row_colors(self, frame: pd.DataFrame) -> None:
        if "signal" not in frame.columns:
            return
        signal_index = frame.columns.get_loc("signal")
        for row_idx in range(len(frame)):
            signal = str(frame.iloc[row_idx, signal_index])
            if signal == "HOT":
                color = Qt.green
            elif signal in {"TX_SPIKE", "VOL_SPIKE"}:
                color = Qt.yellow
            elif signal == "RISK":
                color = Qt.red
            else:
                color = None
            if color is None:
                continue
            for col_idx in range(frame.shape[1]):
                item = self.table.item(row_idx, col_idx)
                if item is not None:
                    item.setBackground(color)

    def handle_build_universe(self) -> None:
        self.update_status("Building universe...")
        min_liq = self._get_float(self.min_liq_input, DEFAULT_MIN_LIQ)
        min_vol = self._get_float(self.min_vol_input, DEFAULT_MIN_VOL_1H)
        target = self._get_int(self.universe_target_input, DEFAULT_UNIVERSE_TARGET)
        exclude_stable = self.exclude_stable_checkbox.isChecked()

        try:
            self.universe_df = build_universe(
                self.session,
                min_liq_usd=min_liq,
                min_vol_1h_usd=min_vol,
                target=target,
                exclude_stable_pairs=exclude_stable,
                status_callback=self.update_status,
            )
            count = len(self.universe_df)
            self.update_status(f"Universe ready: {count} pairs")
        except Exception as exc:  # noqa: BLE001
            self.universe_df = pd.DataFrame()
            self.update_status(f"Universe error: {exc}")
        self.candidates_df = pd.DataFrame()
        self.results_df = pd.DataFrame()
        self.show_dataframe(self.universe_df)
        self.plot_canvas.update_plot(None)

    def handle_select_candidates(self) -> None:
        if self.universe_df.empty:
            self.update_status("Build universe first")
            return
        top_n = self._get_int(self.top_n_input, DEFAULT_TOP_N)
        self.update_status("Selecting candidates...")
        self.candidates_df = select_candidates(self.universe_df, top_n)
        count = len(self.candidates_df)
        self.update_status(f"Candidates ready: {count}")
        self.results_df = pd.DataFrame()
        self.show_dataframe(self.candidates_df)
        self.plot_canvas.update_plot(None)

    def handle_enrich(self) -> None:
        if self.candidates_df.empty:
            self.update_status("Select candidates first")
            return
        lookback = self._get_int(self.lookback_input, DEFAULT_LOOKBACK_MIN)
        rpc_url = self.rpc_input.text().strip() or DEFAULT_RPC
        rpc_client = BscRpcClient(rpc_url, session=self.session)
        self.update_status("Enriching onchain data...")

        try:
            enriched = enrich_candidates(
                self.candidates_df,
                rpc_client,
                lookback_minutes=lookback,
                chunk_blocks=DEFAULT_CHUNK_BLOCKS,
                max_logs_per_token=DEFAULT_MAX_LOGS,
                status_callback=self.update_status,
            )
            self.results_df = apply_signals(enriched, lookback)
            count = len(self.results_df)
            self.update_status(f"Enrichment complete: {count} tokens")
        except Exception as exc:  # noqa: BLE001
            self.results_df = pd.DataFrame()
            self.update_status(f"Enrichment error: {exc}")
        self.show_dataframe(self.results_df)
        self.plot_canvas.update_plot(None)

    def _save_dataframe(self, frame: pd.DataFrame, file_filter: str, default_name: str) -> None:
        if frame.empty:
            self.update_status("No data to save")
            return
        filename, _ = QFileDialog.getSaveFileName(
            self, "Save", default_name, file_filter
        )
        if not filename:
            return
        if file_filter.startswith("CSV"):
            frame.to_csv(filename, index=False)
        else:
            with open(filename, "w", encoding="utf-8") as handle:
                for _, row in frame.iterrows():
                    handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
                    handle.write("\n")
        self.update_status(f"Saved: {filename}")

    def handle_save_csv(self) -> None:
        self._save_dataframe(self._current_df(), "CSV Files (*.csv)", "results.csv")

    def handle_save_jsonl(self) -> None:
        self._save_dataframe(self._current_df(), "JSONL Files (*.jsonl)", "results.jsonl")

    def handle_selection(self) -> None:
        frame = self._current_df()
        if frame.empty:
            return
        selection = self.table.selectionModel().selectedRows()
        if not selection:
            return
        row_index = selection[0].row()
        try:
            row_data = frame.iloc[row_index].to_dict()
        except IndexError:
            return
        self.plot_canvas.update_plot(row_data)


def main() -> None:
    app = QApplication(sys.argv)
    window = RadarWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
