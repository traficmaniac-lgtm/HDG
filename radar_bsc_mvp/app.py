import atexit
import faulthandler
import json
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Callable, Optional

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRASH_JOURNAL = os.path.join(LOG_DIR, "crash_journal.txt")
CRASH_TRACE = os.path.join(LOG_DIR, "faulthandler.log")


def journal(msg: str) -> None:
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(CRASH_JOURNAL, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
            f.flush()
    except Exception:
        pass


try:
    _crash_trace_file = open(CRASH_TRACE, "a", buffering=1, encoding="utf-8")
    faulthandler.enable(file=_crash_trace_file, all_threads=True)
    journal("faulthandler enabled")
except Exception as e:
    journal(f"faulthandler enable failed: {e}")


def excepthook(exctype, value, tb):
    journal("UNCAUGHT PYTHON EXCEPTION:")
    journal("".join(traceback.format_exception(exctype, value, tb)))
    sys.__excepthook__(exctype, value, tb)


sys.excepthook = excepthook

# Force matplotlib backend early (Qt6)
os.environ.setdefault("MPLBACKEND", "qtagg")
journal(f"MPLBACKEND={os.environ.get('MPLBACKEND')}")

atexit.register(lambda: journal("ATEXIT: python exit reached"))

import pandas as pd
import requests
from PySide6.QtCore import QObject, Qt, QThread, Signal
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
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
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from analyzer import EnrichConfig, apply_signals, build_universe_frame, enrich_candidates, select_candidates
from bsc_rpc import BscRpcClient
from dexscreener_client import DexScreenerClient
from plotting import PlotCanvas

DEFAULT_MIN_LIQ = 50_000
DEFAULT_MIN_VOL_1H = 20_000
DEFAULT_UNIVERSE_TARGET = 250
DEFAULT_TOP_N = 25
DEFAULT_LOOKBACK_MIN = 120
DEFAULT_RPC = "https://bsc-dataseed.binance.org"
DEFAULT_DEX_BASE = "https://api.dexscreener.com"
DEFAULT_CHUNK_BLOCKS = 1500
DEFAULT_MAX_LOGS = 12000
DEFAULT_WHALE_THRESHOLD_RAW = 10**21

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


@dataclass
class WorkerResult:
    job: str
    df: pd.DataFrame


class Worker(QObject):
    finished = Signal(object)
    errored = Signal(str)
    status = Signal(str)

    def __init__(self, job: str, func: Callable[[Callable[[str], None]], pd.DataFrame]) -> None:
        super().__init__()
        self.job = job
        self.func = func

    def run(self) -> None:
        journal(f"WORKER START job={self.job} thread={threading.current_thread().name}")
        try:
            result = self.func(self.status.emit)
        except Exception as exc:  # noqa: BLE001
            self.errored.emit(str(exc))
            return
        self.finished.emit(WorkerResult(self.job, result))


class RadarWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.session = requests.Session()
        self.universe_df = pd.DataFrame()
        self.candidates_df = pd.DataFrame()
        self.results_df = pd.DataFrame()

        self.setWindowTitle("BSC Radar MVP v0.2.0")
        self.resize(1400, 860)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        self.table = QTableWidget()
        self.table.setSortingEnabled(False)

        self.plot_canvas = PlotCanvas()

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFixedHeight(120)

        settings_group = QGroupBox("Settings")
        settings_form = QFormLayout()
        self.dex_base_input = QLineEdit(DEFAULT_DEX_BASE)
        self.rpc_input = QLineEdit(DEFAULT_RPC)
        self.min_liq_input = QLineEdit(str(DEFAULT_MIN_LIQ))
        self.min_vol_input = QLineEdit(str(DEFAULT_MIN_VOL_1H))
        self.universe_target_input = QLineEdit(str(DEFAULT_UNIVERSE_TARGET))
        self.top_n_input = QLineEdit(str(DEFAULT_TOP_N))
        self.lookback_input = QLineEdit(str(DEFAULT_LOOKBACK_MIN))
        self.chunk_blocks_input = QLineEdit(str(DEFAULT_CHUNK_BLOCKS))
        self.max_logs_input = QLineEdit(str(DEFAULT_MAX_LOGS))
        self.whale_threshold_input = QLineEdit(str(DEFAULT_WHALE_THRESHOLD_RAW))
        self.exclude_stable_checkbox = QCheckBox("Exclude stable/stable")

        settings_form.addRow("DexScreener Base URL", self.dex_base_input)
        settings_form.addRow("BSC RPC URL", self.rpc_input)
        settings_form.addRow("Min liquidity USD", self.min_liq_input)
        settings_form.addRow("Min volume 1h USD", self.min_vol_input)
        settings_form.addRow("Universe target", self.universe_target_input)
        settings_form.addRow("Top N", self.top_n_input)
        settings_form.addRow("Lookback minutes", self.lookback_input)
        settings_form.addRow("Chunk blocks", self.chunk_blocks_input)
        settings_form.addRow("Max logs per token", self.max_logs_input)
        settings_form.addRow("Whale threshold raw", self.whale_threshold_input)
        settings_form.addRow("", self.exclude_stable_checkbox)
        settings_group.setLayout(settings_form)

        self.build_button = QPushButton("Build Universe")
        self.select_button = QPushButton("Select Candidates")
        self.enrich_button = QPushButton("Enrich Onchain")
        self.plot_button = QPushButton("Plot Selected")
        self.save_csv_button = QPushButton("Save CSV")
        self.save_jsonl_button = QPushButton("Save JSONL")

        self.build_button.clicked.connect(self.handle_build_universe)
        self.select_button.clicked.connect(self.handle_select_candidates)
        self.enrich_button.clicked.connect(self.handle_enrich)
        self.plot_button.clicked.connect(self.handle_plot_selected)
        self.save_csv_button.clicked.connect(self.handle_save_csv)
        self.save_jsonl_button.clicked.connect(self.handle_save_jsonl)

        button_row = QHBoxLayout()
        button_row.addWidget(self.build_button)
        button_row.addWidget(self.select_button)
        button_row.addWidget(self.enrich_button)
        button_row.addWidget(self.plot_button)
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
        layout.addWidget(QLabel("Status log"))
        layout.addWidget(self.log_output)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self._active_thread: Optional[QThread] = None
        self._active_worker: Optional[Worker] = None
        self._active_job: Optional[str] = None

    def update_status(self, message: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        self.status_bar.showMessage(message)
        self.log_output.append(f"[{timestamp}] {message}")
        journal(f"STATUS thread={threading.current_thread().name}: {message}")
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
        if "signal_label" not in frame.columns:
            return
        signal_index = frame.columns.get_loc("signal_label")
        for row_idx in range(len(frame)):
            signal = str(frame.iloc[row_idx, signal_index])
            if signal == "HOT":
                color = Qt.green
            elif signal == "WARM":
                color = Qt.yellow
            elif signal == "RISKY":
                color = Qt.red
            else:
                color = None
            if color is None:
                continue
            for col_idx in range(frame.shape[1]):
                item = self.table.item(row_idx, col_idx)
                if item is not None:
                    item.setBackground(color)

    def closeEvent(self, event) -> None:  # noqa: N802
        journal("WINDOW closeEvent fired")
        super().closeEvent(event)

    def _start_worker(self, job: str, func: Callable[[Callable[[str], None]], pd.DataFrame]) -> None:
        if self._active_thread and self._active_thread.isRunning():
            self.update_status("Worker already running")
            return

        journal(f"WORKER QUEUED job={job} thread={threading.current_thread().name}")
        thread = QThread()
        worker = Worker(job, func)
        worker.moveToThread(thread)

        self._active_thread = thread
        self._active_worker = worker
        self._active_job = job

        thread.started.connect(worker.run)
        worker.finished.connect(self._on_worker_finished)
        worker.errored.connect(self._on_worker_error)

        worker.finished.connect(thread.quit)
        worker.errored.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.errored.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)

        worker.status.connect(self.update_status)

        thread.start()

    def _on_worker_finished(self, result: WorkerResult) -> None:
        self._active_thread = None
        self._active_worker = None
        self._active_job = None
        journal(f"WORKER FINISHED job={result.job} rows={len(result.df)}")

        if result.job == "build_universe":
            self.universe_df = result.df
            self.candidates_df = pd.DataFrame()
            self.results_df = pd.DataFrame()
            self.show_dataframe(self.universe_df)
            self.plot_canvas.update_plot(None)
            self.update_status(f"Universe ready: {len(self.universe_df)}")
            return

        if result.job == "enrich":
            self.results_df = result.df
            self.show_dataframe(self.results_df)
            self.plot_canvas.update_plot(None)
            self.update_status(f"Enrich done: {len(self.results_df)}")
            return

        self.show_dataframe(result.df)
        self.update_status("Done")

    def _on_worker_error(self, error: str) -> None:
        self._active_thread = None
        self._active_worker = None
        self._active_job = None
        journal(f"WORKER ERROR: {error}")
        self.update_status(f"ERROR: {error}")

    def handle_build_universe(self) -> None:
        journal("CLICK: Build Universe")
        min_liq = self._get_float(self.min_liq_input, DEFAULT_MIN_LIQ)
        min_vol = self._get_float(self.min_vol_input, DEFAULT_MIN_VOL_1H)
        target = self._get_int(self.universe_target_input, DEFAULT_UNIVERSE_TARGET)
        exclude_stable = self.exclude_stable_checkbox.isChecked()
        dex_base = self.dex_base_input.text().strip() or DEFAULT_DEX_BASE

        def task(status_callback: Callable[[str], None]) -> pd.DataFrame:
            status_callback("Building universe...")
            client = DexScreenerClient(dex_base, session=self.session)
            frame = build_universe_frame(
                client,
                min_liq_usd=min_liq,
                min_vol_1h_usd=min_vol,
                target=target,
                exclude_stable_pairs=exclude_stable,
                status_callback=status_callback,
            )
            status_callback(f"Universe built: {len(frame)}")
            return frame

        self._start_worker("build_universe", task)

    def handle_select_candidates(self) -> None:
        journal("CLICK: Select Candidates")
        if self.universe_df.empty:
            self.update_status("Build universe first")
            return
        top_n = self._get_int(self.top_n_input, DEFAULT_TOP_N)
        min_liq = self._get_float(self.min_liq_input, DEFAULT_MIN_LIQ)
        min_vol = self._get_float(self.min_vol_input, DEFAULT_MIN_VOL_1H)
        self.update_status("Selecting candidates...")
        self.candidates_df = select_candidates(self.universe_df, top_n, min_liq, min_vol)
        self.results_df = pd.DataFrame()
        self.update_status(f"Candidates ready: {len(self.candidates_df)}")
        self.show_dataframe(self.candidates_df)
        self.plot_canvas.update_plot(None)

    def handle_enrich(self) -> None:
        journal("CLICK: Enrich Onchain")
        if self.candidates_df.empty:
            self.update_status("Select candidates first")
            return
        lookback = self._get_int(self.lookback_input, DEFAULT_LOOKBACK_MIN)
        rpc_url = self.rpc_input.text().strip() or DEFAULT_RPC
        chunk_blocks = self._get_int(self.chunk_blocks_input, DEFAULT_CHUNK_BLOCKS)
        max_logs = self._get_int(self.max_logs_input, DEFAULT_MAX_LOGS)
        whale_threshold_raw = self._get_int(self.whale_threshold_input, DEFAULT_WHALE_THRESHOLD_RAW)

        config = EnrichConfig(
            lookback_minutes=lookback,
            chunk_blocks=chunk_blocks,
            max_logs_per_token=max_logs,
            whale_threshold_raw=whale_threshold_raw,
        )

        def task(status_callback: Callable[[str], None]) -> pd.DataFrame:
            status_callback("Enriching onchain...")
            rpc_client = BscRpcClient(rpc_url, session=self.session)
            enriched = enrich_candidates(
                self.candidates_df,
                rpc_client,
                config,
                status_callback=status_callback,
            )
            result = apply_signals(enriched, lookback)
            status_callback("Enrich finished")
            return result

        self._start_worker("enrich", task)

    def _selected_row(self) -> Optional[dict]:
        frame = self._current_df()
        if frame.empty:
            return None
        selection = self.table.selectionModel().selectedRows()
        if not selection:
            return None
        row_index = selection[0].row()
        try:
            return frame.iloc[row_index].to_dict()
        except IndexError:
            return None

    def handle_plot_selected(self) -> None:
        row_data = self._selected_row()
        if row_data is None:
            self.update_status("Select a row first")
            return
        journal("TABLE: row selected -> plot")
        self.plot_canvas.update_plot(row_data)
        self.update_status("Plot updated")

    def _save_dataframe(self, frame: pd.DataFrame, extension: str) -> None:
        if frame.empty:
            self.update_status("No data to save")
            return
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(OUTPUT_DIR, f"results_{timestamp}.{extension}")
        if extension == "csv":
            frame.to_csv(filename, index=False)
        else:
            with open(filename, "w", encoding="utf-8") as handle:
                for _, row in frame.iterrows():
                    handle.write(json.dumps(row.to_dict(), ensure_ascii=False))
                    handle.write("\n")
        self.update_status(f"Saved: {filename}")

    def handle_save_csv(self) -> None:
        self._save_dataframe(self._current_df(), "csv")

    def handle_save_jsonl(self) -> None:
        self._save_dataframe(self._current_df(), "jsonl")


def main() -> None:
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(lambda: journal("Qt aboutToQuit fired"))
    window = RadarWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
