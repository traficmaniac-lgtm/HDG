import math
import sys
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

DEXSCREENER_BSC_URL = "https://api.dexscreener.com/latest/dex/pairs/bsc"
BSC_RPC_URL = "https://bsc-dataseed.binance.org"
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
LIQUIDITY_MIN_USD = 50_000
VOLUME_1H_MIN_USD = 20_000
DEFAULT_CANDIDATES = 25
DEFAULT_LOOKBACK_MINUTES = 120
BLOCK_TIME_SECONDS = 3.0
CHUNK_SIZE_BLOCKS = 2000


def _to_float(value: Optional[str]) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_universe(session: requests.Session) -> pd.DataFrame:
    response = session.get(DEXSCREENER_BSC_URL, timeout=20)
    response.raise_for_status()
    payload = response.json()
    pairs = payload.get("pairs", [])

    rows: List[Dict[str, object]] = []
    for pair in pairs:
        liquidity = pair.get("liquidity", {}) or {}
        volume = pair.get("volume", {}) or {}
        price_change = pair.get("priceChange", {}) or {}

        liquidity_usd = _to_float(liquidity.get("usd"))
        volume_1h = _to_float(volume.get("h1"))
        if liquidity_usd < LIQUIDITY_MIN_USD or volume_1h < VOLUME_1H_MIN_USD:
            continue

        base_token = pair.get("baseToken", {}) or {}
        quote_token = pair.get("quoteToken", {}) or {}
        rows.append(
            {
                "pair_address": pair.get("pairAddress"),
                "base_token_address": base_token.get("address"),
                "base_token_symbol": base_token.get("symbol"),
                "quote_token_symbol": quote_token.get("symbol"),
                "dex_id": pair.get("dexId"),
                "price_usd": _to_float(pair.get("priceUsd")),
                "liquidity_usd": liquidity_usd,
                "volume_1h": volume_1h,
                "price_change_1h": _to_float(price_change.get("h1")),
                "fdv": _to_float(pair.get("fdv")),
                "pair_url": pair.get("url"),
            }
        )

    universe = pd.DataFrame(rows)
    if universe.empty:
        return universe

    universe = universe.sort_values("volume_1h", ascending=False).head(600)
    return universe.reset_index(drop=True)


def select_candidates(universe: pd.DataFrame, top_n: int = DEFAULT_CANDIDATES) -> pd.DataFrame:
    if universe.empty:
        return universe

    candidates = universe.copy()
    candidates["price_change_1h_abs"] = candidates["price_change_1h"].abs()
    candidates = candidates.sort_values(
        ["volume_1h", "price_change_1h_abs"], ascending=[False, False]
    ).head(top_n)
    return candidates.drop(columns=["price_change_1h_abs"]).reset_index(drop=True)


def _rpc_call(session: requests.Session, method: str, params: List[object]) -> Dict[str, object]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    response = session.post(BSC_RPC_URL, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data


def _get_latest_block(session: requests.Session) -> int:
    data = _rpc_call(session, "eth_blockNumber", [])
    return int(data["result"], 16)


def _get_logs(
    session: requests.Session,
    address: str,
    from_block: int,
    to_block: int,
) -> List[Dict[str, object]]:
    params = {
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "address": address,
        "topics": [TRANSFER_TOPIC],
    }
    data = _rpc_call(session, "eth_getLogs", [params])
    return data.get("result", [])


def enrich_onchain(
    session: requests.Session,
    candidates: pd.DataFrame,
    status_callback,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    latest_block = _get_latest_block(session)
    blocks_back = int((lookback_minutes * 60) / BLOCK_TIME_SECONDS)
    start_block = max(0, latest_block - blocks_back)

    enriched_rows: List[Dict[str, object]] = []
    for _, row in candidates.iterrows():
        token_address = row.get("base_token_address")
        if not token_address:
            continue

        status_callback(f"Enriching {row.get('base_token_symbol')}...")
        tx_count = 0
        transfer_volume = 0
        unique_senders = set()
        unique_receivers = set()

        try:
            chunk_count = math.ceil((latest_block - start_block + 1) / CHUNK_SIZE_BLOCKS)
            for chunk_index in range(chunk_count):
                chunk_start = start_block + chunk_index * CHUNK_SIZE_BLOCKS
                chunk_end = min(latest_block, chunk_start + CHUNK_SIZE_BLOCKS - 1)
                logs = _get_logs(session, token_address, chunk_start, chunk_end)
                for log in logs:
                    topics = log.get("topics", [])
                    if len(topics) < 3:
                        continue
                    tx_count += 1
                    try:
                        transfer_volume += int(log.get("data", "0x0"), 16)
                    except ValueError:
                        pass
                    sender = f"0x{topics[1][-40:]}"
                    receiver = f"0x{topics[2][-40:]}"
                    unique_senders.add(sender.lower())
                    unique_receivers.add(receiver.lower())
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            status_callback(f"RPC error for {row.get('base_token_symbol')}: {exc}")

        enriched_rows.append(
            {
                **row.to_dict(),
                "tx_count": tx_count,
                "transfer_volume_raw": transfer_volume,
                "unique_senders": len(unique_senders),
                "unique_receivers": len(unique_receivers),
            }
        )

    enriched = pd.DataFrame(enriched_rows)
    return enriched.reset_index(drop=True)


class RadarWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.session = requests.Session()
        self.universe_df = pd.DataFrame()
        self.candidates_df = pd.DataFrame()
        self.enriched_df = pd.DataFrame()

        self.setWindowTitle("BSC Radar MVP")
        self.resize(1100, 600)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        self.table = QTableWidget()
        self.table.setSortingEnabled(False)

        self.build_button = QPushButton("Build Universe")
        self.select_button = QPushButton("Select Candidates")
        self.enrich_button = QPushButton("Enrich Onchain")
        self.save_button = QPushButton("Save CSV")

        self.build_button.clicked.connect(self.handle_build_universe)
        self.select_button.clicked.connect(self.handle_select_candidates)
        self.enrich_button.clicked.connect(self.handle_enrich)
        self.save_button.clicked.connect(self.handle_save)

        button_row = QHBoxLayout()
        button_row.addWidget(self.build_button)
        button_row.addWidget(self.select_button)
        button_row.addWidget(self.enrich_button)
        button_row.addWidget(self.save_button)
        button_row.addStretch()

        layout = QVBoxLayout()
        layout.addLayout(button_row)
        layout.addWidget(QLabel("Results:"))
        layout.addWidget(self.table)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def update_status(self, message: str) -> None:
        self.status_bar.showMessage(message)
        QApplication.processEvents()

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

        self.table.resizeColumnsToContents()

    def handle_build_universe(self) -> None:
        self.update_status("Building universe from DexScreener...")
        try:
            self.universe_df = build_universe(self.session)
            count = len(self.universe_df)
            self.update_status(f"Universe ready: {count} pairs")
        except Exception as exc:  # noqa: BLE001
            self.universe_df = pd.DataFrame()
            self.update_status(f"Universe error: {exc}")
        self.show_dataframe(self.universe_df)

    def handle_select_candidates(self) -> None:
        if self.universe_df.empty:
            self.update_status("Build universe first")
            return
        self.update_status("Selecting candidates...")
        self.candidates_df = select_candidates(self.universe_df)
        count = len(self.candidates_df)
        self.update_status(f"Candidates ready: {count}")
        self.show_dataframe(self.candidates_df)

    def handle_enrich(self) -> None:
        if self.candidates_df.empty:
            self.update_status("Select candidates first")
            return
        self.update_status("Enriching onchain data...")
        try:
            self.enriched_df = enrich_onchain(
                self.session, self.candidates_df, self.update_status
            )
            count = len(self.enriched_df)
            self.update_status(f"Enrichment complete: {count} tokens")
        except Exception as exc:  # noqa: BLE001
            self.enriched_df = pd.DataFrame()
            self.update_status(f"Enrichment error: {exc}")
        self.show_dataframe(self.enriched_df)

    def handle_save(self) -> None:
        if self.enriched_df.empty:
            self.update_status("No enriched data to save")
            return
        filename, _ = QFileDialog.getSaveFileName(
            self, "Save CSV", "enriched.csv", "CSV Files (*.csv)"
        )
        if not filename:
            return
        self.enriched_df.to_csv(filename, index=False)
        self.update_status(f"Saved: {filename}")


def main() -> None:
    app = QApplication(sys.argv)
    window = RadarWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
