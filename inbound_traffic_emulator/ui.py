import ctypes
import tkinter as tk
from tkinter import ttk, messagebox

from backend import EmulatorConfig, InboundTrafficEmulator, Mode


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Inbound Traffic Emulator")
        self.geometry("560x440")
        self.resizable(False, False)

        self.emulator = InboundTrafficEmulator()
        self._build_ui()
        self._tick_stats()

    def _build_ui(self) -> None:
        pad = {"padx": 10, "pady": 6}

        self.remote_ip = tk.StringVar()
        self.remote_port = tk.StringVar()
        self.process_name = tk.StringVar()
        self.interface_index = tk.StringVar()
        self.mode_var = tk.StringVar(value=Mode.FREEZE.value)
        self.lag_ms = tk.StringVar(value="500")
        self.limit_kbps = tk.StringVar(value="64")
        self.loss_percent = tk.StringVar(value="10")
        self.freeze_hard = tk.BooleanVar(value=True)

        row = 0
        ttk.Label(self, text="Remote IP").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.remote_ip, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Remote Port").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.remote_port, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Process (name contains)").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.process_name, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Interface Index").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.interface_index, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Mode").grid(row=row, column=0, sticky="w", **pad)
        ttk.Combobox(
            self,
            textvariable=self.mode_var,
            values=[m.value for m in Mode],
            state="readonly",
            width=28,
        ).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Lag (ms)").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.lag_ms, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Limit (KB/s)").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.limit_kbps, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Label(self, text="Loss (%)").grid(row=row, column=0, sticky="w", **pad)
        ttk.Entry(self, textvariable=self.loss_percent, width=30).grid(row=row, column=1, sticky="ew", **pad)

        row += 1
        ttk.Checkbutton(
            self,
            text="FREEZE: Hard Freeze (buffer until STOP)",
            variable=self.freeze_hard,
        ).grid(row=row, column=0, columnspan=2, sticky="w", **pad)

        row += 1
        button_frame = ttk.Frame(self)
        button_frame.grid(row=row, column=0, columnspan=2, sticky="ew", **pad)
        ttk.Button(button_frame, text="START", command=self.on_start).pack(side="left", padx=5)
        ttk.Button(button_frame, text="STOP", command=self.on_stop).pack(side="left", padx=5)

        row += 1
        self.status_var = tk.StringVar(value="Inactive")
        self.counter_var = tk.StringVar(value="packets intercepted: 0 | forwarded: 0 | dropped: 0 | buffered: 0")
        ttk.Label(self, textvariable=self.status_var, foreground="blue").grid(row=row, column=0, columnspan=2, sticky="w", **pad)

        row += 1
        ttk.Label(self, textvariable=self.counter_var).grid(row=row, column=0, columnspan=2, sticky="w", **pad)

    def _collect_config(self) -> EmulatorConfig:
        return EmulatorConfig(
            remote_ip=self.remote_ip.get().strip(),
            remote_port=int(self.remote_port.get() or 0),
            mode=Mode(self.mode_var.get()),
            lag_ms=int(self.lag_ms.get() or 0),
            limit_kbps=int(self.limit_kbps.get() or 1),
            loss_percent=float(self.loss_percent.get() or 0),
            process_name=self.process_name.get().strip(),
            interface_index=int(self.interface_index.get() or 0),
            freeze_hard=self.freeze_hard.get(),
        )

    def on_start(self) -> None:
        try:
            config = self._collect_config()
            self.emulator.update_config(config)
            self.emulator.start()
            self.status_var.set("Active")
        except Exception as exc:
            messagebox.showerror("Start error", str(exc))

    def on_stop(self) -> None:
        try:
            self.emulator.stop()
            self.status_var.set("Inactive")
        except Exception as exc:
            messagebox.showerror("Stop error", str(exc))

    def _tick_stats(self) -> None:
        st = self.emulator.stats()
        self.counter_var.set(
            f"packets intercepted: {st['intercepted']} | forwarded: {st['forwarded']} | "
            f"dropped: {st['dropped']} | buffered: {st['buffered']}"
        )
        self.after(500, self._tick_stats)


def _is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def run_app() -> None:
    if not _is_admin():
        messagebox.showwarning(
            "Admin required",
            "Запустите программу от имени администратора для корректной работы WinDivert.",
        )
    app = App()
    app.mainloop()
