from __future__ import annotations

from PySide6.QtWidgets import QFrame, QLabel, QVBoxLayout, QWidget


def make_card(title: str, content: QWidget) -> QFrame:
    card = QFrame()
    card.setFrameShape(QFrame.StyledPanel)
    card.setStyleSheet(
        "QFrame { background-color: #1e1e1e; border: 1px solid #2a2a2a; border-radius: 6px; }"
    )
    layout = QVBoxLayout(card)
    layout.setContentsMargins(12, 12, 12, 12)
    layout.setSpacing(8)
    if title:
        title_label = QLabel(title)
        title_label.setStyleSheet("color: #c8c8c8; font-weight: 600;")
        layout.addWidget(title_label)
    content.setParent(card)
    layout.addWidget(content)
    return card
