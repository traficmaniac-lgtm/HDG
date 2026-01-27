from __future__ import annotations

from PySide6.QtWidgets import QFrame, QVBoxLayout, QWidget


def make_card(title: str, content: QWidget) -> QFrame:
    card = QFrame()
    card.setFrameShape(QFrame.StyledPanel)
    card.setStyleSheet(
        "QFrame { background-color: #1e1e1e; border: 1px solid #2a2a2a; border-radius: 6px; }"
    )
    layout = QVBoxLayout(card)
    layout.setContentsMargins(12, 12, 12, 12)
    layout.setSpacing(8)
    content.setParent(card)
    layout.addWidget(content)
    return card
