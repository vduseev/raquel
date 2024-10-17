from dataclasses import dataclass


@dataclass
class QueueStats:
    name: str
    total: int
    queued: int
    locked: int
    success: int
    failed: int
    cancelled: int

    @staticmethod
    def from_row(row: tuple) -> "QueueStats":
        name, total, queued, locked, success, failed, cancelled = row
        return QueueStats(
            name=name,
            total=total,
            queued=queued,
            locked=locked,
            success=success,
            failed=failed,
            cancelled=cancelled,
        )
