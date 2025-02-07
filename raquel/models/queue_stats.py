from dataclasses import dataclass


@dataclass
class QueueStats:
    name: str
    total: int
    queued: int
    claimed: int
    success: int
    failed: int
    expired: int
    exhausted: int
    cancelled: int

    @staticmethod
    def from_row(row: tuple) -> "QueueStats":
        (
            name,
            total,
            queued,
            claimed,
            success,
            failed,
            expired,
            exhausted,
            cancelled,
        ) = row
        return QueueStats(
            name=name,
            total=total,
            queued=queued,
            claimed=claimed,
            success=success,
            failed=failed,
            expired=expired,
            exhausted=exhausted,
            cancelled=cancelled,
        )
