from typing import overload
from contextlib import contextmanager
from dataclasses import dataclass


class A:

    @staticmethod
    def f(x: int) -> None:
        print(f"Static: {x}")

    def f(self, x: int) -> None:
        print(f"Self: {x}")


@contextmanager
def foo():
    try:
        print("Enter")
        yield "123dfg"

        print("Middle")

    except Exception as e:
        print(f"Exception: {e}")

    finally:
        print("Exit")


@dataclass
class B:
    x: int
    y: int

    @staticmethod
    def from_row(row: list[int]) -> "B":
        return B(row[0], row[1])


if __name__ == "__main__":
    b = B.from_row([1, 2])
    print(b)
