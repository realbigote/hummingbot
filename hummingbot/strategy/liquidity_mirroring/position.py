from decimal import Decimal
from functools import reduce

import logging

class Position:
    def __init__(self, price : Decimal, amount : Decimal):
        self.price : Decimal = price
        self.amount : Decimal = amount

    def __add__(self, other):
        if not isinstance(other, Position):
            raise RuntimeError(f"Position._add__ does not support addition with type {type(other).__name__}")

        total_base : Decimal = self.amount + other.amount
        if total_base.is_zero():
            return Position(Decimal(0), Decimal(0))

        total_quote : Decimal = self.amount * self.price + other.amount * other.price
        return Position(total_quote / total_base, total_base)

    def __str__(self):
        return f"({self.price}, {self.amount})"

zero_pos = Position(Decimal(0), Decimal(0))

class PositionManager:
    def __init__(self):
        self.total_loss = Decimal(0)
        self._trades : List[Trade] = []
        self._offsets : List[Trade] = []

    @property
    def avg_price(self) -> Decimal:
        return self._sum_trades().price

    @property
    def amount_to_offset(self) -> Decimal:
        return self._sum_trades().amount + self._sum_offsets().amount

    def register_trade(self, price : Decimal, amount : Decimal):
        self._trades.append(Position(price, amount))
        self._clean()

    def register_offset(self, price : Decimal, amount : Decimal):
        self._offsets.append(Position(price, amount))
        self._clean()

    def _clean(self):
        if self.amount_to_offset == 0:
            total_in = self._sum_trades()
            total_out = self._sum_offsets()
            diff = (total_out.price*total_out.amount + total_in.price*total_in.amount)
            self.total_loss += diff

            self._trades.clear()
            self._offsets.clear()
    
    def _sum_trades(self):
        return reduce(lambda x, y: x + y, self._trades, zero_pos)

    def _sum_offsets(self):
        return reduce(lambda x, y: x + y, self._offsets, zero_pos)

