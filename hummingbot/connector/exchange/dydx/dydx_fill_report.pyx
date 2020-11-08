from decimal import Decimal

from hummingbot.core.event.events import TradeFee


cdef class DydxFillReport:
    def __init__(self, id: str, amount: Decimal, price: Decimal, fee: TradeFee):
        self.id = id
        self.amount = amount
        self.price = price
        self.fee = fee

    def __add__(self, DydxFillReport other):
        """ Returns a fill report representing the combination of fill reports such that the amount
            is the total of both filled amounts and the price is the vwap of the two fills.
            These combined fill reports do not have a valid id, so id is set to None.
        """
        value = self.value + other.value
        amount = self.amount + other.amount
        price = value / amount
        return DydxFillReport(None, amount, price)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, DydxFillReport other):
        return self.id == other.id

    def as_dict(self):
        return {
            "id": self.id,
            "amount": self.amount,
            "price": self.price,
            "fee": self.fee
        }

    @property
    def value(self) -> Decimal:
        return self.amount * self.price
