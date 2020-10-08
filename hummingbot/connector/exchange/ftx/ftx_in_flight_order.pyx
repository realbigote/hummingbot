from decimal import Decimal
from typing import Optional, Dict, Any

from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.connector.exchange.ftx.ftx_exchange import FtxExchange
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


cdef class FtxInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 created_at : float,
                 initial_state: str = "OPEN"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state
        )
        self.created_at = created_at

    def __repr__(self) -> str:
        return f"super().__repr__()" \
               f"created_at='{str(self.created_at)}'')"
    
    def to_json(self) -> Dict[str, Any]:
        response = super().to_json()
        response["created_at"] = str(self.created_at)
        return response

    @property
    def is_done(self) -> bool:
        return self.last_state in {"CLOSED", "CANCELLED"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"CANCELLED", "FAILURE"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"CANCELLED"}

    @property
    def order_type_description(self) -> str:
        order_type = "market" if self.order_type is OrderType.MARKET else "limit"
        side = "buy" if self.trade_type is TradeType.BUY else "sell"
        return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            FtxInFlightOrder retval = FtxInFlightOrder(
                data["client_order_id"],
                data["exchange_order_id"],
                data["trading_pair"],
                getattr(OrderType, data["order_type"]),
                getattr(TradeType, data["trade_type"]),
                Decimal(data["price"]),
                Decimal(data["amount"]),
                float(data["created_at"] if "created_at" in data else 0),
                data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval
