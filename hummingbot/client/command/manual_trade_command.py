import pandas as pd
from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.market.market_base import MarketBase
from hummingbot.strategy import market_trading_pair_tuple

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class ManualTradeCommand:
    def trade(self, exchange_name: str, base_asset: str, quote_asset: str, amount: Decimal, is_buy: bool):
        market: MarketBase = self.markets[exchange_name]
        trading_pair_str: str = f"{base_asset}-{quote_asset}"
        trading_pair = market.convert_to_exchange_trading_pair(trading_pair_str)
        #order_id: str = market.c_sell(market_trading_pair_tuple.trading_pair, amount,
        #                                 order_type=order_type, price=price, kwargs=kwargs)

        

