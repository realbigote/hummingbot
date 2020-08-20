from typing import TYPE_CHECKING
from decimal import Decimal

from hummingbot.market.market_base import MarketBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.core.event.events import OrderType
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class ManualTradeCommand:
    def trade(self, exchange_name: str, base_asset: str, quote_asset: str, amount: Decimal, buy_sell: str, price: Decimal = None):
        market: MarketBase = self.markets[exchange_name[0]]
        
        for trading_tuple in self.market_trading_pair_tuples:
            if (trading_tuple.market == market) and (trading_tuple.base_asset == base_asset[0].upper()) and (trading_tuple.quote_asset == quote_asset[0].upper()):
                market_trading_pair_tuple = trading_tuple
                break              
        
        strategy = StrategyBase()
        strategy.add_markets([market])
        if (buy_sell[0].lower() == "buy"):
            if price == None:
                order_id = strategy.buy_with_specific_market(market_trading_pair_tuple,Decimal(amount[0]),OrderType.MARKET)
            else:
                order_id = strategy.buy_with_specific_market(market_trading_pair_tuple,Decimal(amount[0]),OrderType.LIMIT,Decimal(price))
        elif (buy_sell[0].lower() == "sell"):
            if price == None:
                order_id = strategy.sell_with_specific_market(market_trading_pair_tuple,Decimal(amount[0]),OrderType.MARKET)
            else:
                order_id = strategy.sell_with_specific_market(market_trading_pair_tuple,Decimal(amount[0]),OrderType.LIMIT,Decimal(price)) 
        self._notify(f"{order_id}")


        

