from typing import (
    List,
    Tuple,
)

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring_market_pair import LiquidityMirroringMarketPair
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring import LiquidityMirroringStrategy
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring_config_map import liquidity_mirroring_config_map


def start(self):
    primary_market = liquidity_mirroring_config_map.get("primary_market").value.lower()
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value.lower()
    mirrored_trading_pair = liquidity_mirroring_config_map.get("market_trading_pair_to_mirror").value
    two_sided_mirroring = liquidity_mirroring_config_map.get("two_sided_mirroring").value
    spread_percent = liquidity_mirroring_config_map.get("spread_percent").value
    max_exposure_base = liquidity_mirroring_config_map.get("max_exposure_base").value
    max_exposure_quote = liquidity_mirroring_config_map.get("max_exposure_quote").value
    try:
        primary_market_trading_pair: str = self._convert_to_exchange_trading_pair(primary_market, [mirrored_trading_pair])[0]
        mirrored_market_trading_pair: str = self._convert_to_exchange_trading_pair(mirrored_market, [mirrored_trading_pair])[0]
        primary_assets: List[Tuple[str, str]] = self._initialize_market_assets(primary_market, [primary_market_trading_pair])[0]
        secondary_assets: List[Tuple[str, str]] = self._initialize_market_assets(mirrored_market,
                                                                           [mirrored_market_trading_pair])[0]
    except ValueError as e:
        self._notify(str(e))
        return

    market_names: List[Tuple[str, List[str]]] = [(primary_market, [primary_market_trading_pair]),
                                                 (mirrored_market, [mirrored_market_trading_pair])]
    self._initialize_wallet(token_trading_pairs=list(set(primary_assets + secondary_assets)))
    self._initialize_markets(market_names)
    self.assets = set(primary_assets + secondary_assets)

    self.primary_market_trading_pair_tuples = [MarketTradingPairTuple(self.markets[primary_market], primary_market_trading_pair, primary_assets[0], primary_assets[1])]
    self.mirrored_market_trading_pair_tuples = [MarketTradingPairTuple(self.markets[mirrored_market], mirrored_market_trading_pair, secondary_assets[0], secondary_assets[1])]
    self.strategy = LiquidityMirroringStrategy(primary_market_pairs=self.primary_market_trading_pair_tuples,
                                               mirrored_market_pairs=self.mirrored_market_trading_pair_tuples,
                                               two_sided_mirroring=two_sided_mirroring,
                                               spread_percent=spread_percent,
                                               max_exposure_base=max_exposure_base,
                                               max_exposure_quote=max_exposure_quote,
                                               logging_options=LiquidityMirroringStrategy.OPTION_LOG_ALL)
