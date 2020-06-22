#!/usr/bin/env python

from typing import NamedTuple

from hummingbot.market.market_base import MarketBase


class LiquidityMirroringMarketPair(NamedTuple):
    """
    Specifies a pair of markets for arbitrage
    """
    first: MarketBase
    second: MarketBase
