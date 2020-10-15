import aiohttp
from typing import Dict, Any

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

CENTRALIZED = False

EXAMPLE_PAIR = "WETH-DAI"

DEFAULT_FEES = [0.0, 0.2]

DYDX_ROOT_API = "https://api.dydx.exchange/v1"

KEYS = {
    "dydx_accountid":
        ConfigVar(key="dydx_accountid",
                  prompt="Enter your dydx account id >>> ",
                  required_if=using_exchange("dydx"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_exchangeid":
        ConfigVar(key="dydx_exchangeid",
                  prompt="Enter the dydx exchange id >>> ",
                  required_if=using_exchange("dydx"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_private_key":
        ConfigVar(key="dydx_private_key",
                  prompt="Enter your dydx private key >>> ",
                  required_if=using_exchange("dydx"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_api_key":
        ConfigVar(key="dydx_api_key",
                  prompt="Enter your dydx api key >>> ",
                  required_if=using_exchange("dydx"),
                  is_secure=True,
                  is_connect_key=True)
}


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    # dydx returns trading pairs in the correct format natively
    return exchange_trading_pair


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    # dydx expects trading pairs in the same format as hummingbot internally represents them
    return hb_trading_pair
