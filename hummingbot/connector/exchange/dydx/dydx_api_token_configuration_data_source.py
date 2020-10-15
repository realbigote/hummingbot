import aiohttp
# import asyncio
# import logging
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    # Optional
)

from hummingbot.core.event.events import TradeType
from hummingbot.core.utils.async_utils import safe_ensure_future

TOKEN_CONFIGURATIONS_URL = 'https://api.dydx.exchange/v2/markets'


class DydxAPITokenConfigurationDataSource():
    """ Gets the token configuration on creation.

        Use DydxAPITokenConfigurationDataSource.create() to create.
    """

    def __init__(self):
        self._tokenid_lookup: Dict[str, int] = {}
        self._symbol_lookup: Dict[int, str] = {}
        self._token_configurations: Dict[int, Any] = {}
        self._decimals: Dict[int, Decimal] = {}

    @classmethod
    def create(cls):
        configuration_data_source = cls()
        safe_ensure_future(configuration_data_source._configure())

        return configuration_data_source

    async def _configure(self):
        async with aiohttp.ClientSession() as client:
            response: aiohttp.ClientResponse = await client.get(
                f"{TOKEN_CONFIGURATIONS_URL}"
            )

            if response.status >= 300:
                raise IOError(f"Error fetching active dydx token configurations. HTTP status is {response.status}.")

            response_dict: Dict[str, Any] = await response.json()

            for market in response_dict['markets']:
                details = response_dict['markets'][market]
                if "baseCurrency" in details:
                    self._token_configurations[details['baseCurrency']['currency']] = details['baseCurrency']
                    self._token_configurations[details['quoteCurrency']['currency']] = details['quoteCurrency']
                    self._tokenid_lookup[details['baseCurrency']['currency']] = details['baseCurrency']['soloMarketId']
                    self._tokenid_lookup[details['quoteCurrency']['currency']] = details['quoteCurrency']['soloMarketId']
                    self._symbol_lookup[details['baseCurrency']['soloMarketId']] = details['baseCurrency']['currency']
                    self._symbol_lookup[details['quoteCurrency']['soloMarketId']] = details['quoteCurrency']['currency']
                    self._decimals[details['baseCurrency']['soloMarketId']] = Decimal(f"10e{-(details['baseCurrency']['decimals'] + 1)}")
                    self._decimals[details['quoteCurrency']['soloMarketId']] = Decimal(f"10e{-(details['quoteCurrency']['decimals'] + 1)}")

    def get_bq(self, symbol: str) -> List[str]:
        """ Returns the base and quote of a trading pair """
        return symbol.split('-')

    def get_tokenid(self, symbol: str) -> int:
        """ Returns the token id for the given token symbol """
        return self._tokenid_lookup.get(symbol)

    def get_symbol(self, tokenid: int) -> str:
        """Returns the symbol for the given tokenid """
        return self._symbol_lookup.get(tokenid)

    def unpad(self, volume: str, tokenid: int) -> Decimal:
        """Converts the padded volume/size string into the correct Decimal representation
        based on the "decimals" setting from the token configuration for the referenced token
        """
        return Decimal(volume) * self._decimals[tokenid]

    def pad(self, volume: Decimal, tokenid: int) -> str:
        """Converts the volume/size Decimal into the padded string representation for the api
        based on the "decimals" setting from the token configuration for the referenced token
        """
        return str(Decimal(volume) // self._decimals[tokenid])

    def get_config(self, tokenid: int) -> Dict[str, Any]:
        """ Returns the token configuration for the referenced token id """
        return self._token_configurations.get(tokenid)

    def get_tokens(self) -> List[int]:
        return list(self._token_configurations.keys())

    def sell_buy_amounts(self, baseid, quoteid, amount, price, side) -> Tuple[int]:
        """ Returns the buying and selling amounts for unidirectional orders, based on the order
            side, price and amount and returns the padded values.
        """
        quote_amount = amount * price
        padded_amount = self.pad(amount, baseid)
        padded_quote_amount = self.pad(quote_amount, quoteid)

        if side is TradeType.SELL:
            return {
                "tokenSId": baseid,
                "tokenBId": quoteid,
                "amountS": padded_amount,
                "amountB": padded_quote_amount,
            }
        else:
            return {
                "tokenSId": quoteid,
                "tokenBId": baseid,
                "amountS": padded_quote_amount,
                "amountB": padded_amount,
            }
