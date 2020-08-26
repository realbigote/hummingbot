#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import re
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.market.novadax.novadax_order_book import NovadaxOrderBook
from hummingbot.market.novadax.novadax_order_book_tracker_entry import NovadaxOrderBookTrackerEntry
from hummingbot.market.novadax.novadax_active_order_tracker import NovadaxActiveOrderTracker

TRADING_PAIR_FILTER = re.compile(r"(BTC|ETH|USDT)$")

SNAPSHOT_REST_URL = "https://api.novadax.com/v1/market/depth"
DIFF_STREAM_URL = "wss://ws.novadax.com/socket.io/?EIO=3&transport=websocket"
TICKER_PRICE_CHANGE_URL = "https://api.novadax.com//v1/market/tickers"
EXCHANGE_INFO_URL = "https://api.novadax.com//v1/common/symbols"


class NovadaxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading_pair as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:

            market_response, exchange_response = await safe_gather(
                client.get(TICKER_PRICE_CHANGE_URL),
                client.get(EXCHANGE_INFO_URL)
            )
            market_response: aiohttp.ClientResponse = market_response
            exchange_response: aiohttp.ClientResponse = exchange_response

            if market_response.status != 200:
                raise IOError(f"Error fetching novadax markets information. "
                              f"HTTP status is {market_response.status}.")
            if exchange_response.status != 200:
                raise IOError(f"Error fetching novadax exchange information. "
                              f"HTTP status is {exchange_response.status}.")

            market_data = await market_response.json()
            exchange_data = await exchange_response.json()

            trading_pairs: Dict[str, Any] = {item["symbol"]: {k: item[k] for k in ["baseCurrency", "quoteCurrency"]}
                                             for item in exchange_data["data"]
                                             if item["status"] == "ONLINE"}

            market_data: List[Dict[str, Any]] = [{**item, **trading_pairs[item["symbol"]]}
                                                 for item in market_data["data"]
                                                 if item["symbol"] in trading_pairs]

            # Build the data frame.
            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="symbol")
            btc_price: float = float(all_markets.loc["BTC_USDT"].lastPrice)
            eth_price: float = float(all_markets.loc["ETH_USDT"].lastPrice)
            usd_volume: float = [
                (
                    quoteVolume * btc_price if trading_pair.endswith("BTC") else
                    quoteVolume * eth_price if trading_pair.endswith("ETH") else
                    quoteVolume
                )
                for trading_pair, quoteVolume in zip(all_markets.index,
                                                     all_markets.quoteVolume24h.astype("float"))]
            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "volume"] = all_markets.quoteVolume24h

            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        async with client.get(f"{SNAPSHOT_REST_URL}?symbol={trading_pair}") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching novadax market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            data = data["data"]

            # Need to add the symbol into the snapshot message for the Kafka message queue.
            # Because otherwise, there'd be no way for the receiver to know which market the
            # snapshot belongs to.

            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, NovadaxOrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = NovadaxOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"symbol": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    active_order_tracker: NovadaxActiveOrderTracker = NovadaxActiveOrderTracker()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = NovadaxOrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book, active_order_tracker)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    # Each 1000 limit snapshot costs 10 requests and novadax rate limit is 20 requests per second.
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    if msg[0:2] == '42':
                        yield msg[2:]
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                stream_url: str = f"{DIFF_STREAM_URL}"
                for trading_pair in trading_pairs:
                    subscription_request = f'''42["join", "{trading_pair}"]'''
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        await ws.send(subscription_request)
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg[0] == "basic_with_trades":
                                for trade in msg[1]["trades"]:
                                    trade_msg: OrderBookMessage = NovadaxOrderBook.trade_message_from_exchange(trade,msg[1]["basic"])
                                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                stream_url: str = f"{DIFF_STREAM_URL}"
                for trading_pair in trading_pairs:
                    subscription_request = f'''42["join", "{trading_pair}_depth_0"]'''
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        await ws.send(subscription_request)
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg[0] == "depth":
                                order_book_message: OrderBookMessage = NovadaxOrderBook.snapshot_message_from_exchange(
                                    msg[1]["depth"], time.time(), msg[1]["basic"])
                                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

