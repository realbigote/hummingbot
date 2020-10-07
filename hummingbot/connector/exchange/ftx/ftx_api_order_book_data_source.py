#!/usr/bin/env python
import aiohttp
import asyncio
import cachetools.func
import logging
import pandas as pd
import requests
import signalr_aio
import simplejson
import time
import ujson
import websockets

from async_timeout import timeout
from base64 import b64decode
from decimal import Decimal
from signalr_aio import Connection
from signalr_aio.hubs import Hub
from typing import Optional, List, Dict, AsyncIterable, Any
from websockets.exceptions import ConnectionClosed
from zlib import decompress, MAX_WBITS

from hummingbot.connector.exchange.ftx.ftx_active_order_tracker import FtxActiveOrderTracker
from hummingbot.connector.exchange.ftx.ftx_order_book import FtxOrderBook
from hummingbot.connector.exchange.ftx.ftx_order_book_tracker_entry import FtxOrderBookTrackerEntry
from hummingbot.connector.exchange.ftx.ftx_utils import convert_from_exchange_trading_pair, convert_to_exchange_trading_pair
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

EXCHANGE_NAME = "ftx"

FTX_REST_URL = "https://ftx.com/api"
FTX_EXCHANGE_INFO_PATH = "/markets"
FTX_WS_FEED = "wss://ftx.com/ws/"

MAX_RETRIES = 20
SNAPSHOT_TIMEOUT = 10.0
NaN = float("nan")
API_CALL_TIMEOUT = 5.0


class FtxAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _ftxaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._ftxaobds_logger is None:
            cls._ftxaobds_logger = logging.getLogger(__name__)
        return cls._ftxaobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._websocket_connection: Optional[Connection] = None
        self._snapshot_msg: Dict[str, any] = {}

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading pair as index and include USDVolume, baseAsset and quoteAsset
        """
        market_path_url = f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}"

        async with aiohttp.ClientSession() as client:

            market_response = await client.get(market_path_url)

            market_response: aiohttp.ClientResponse = market_response

            if market_response.status != 200:
                raise IOError(
                    f"Error fetching active ftx markets information. " f"HTTP status is {market_response.status}."
                )

            raw_market_data = market_response.json()

            market_data = [data for data in raw_market_data if raw_market_data["type"] == "spot"]

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="name")
            all_markets.rename(
                {"baseCurrency": "baseAsset", "quoteCurrency": "quoteAsset", "volumeUsd24h": "USDVolume"}, axis="columns", inplace=True
            )

            await client.close()
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
                    exc_info=True
                )
        return self._trading_pairs

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(url=f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{convert_to_exchange_trading_pair(trading_pair)}")
        record = resp.json()
        result = (Decimal(record.get("bid", "0")) + Decimal(record.get("ask", "0"))) / Decimal("2")
        return result if result else None

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}", timeout=API_CALL_TIMEOUT) as response:
                    if response.status == 200:
                        all_trading_pairs: Dict[str, Any] = await response.json()
                        valid_trading_pairs: list = []
                        for item in all_trading_pairs["result"]:
                            if item["type"] == "spot":
                                valid_trading_pairs.append(item["name"])
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in valid_trading_pairs:
                            converted_trading_pair: Optional[str] = \
                                convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list
        except Exception:
            pass

        return []

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        
        async with client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{trading_pair}/orderbook") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching FTX market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = simplejson.loads(await response.text(), parse_float=Decimal)

            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot["data"] = snapshot["result"]
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = FtxOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"market": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    active_order_tracker: FtxActiveOrderTracker = FtxActiveOrderTracker()
                    retval[trading_pair] = FtxOrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book, active_order_tracker)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
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
                    yield msg
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
                stream_url: str = f"{FTX_WS_FEED}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "trades",
                            "market": pair
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "trades" and msg["type"] == "update":
                                for trade in msg["data"]:
                                    trade_msg: OrderBookMessage = FtxOrderBook.trade_message_from_exchange(msg, trade)
                                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                stream_url: str = f"{FTX_WS_FEED}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "orderbook",
                            "market": pair
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "orderbook" and msg["type"] == "update":
                                order_book_message: OrderBookMessage = FtxOrderBook.diff_message_from_exchange(msg, msg["data"]["time"])
                                output.put_nowait(order_book_message)
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
                stream_url: str = f"{FTX_WS_FEED}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "orderbook",
                            "market": pair
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "orderbook" and msg["type"] == "partial":
                                order_book_message: OrderBookMessage = FtxOrderBook.snapshot_message_from_exchange(msg, msg["data"]["time"])
                                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)


