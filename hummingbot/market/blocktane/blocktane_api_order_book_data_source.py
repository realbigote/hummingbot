#!/usr/bin/env python

import asyncio
import aiohttp
from collections import namedtuple
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
from hummingbot.market.blocktane.blocktane_order_book import BlocktaneOrderBook

BLOCKTANE_REST_URL = "https://bolsa.tokamaktech.net/api/v2/peatio/public"
DIFF_STREAM_URL = "wss://bolsa.tokamaktech.net/api/v2/ranger/public"
TICKER_PRICE_CHANGE_URL = "https://bolsa.tokamaktech.net/api/v2/peatio/public/markets/tickers"
EXCHANGE_INFO_URL = "https://bolsa.tokamaktech.net/api/v2/peatio/public/markets"

OrderBookRow = namedtuple("Book", ["price", "amount"])


class BlocktaneAPIOrderBookDataSource(OrderBookTrackerDataSource):

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
        async with aiohttp.ClientSession() as client:

            market_response, ticker_response = await safe_gather(
                client.get(f"{BLOCKTANE_REST_URL}/markets"),
                client.get(f"{BLOCKTANE_REST_URL}/markets/tickers")
            )
            market_response: aiohttp.ClientResponse = market_response

            if market_response.status != 200:
                raise IOError(f"Error fetching blocktane markets information. "
                              f"HTTP status is {market_response.status}.")

            if ticker_response.status != 200:
                raise IOError(f"Error fetching blocktane tickers information. "
                              f"HTTP status is {ticker_response.status}.")

            market_data = await market_response.json()
            ticker_data = await ticker_response.json()

            exchange_markets: Dict[str, Any] = {item["id"]: {"baseAsset": item["base_unit"],
                                                             "quoteAsset": item["quote_unit"],
                                                             "volume": ticker_data[item["id"]]["ticker"]["volume"],
                                                             "lastPrice": ticker_data[item["id"]]["ticker"]["last"]}
                                                for item in market_data if item["state"] == "enabled"}
            columns = [item["id"] for item in market_data if item["state"] == "enabled"]
            
            all_markets: pd.DataFrame = pd.DataFrame(exchange_markets, columns = columns)
            all_markets = all_markets.swapaxes("index", "columns")
            eth_price: float = float(all_markets.loc["ethusd"].lastPrice)
            trst_price: float = float(all_markets.loc["trstusd"].lastPrice)
            fth_price: float = float(all_markets.loc["fthusd"].lastPrice)
            usd_volume: float = [
                (
                    volume * trst_price if trading_pair.endswith("trst") else
                    volume * eth_price if trading_pair.endswith("eth") else
                    volume * fth_price if trading_pair.endswith("fth") else
                    volume
                )
                for trading_pair, volume in zip(all_markets.index, all_markets.volume.astype("float"))]
            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "volume"] = all_markets.volume

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
        request_url: str = f"{BLOCKTANE_REST_URL}/markets/{trading_pair}/depth"

        async with client.get(request_url) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching blocktane market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")

            data: Dict[str, Any] = await response.json()

            # Need to add the symbol into the snapshot message for the Kafka message queue.
            # Because otherwise, there'd be no way for the receiver to know which market the
            # snapshot belongs to.

            return _prepare_snapshot(trading_pair, data["bids"], data["asks"])

    def _parse_raw_update(self, pair: str, raw_response: str) -> OrderBookMessage:
        """
        Parses raw update, if price for a tracked order identified by ID is 0, then order is deleted
        Returns OrderBookMessage
        """
        _, content = ujson.loads(raw_response)

        if isinstance(content, list) and len(content) == 3:
            order_id = content[0]
            price = content[1]
            amount = content[2]

            os = self._get_tracked_order_by_id(order_id)
            order = os["order"]
            side = os["side"]

            if order is not None:
                # this is not a new order. Either update it or delete it
                if price == 0:
                    self._untrack_order(order_id)
                    # print("-------------- Deleted order %d" % (order_id))
                    return self._generate_delete_message(pair, order.price, side)
                else:
                    self._track_order(order_id, OrderBookRow(price, abs(amount), order.update_id), side)
                    return None
            else:
                # this is a new order unless the price is 0, just track it and create message that
                # will add it to the order book
                if price != 0:
                    # print("-------------- Add order %d" % (order_id))
                    return self._generate_add_message(pair, price, amount)
        return None

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: Dict[str, Any] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = BlocktaneOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    # Each 1000 limit snapshot costs 10 requests and blocktane rate limit is 20 requests per second.
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
                ws_path: str = "&stream=".join([f"{trading_pair}.trades" for trading_pair in trading_pairs])
                stream_url: str = f"{DIFF_STREAM_URL}/?stream={ws_path}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        if (list(msg.keys())[0].endswith("trades")):
                            trade_msg: OrderBookMessage = BlocktaneOrderBook.trade_message_from_exchange(msg)
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
                ws_path: str = "&stream=".join([f"{trading_pair}.ob-inc" for trading_pair in trading_pairs])
                stream_url: str = f"{DIFF_STREAM_URL}/?stream={ws_path}"

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        key = list(msg.keys())[0]
                        if ('ob-inc' in key):
                            pair = re.sub(r'\.ob-inc', '', key)
                            parsed_msg = {"pair": pair,
                                          "bids": msg[key]["bids"],
                                          "asks": msg[key]["asks"]}
                            order_book_message: OrderBookMessage = BlocktaneOrderBook.diff_message_from_exchange(parsed_msg, time.time())
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
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = BlocktaneOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            # Be careful not to go above blocktane's API rate limits.
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)


def _prepare_snapshot(pair: str, bids: List, asks: List) -> Dict[str, Any]:
    """
    Return structure of three elements:
        symbol: traded pair symbol
        bids: List of OrderBookRow for bids
        asks: List of OrderBookRow for asks
    """

    format_bids = [OrderBookRow(i[0], i[1]) for i in bids]
    format_asks = [OrderBookRow(i[0], i[1]) for i in asks]

    return {
        "symbol": pair,
        "bids": format_bids,
        "asks": format_asks,
    }
