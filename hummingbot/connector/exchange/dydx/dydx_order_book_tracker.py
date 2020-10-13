import asyncio
import logging
# import sys
from collections import deque, defaultdict
from typing import (
    Optional,
    Deque,
    List,
    Dict,
    # Set
)
from hummingbot.connector.exchange.dydx.dydx_active_order_tracker import DYDXActiveOrderTracker
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.connector.exchange.dydx.dydx_order_book import DYDXOrderBook
from hummingbot.connector.exchange.dydx.dydx_order_book_message import DYDXOrderBookMessage
from hummingbot.connector.exchange.dydx.dydx_api_order_book_data_source import DYDXAPIOrderBookDataSource
from hummingbot.connector.exchange.dydx.dydx_auth import DYDXAuth
from hummingbot.connector.exchange.dydx.dydx_api_token_configuration_data_source import DYDXAPITokenConfigurationDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class DYDXOrderBookTracker(OrderBookTracker):
    _dobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._dobt_logger is None:
            cls._dobt_logger = logging.getLogger(__name__)
        return cls._dobt_logger

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        rest_api_url: str = "https://api.dydx.exchange/v1",
        websocket_url: str = "wss://api.dydx.exchange/v1/ws",
        token_configuration: DYDXAPITokenConfigurationDataSource = None,
        dydx_auth: str = ""
    ):
        super().__init__(
            DYDXAPIOrderBookDataSource(
                trading_pairs=trading_pairs,
                rest_api_url=rest_api_url,
                websocket_url=websocket_url,
                token_configuration=token_configuration,
            ),
            trading_pairs)
        self._order_books: Dict[str, DYDXOrderBook] = {}
        self._saved_message_queues: Dict[str, Deque[DYDXOrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._dydx_auth = DYDXAuth(dydx_auth)
        self._token_configuration: DYDXAPITokenConfigurationDataSource = token_configuration
        self.token_configuration
        self._active_order_trackers: Dict[str, DYDXActiveOrderTracker] = defaultdict(lambda: DYDXActiveOrderTracker(self._token_configuration))

    @property
    def token_configuration(self) -> DYDXAPITokenConfigurationDataSource:
        if not self._token_configuration:
            self._token_configuration = DYDXAPITokenConfigurationDataSource.create()
        return self._token_configuration

    @property
    def exchange_name(self) -> str:
        return "dydx"

    async def _track_single_book(self, trading_pair: str):
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: DYDXOrderBook = self._order_books[trading_pair]
        active_order_tracker: DYDXActiveOrderTracker = self._active_order_trackers[trading_pair]
        while True:
            try:
                message: DYDXOrderBookMessage = None
                saved_messages: Deque[DYDXOrderBookMessage] = self._saved_message_queues[trading_pair]
                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = active_order_tracker.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.content["startVersion"])

                elif message.type is OrderBookMessageType.SNAPSHOT:
                    s_bids, s_asks = active_order_tracker.convert_snapshot_message_to_order_book_row(message)
                    order_book.apply_snapshot(s_bids, s_asks, message.timestamp)
                    self.logger().debug("Processed order book snapshot for %s.", trading_pair)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds.",
                )
                await asyncio.sleep(5.0)
