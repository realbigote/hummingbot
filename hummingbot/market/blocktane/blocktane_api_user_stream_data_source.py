#!/usr/bin/env python

import asyncio
import aiohttp
import hashlib
import hmac
import logging
import os
import requests
import time
from typing import (
    AsyncIterable,
    Dict,
    Optional
)
import ujson
import websockets
from datetime import datetime
from typing import Optional
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

class BlocktaneAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _bausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    def __init__(self):
        self._Blocktane_client = BlocktaneClient()
        self._listen_for_user_stream_task = None
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                if self._listen_for_user_stream_task is not None:
                    self._listen_for_user_stream_task.cancel()
                self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                await self.wait_til_next_tick(seconds=60.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error while maintaining the user event listen key. Retrying after "
                                    "5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    async def _inner_messages(self, ws) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    self._last_recv_time = time.time()
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                        self._last_recv_time = time.time()
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except websockets.exceptions.ConnectionClosed:
            return
        finally:
            await ws.close()

    async def messages(self) -> AsyncIterable[str]:
        try:
            async with (await self.get_ws_connection()) as ws:
                async for msg in self._inner_messages(ws):
                    yield msg
        except asyncio.CancelledError:
            return

    async def get_ws_connection(self):
        stream_url: str = f"wss://opendax.tokamaktech.net/api/v2/ranger/private/?stream=order&stream=trade"
        self.logger().info(f"Reconnecting to {stream_url}.")

        nonce = str(int(datetime.timestamp(datetime.now()) * 1000))
        signature = hmac.new(self._Blocktane_client.secret_key.encode(),msg=(nonce + self._Blocktane_client.access_key).encode(),digestmod=hashlib.sha256).hexdigest()
        # Create the WS connection.
        ws = websockets.connect(stream_url, extra_headers={'X-Auth-Apikey': self._Blocktane_client.access_key,
                                                       'X-Auth-Nonce': nonce,
                                                       'X-Auth-Signature': signature})

        return ws

    async def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                async for message in self.messages():
                    decoded: Dict[str, any] = ujson.loads(message)
                    output.put_nowait(decoded)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. Retrying after 5 seconds...", exc_info=True)
                await asyncio.sleep(5.0)



class BlocktaneClient:
    def __init__(self):
        self.access_key = os.getenv("BLOCKTANE_EXCHANGE_API_KEY")
        self.secret_key = os.getenv("BLOCKTANE_SECRET_KEY")
