#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import os
import requests
import time
from typing import Optional
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

BLOCKTANE_API_ENDPOINT = "https://opendax.tokamaktech.net/api/v2/"
BLOCKTANE_SIGNIN = "barong/identity/sessions"
BLOCKTANE_USER_STREAM_BALANCES = "peatio/account/balances"
BLOCKTANE_USER_STREAM_DEPOSITS = "peatio/account/deposits"
BLOCKTANE_USER_STREAM_ORDERS = "peatio/market/orders"
BLOCKTANE_USER_STREAM_TRADES = "peatio/market/trades"


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
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def get_listen_key(self):
        response = requests.post(f"{BLOCKTANE_API_ENDPOINT}{BLOCKTANE_SIGNIN}", params={"email": self._Blocktane_client.email,
                                                                                        "password": self._Blocktane_client.password})
        if response.status_code != 200:
            raise IOError(f"Error fetching blocktane user stream listen key. HTTP status is {response.status_code}.")
        return response.cookies

    async def ping_listen_key(self, listen_key: str) -> bool:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{BLOCKTANE_API_ENDPOINT}{BLOCKTANE_USER_STREAM_BALANCES}",
                                  cookies = self._current_listen_key) as response:
                data: [str, any] = await response.json()
                if "code" in data:
                    self.logger().warning(f"Failed to refresh the listen key {listen_key}: {data}")
                    return False
                return True

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                if self._current_listen_key is None:
                    self._current_listen_key = self.get_listen_key()
                    self.logger().debug(f"Obtained listen key {self._current_listen_key}.")
                    if self._listen_for_user_stream_task is not None:
                        self._listen_for_user_stream_task.cancel()
                    self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                    await self.wait_til_next_tick(seconds=60.0)
                success: bool = await self.ping_listen_key(self._current_listen_key)
                if not success:
                    self._current_listen_key = None
                    if self._listen_for_user_stream_task is not None:
                        self._listen_for_user_stream_task.cancel()
                        self._listen_for_user_stream_task = None
                    continue
                self.logger().debug(f"Refreshed listen key {self._current_listen_key}.")

                await self.wait_til_next_tick(seconds=60.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error while maintaining the user event listen key. Retrying after "
                                    "5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                message = requests.get(f"{BLOCKTANE_API_ENDPOINT}{BLOCKTANE_USER_STREAM_BALANCES}", cookies=self._current_listen_key)
                output.put_nowait(message.json())

                message = requests.get(f"{BLOCKTANE_API_ENDPOINT}{BLOCKTANE_USER_STREAM_ORDERS}", cookies=self._current_listen_key)
                output.put_nowait(message.json())

                message = requests.get(f"{BLOCKTANE_API_ENDPOINT}{BLOCKTANE_USER_STREAM_TRADES}", cookies=self._current_listen_key)
                output.put_nowait(message.json())
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. Retrying after 5 seconds...", exc_info=True)
                time.sleep(5)


class BlocktaneClient:
    def __init__(self):
        self.email = os.getenv("BLOCKTANE_EXCHANGE_API_KEY")
        self.password = os.getenv("BLOCKTANE_PW")
