import platform
from hummingbot.core.utils.async_utils import safe_ensure_future
from typing import TYPE_CHECKING


from hummingbot.core.utils.async_utils import safe_ensure_future
if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication


class CancelCommand:
    def cancel(self, exchange: str, order_id: str = None):
        safe_ensure_future(async_cancel(exchange[0], order_id))

    async def async_cancel(self, exchange: str, order_id: str = None):
        # invoke the market here
        if order_id is None:
            market.cancel_all()
        else:
            market.cancel(order_id)