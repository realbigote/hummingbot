import aiohttp


LOOPRING_ROOT_API = "https://api.loopring.io"
LOOPRING_WS_KEY_PATH = "/v2/ws/key"

async def get_ws_api_key():
    async with aiohttp.ClientSession() as client:
        response: aiohttp.ClientResponse = await client.get(
            f"{LOOPRING_ROOT_API}{LOOPRING_WS_KEY_PATH}"
        )
        if response.status != 200:
            raise IOError(f"Error getting WS key. Server responded with status: {response.status}.")

        response_dict : Dict[str, Any] = await response.json()
        return response_dict['data']