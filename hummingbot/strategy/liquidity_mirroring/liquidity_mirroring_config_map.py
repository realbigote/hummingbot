from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_exchange,
    validate_market_trading_pair,
)
from hummingbot.client.settings import (
    required_exchanges,
    EXAMPLE_PAIRS,
)

def is_valid_mirroring_market_trading_pair(value: str) -> bool:
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
    in_mirrored_market = validate_market_trading_pair(mirrored_market, value)
    return in_mirrored_market

def is_valid_primary_market_trading_pair(value: str) -> bool:
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    in_primary_market = validate_market_trading_pair(primary_market, value)
    return in_primary_market

def mirror_trading_pair_prompt():
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
    example = EXAMPLE_PAIRS.get(mirrored_market)
    return "Enter the token trading pair from %s you would like to mirror %s >>> " \
           % (mirrored_market, f" (e.g. {example})" if example else "")

def primary_trading_pair_prompt():
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    example = EXAMPLE_PAIRS.get(primary_market)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (primary_market, f" (e.g. {example})" if example else "")

liquidity_mirroring_config_map = {
    "strategy": ConfigVar(key="strategy",
                          prompt="",
                          default="liquidity_mirroring"
                          ),
    "primary_market": ConfigVar(
        key="primary_market",
        prompt="Enter your primary exchange name >>> ",
        validator=validate_exchange,
        prompt_on_new=True,
        on_validated=lambda value: required_exchanges.append(value)),
    "mirrored_market": ConfigVar(
        key="mirrored_market",
        prompt="Enter the name of the exchange which you would like to mirror >>> ",
        validator=validate_exchange,
        prompt_on_new=True,
        on_validated=lambda value: required_exchanges.append(value)),
    "market_trading_pair_to_mirror": ConfigVar(
        key="market_trading_pair_to_mirror",
        prompt=mirror_trading_pair_prompt,
        prompt_on_new=True,
        validator=lambda value: is_valid_mirroring_market_trading_pair(value),
    ),
    "primary_market_trading_pair": ConfigVar(
        key="primary_market_trading_pair",
        prompt=primary_trading_pair_prompt,
        prompt_on_new=True,
        validator=lambda value: is_valid_primary_market_trading_pair(value),
    ),
    "equivalent_tokens": ConfigVar(
        key="equivalent_tokens",
        prompt=None,
        type_str="list",
        required_if=lambda: False,
        default=[["USDT", "USDC", "USDS", "DAI", "PAX", "TUSD", "USD", "ZUSD"],
        ["XETH", "ETH", "WETH", "FTH"], ["BTC", "WBTC"], ["ZRX"], ["XTZ"]],
    ),
    "two_sided_mirroring": ConfigVar(
        key="two_sided_mirroring",
        prompt="Two-Sided Mirroring Threshold in base currency (inf for one-sided) >>> ",
        default=float("inf"),
        prompt_on_new=True,
        type_str="float" 
    ),
    "spread_percent": ConfigVar(
        key="spread_percent",
        prompt="Enter your desired spread percentage >>> ",
        default=0.001,
        prompt_on_new=True,
        type_str="float"
    ),
    "max_exposure_base": ConfigVar(
        key="max_exposure_base",
        prompt="Enter the max desired exposure for the base asset >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
    "max_exposure_quote": ConfigVar(
        key="max_exposure_quote",
        prompt="Enter the max desired exposure for the quote asset >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
    "max_offset_loss": ConfigVar(
        key="max_offset_loss",
        prompt="Enter your maximum tolerated one-time offset loss >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
    "max_total_offset_loss": ConfigVar(
        key="max_total_offset_loss",
        prompt="Enter your maximum tolerated total offset loss >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
    "min_primary_amount": ConfigVar(
        key="min_primary_amount",
        prompt="Enter the min amount per trade on the primary exchange >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
    "min_mirroring_amount": ConfigVar(
        key="min_mirroring_amount",
        prompt="Enter the min amount per trade on the mirrored exchange >>> ",
        prompt_on_new=True,
        type_str="float"
    ),
}

