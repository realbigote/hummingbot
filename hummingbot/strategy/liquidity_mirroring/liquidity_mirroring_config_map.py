from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    is_exchange,
    is_valid_market_trading_pair,
)
from hummingbot.client.settings import (
    required_exchanges,
    EXAMPLE_PAIRS,
)

def is_valid_mirroring_market_trading_pair(value: str) -> bool:
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    mirrored_market = liquidity_mirroring_config_map.get("mirrored_market").value
    return (is_valid_market_trading_pair(primary_market, value) and is_valid_market_trading_pair(mirrored_market, value))

def mirror_trading_pair_prompt():
    primary_market = liquidity_mirroring_config_map.get("primary_market").value
    example = EXAMPLE_PAIRS.get(primary_market)
    return "Enter the token trading pair you would like to mirror on %s%s >>> " \
           % (primary_market, f" (e.g. {example})" if example else "")

liquidity_mirroring_config_map = {
    "primary_market": ConfigVar(
        key="primary_market",
        prompt="Enter your primary exchange name >>> ",
        validator=is_exchange,
        on_validated=lambda value: required_exchanges.append(value)),
    "mirrored_market": ConfigVar(
        key="mirrored_market",
        prompt="Enter the name of the exchange which you would like to mirror >>> ",
        validator=is_exchange,
        on_validated=lambda value: required_exchanges.append(value)),
    "market_trading_pair_to_mirror": ConfigVar(
        key="market_trading_pair_to_mirror",
        prompt=mirror_trading_pair_prompt,
        validator=is_valid_mirroring_market_trading_pair),
}

