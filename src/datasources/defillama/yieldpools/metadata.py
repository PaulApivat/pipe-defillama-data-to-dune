from dataclasses import dataclass
from datetime import date
import polars as pl
from src.coreutils.request import new_session, get_data

YIELD_POOLS_ENDPOINT = "https://yields.llama.fi/pools"


@dataclass
class YieldPoolsMetadata:
    df: pl.DataFrame

    @classmethod
    def fetch(cls) -> "YieldPoolsMetadata":
        # Create a new session for this fetch operation
        session = new_session()
        try:
            response = get_data(session, YIELD_POOLS_ENDPOINT)
            return cls.of(data=response["data"], process_dt=date.today())
        finally:
            # Clean up the session
            session.close()

    @classmethod
    def of(cls, data: list[dict], process_dt: date) -> "YieldPoolsMetadata":
        records = []
        indexed = {}

        for pool in data:
            pool_id = pool["pool"]

            row = {
                "pool": pool["pool"],
                "protocol_slug": pool["project"],
                "chain": pool["chain"],
                "symbol": pool["symbol"],
                "underlying_tokens": pool.get("underlyingTokens", []),
                "reward_tokens": pool.get("rewardTokens", []),
                "il_risk": pool["ilRisk"],
                "is_stablecoin": pool["stablecoin"],
                "exposure": pool["exposure"],
                "pool_meta": pool["poolMeta"] or "main_pool",
            }

            records.append(row)
            indexed[pool_id] = row

        return cls(df=pl.DataFrame(records).with_columns(dt=pl.lit(process_dt)))
