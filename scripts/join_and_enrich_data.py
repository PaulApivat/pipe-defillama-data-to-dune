"""
Join and enrich TVL , tvl_data.parquet, (fact) data with current state, current_state.parquet (dimension) data 
using DuckDb for in-memory processing and SQL joins
"""

import duckdb
from pathlib import Path
from src.coreutils.data import DataConverter


def main():
    """Main function to join and enrich data"""
    print(" >>> Starting data join and enrichment process...")

    # Step 1: create duckdb connection
    print("\n #1. Creating DuckDB connections...")
    conn = duckdb.connect(":memory:")
    print(f"  ✅ DuckDB connection established in memory")

    # Step 2: Load both parquet files
    print("\n #2. Loading parquet files...")
    load_parquet_file(conn)

    # Step 3: Perform join and enrichment
    print("\n #3. Performing join and enrichment...")
    enriched_data = perform_join(conn)

    # Step 4:  Validate results
    print("\n #4. Validating results...")
    validate_results(conn, enriched_data)

    # Cleanup
    conn.close()
    print(f"\n ✅ Process completed successfully.DuckDB connection closed")


def load_parquet_file(conn):
    """Load both parquet files into DuckDB"""
    # Define file paths
    tvl_path = Path("output/tvl_data.parquet")
    current_state_path = Path("output/current_state.parquet")

    # check if file exist
    if not tvl_path.exists():
        raise FileNotFoundError(f"TVL file not found: {tvl_path}")
    if not current_state_path.exists():
        raise FileNotFoundError(f"Current state file not found: {current_state_path}")

    # Load TVL data (fact table)
    print(f"   Loading TVL data from {tvl_path}")
    conn.execute(
        f"""
        CREATE TABLE tvl_data AS 
        SELECT * FROM read_parquet('{tvl_path}')        
    """
    )

    # Load current state data (dimension table)
    print(f"   Loading current state data from {current_state_path}")
    conn.execute(
        f"""
        CREATE TABLE current_state AS 
        SELECT * FROM read_parquet('{current_state_path}')
    """
    )

    # Show basic info about loaded data
    tvl_count = conn.execute("SELECT COUNT(*) FROM tvl_data").fetchone()[0]
    current_count = conn.execute("SELECT COUNT(*) FROM current_state").fetchone()[0]

    print(f"  ✅ TVL data: {tvl_count:,} rows")
    print(f"  ✅ Current state data: {current_count:,} rows")


def perform_join(conn):
    """Perform the join between fact and dimension tables"""
    print("   Executing join query...")

    # join query
    join_query = """
        SELECT 
            tvl.timestamp,
            tvl.tvlUsd,
            tvl.apy as historical_apy,
            tvl.apyBase as historical_apy_base,
            tvl.apyReward as historical_apy_reward,
            tvl.pool_id,
            
            -- Current state enrichment
            current.protocol_slug,
            current.chain,
            current.symbol,
            current.underlying_tokens,
            current.reward_tokens,
            current.pool_meta,
            current.pool_old,
            current.tvl_usd as current_tvl_usd,
            current.apy as current_apy,
            current.apy_base as current_apy_base,
            current.apy_reward as current_apy_reward,
            current.volume_usd_1d,
            current.volume_usd_7d,
            current.stablecoin,
            current.il_risk,
            current.exposure,
            current.outlier,
            current.predictions
            
        FROM tvl_data tvl
        LEFT JOIN current_state current 
            ON tvl.pool_id = current.pool 
    """

    # execute join query
    result = conn.execute(join_query)
    enriched_data = result.fetchall()

    print(f"  ✅ Join completed: {len(enriched_data):,} enriched records")

    return enriched_data


def validate_results(conn, enriched_data):
    """Validate the join results"""
    print("   Running validation checks...")

    # check 1: total records
    total_records = len(enriched_data)
    print(f"  ✅ Total records: {total_records:,}")

    # check 2: records with succeessful joins
    successful_joins = conn.execute(
        """
        SELECT COUNT(*) 
        FROM tvl_data t
        LEFT JOIN current_state c ON t.pool_id = c.pool 
        WHERE c.pool IS NOT NULL
    """
    ).fetchone()[0]

    print(f"  ✅ Successful joins: {successful_joins:,}")
    print(f"  ✅ Success rate: {(successful_joins / total_records) * 100:.2f}%")

    # check 3: unique pools
    unique_pools = conn.execute(
        """
        SELECT COUNT(DISTINCT pool_id) FROM tvl_data 
    """
    ).fetchone()[0]

    print(f"  ✅ Unique pools: {unique_pools:,}")

    # check 4: sample of enriched data
    print("\n  📊 Sample enriched data:")
    sample = conn.execute(
        """
        SELECT 
            timestamp,
            pool_id,
            pool_old,
            protocol_slug,
            chain,
            symbol,
            tvlUsd,
            current_tvl_usd
        FROM (
            SELECT 
                tvl.timestamp,
                tvl.pool_id,
                current.pool_old,
                current.protocol_slug,
                current.chain,
                current.symbol,
                tvl.tvlUsd,
                current.tvl_usd as current_tvl_usd
            FROM tvl_data tvl
            LEFT JOIN current_state current ON tvl.pool_id = current.pool
        ) 
        LIMIT 5
    """
    ).fetchall()

    for row in sample:
        # Handle None values and different data types
        timestamp = row[0][:10] if row[0] else "N/A"
        pool_id = row[1][:8] + "..." if row[1] else "N/A"
        pool_old = row[2][:8] + "..." if row[2] else "N/A"
        protocol = row[3] if row[3] else "N/A"
        chain = row[4] if row[4] else "N/A"
        symbol = row[5] if row[5] else "N/A"

        # Format numeric values safely
        tvl_usd = f"${row[6]:,.0f}" if row[6] is not None else "N/A"
        current_tvl = f"${row[7]:,.0f}" if row[7] is not None else "N/A"

        print(
            f"      {timestamp} | {pool_id} | {pool_old} | {protocol} | {chain} | {symbol} | {tvl_usd} | {current_tvl}"
        )


if __name__ == "__main__":
    main()
