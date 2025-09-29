# DeFiLlama Data Pipeline to Dune

A pipeline that extracts Pool TVL and APY data from DeFiLlama APIs and prepares it for Dune consumption.

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   EXTRACT       │    │   TRANSFORM      │    │      LOAD       │
│                 │    │                  │    │                 │
│ raw_pools_*.pq  │───▶│ pool_dimensions  │    │                 │
│ raw_tvl_*.pq    │───▶│                  │───▶│ defillama_      │
│                 │    │                  │    │ historical_     │
│                 │    │ historical_facts │    │ facts           │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   ORCHESTRATION  │    │   SCHEDULER     │
                       │                  │    │                 │
                       │ - Daily facts    │    │ - Daily cron    │
                       │   only           │    │ - Error handling│
                       └──────────────────┘    └─────────────────┘

```


## Project Structure

```
pipe-defillama-data-to-dune/
├── src/
│   ├── extract/                 
│   │   ├── defillama_api.py     
│   │   ├── data_fetcher.py      
│   │   └── schemas.py           

│   ├── transformation/          
│   │   ├── schemas.py           
│   │   ├── validators.py        
│   │   └── transformers.py      

│   ├── load/                    
│   │   ├── local_storage.py     
│   │   └── dune_uploader.py     

│   ├── orchestration/           
│   │   ├── pipeline.py          
│   │   └── scheduler.py         
│   │   └── incremental_pipeline.py       (# decrecated)
│   │   └── pipeline_test.py              (# decrecated)

├── .github/                     
│   ├── workflows/  
│   │   └── defillama_daily_pipeline.yml       

├── tests/ 
│   ├── test_extract_layer.py
│   ├── test_transform_layer.py
│   ├── test_full_pipeline.py
│   ├── test_github_actions.py
│   ├── test_incremental_caching.py       # for debugging

├── output/  
├── debug/ 
├── requirements.txt
├── scripts/                        
├── notebooks/                                     
│   
├── README.md/                        
│  
└── requirements.txt                        
```

## Primary Operations & Architectural Principles

This pipeline follows Persistent ETL 

### 1. **Extract** (Dimension & Fact Raw Data)
- No imports from transform or load
- Pure I/O operations only
- Handles API rate limiting and retries
- Stores raw data for testing

### 2. **Transform** (Fact Table)
- Pure functions (input → output)
- No I/O operations
- Unit testable and deterministic
- Stores transformed data for testing

### 3. **Load**
- Handles local files and external uploads
- No business logic

### 4. **Orchestration**
- Pure workflow coordination
- Composes extract, transform, and load
- No business logic


- **Functional Pipeline**: Method chaining for composable data transformations
- **Abstract Classes**: For tracking state, external connections
- **Modular Design**: Separation of concerns with clear boundaries
- **Schema-First**: Pydantic validation and Polars schemas
- **Reliability**: Exponential backoff, retry logic, and idempotent operations
- **Performance**: Polars for fast data processing, DuckDB for analytics
- **Data Quality**: Comprehensive validation and quality checks
- **Observability**: Detailed logging and error handling

## Quick Start

### Prerequisites

- Python 3.10+
- Virtual environment (recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/PaulApivat/pipe-defillama-data-to-dune.git
cd pipe-defillama-data-to-dune

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Testing

```bash
# 1. Fetch raw data 
python -m tests.test_extract_layer

# 2. Transformations
python -m tests.test_transform_layer

# 3. End-to-End testing and initial load to Dune
python -m tests.test_full_pipeline

# 4. Setup Github secrets before testing Github Actions
python -m tests.test_github_actions

# 4. Fast data exploration via SQL
duckdb
```

## Data Model

### Star Schema Design

**Fact Table: `HISTORICAL_FACTS_SCHEMA`**
- Historical TVL and APY data over time
- Primary key: `(pool_id, timestamp)`
- Measures: `tvl_usd`, `apy`, `apy_base`, `apy_reward`

**Dimension Table: `POOL_DIM_SCHEMA`**
- Pool metadata and state (only current, no history)
- Primary key: `pool`
- Attributes: `protocol_slug`, `chain`, `symbol`, `pool_old`, etc.

### Simplifed Transformation (No slowly-changing dimension complexity needed)

**Joined Table: `HISTORICAL_FACTS_SCHEMA`**
- Initial Load to Dune (join Dimension & Fact table)
- Primary Key: `pool_old_clean`, `timestamp`


### Data Dictionary

#### Fact Table - `HISTORICAL_FACTS_SCHEMA`
| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | STRING | Data point timestamp (ISO 8601) |
| `tvl_usd` | FLOAT64 | Total Value Locked in USD |
| `apy` | FLOAT64 | Annual Percentage Yield |
| `apy_base` | FLOAT64 | Base APY (without rewards) |
| `apy_reward` | FLOAT64 | Reward APY component |
| `pool_id` | STRING | Unique pool identifier |

#### Dimension Table - `POOL_DIM_SCHEMA`
| Column | Type | Description |
|--------|------|-------------|
| `pool` | STRING | Pool identifier (join key) |
| `protocol_slug` | STRING | Protocol name (e.g., 'curve-dex') |
| `chain` | STRING | Blockchain network |
| `symbol` | STRING | Pool symbol (e.g., 'WETH-USDC') |
| `underlying_tokens` | LIST[STRING] | List of underlying token addresses |
| `reward_tokens` | LIST[STRING] | List of reward token addresses |
| `timestamp` | STRING | ISO 8601 timestamp of current state |
| `tvl_usd` | FLOAT64 | Current Total Value Locked in USD |
| `apy` | FLOAT64 | Current Annual Percentage Yield |
| `apy_base` | FLOAT64 | Current Base APY (without rewards) |
| `apy_reward` | FLOAT64 | Current Reward APY |
| `pool_old` | STRING | Legacy pool identifier |



## Development

### Adding New Data Sources

1. Create new target projects in `src/extract/data_fetcher.py`
1. Add new endpoints in `src/extract/defillama_api.py`
2. Define Pydantic schemas in `src/extract/schemas.py`
3. Implement data fetching logic
4. Add data quality checks
5. Run `tests.test_extract_layer`


### Data Exploration

```python
import duckdb
import polars as pl

# Connect to DuckDB
conn = duckdb.connect(":memory:")

# Query Pool Dimensions
raw_pools = conn.execute("""
    SELECT 
        *
    FROM read_parquet('output/raw_pools_2025-09-28.parquet')
""").fetchdf()

# Query Facts
raw_tvl = conn.execute("""
    SELECT 
        *
    FROM read_parquet('output/raw_tvl_2025-09-28.parquet')
""").fetchdf()
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Dune Analytics Configuration
DUNE_API_KEY=your_dune_api_key
```

## Monitoring & Observability

### Logging
- Request timing and success rates
- Data quality metrics
- Error tracking and alerting
- Performance monitoring

### Data Quality Metrics
- Record counts and completeness
- Data freshness and staleness
- Schema validation success rates
- Duplicate detection results

## Future Enhancements

### Immediate (Next Sprint)
- [x] Unit test suite with pytest
- [x] CI/CD pipeline with GitHub Actions
- [ ] Data quality dashboard
- [x] Automated alerting system

### Medium Term
- [ ] Real-time streaming pipeline
- [ ] Data lineage tracking
- [ ] Advanced anomaly detection
- [ ] Multi-source data integration

### Long Term
- [ ] Machine learning pipeline
- [ ] Advanced analytics and forecasting
- [ ] Multi-cloud deployment
- [ ] Data governance framework

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- DeFiLlama for providing comprehensive DeFi data APIs
- Dune for powerful blockchain analytics platform
- Polars team for fast DataFrame library
- DuckDB team for embedded analytical database

---

