# Pipeline Integration Tests

This directory contains comprehensive tests for the data pipeline integration, verifying each step of the process:

## Test Structure

### 1. `test_duckdb_join.py`
- **Purpose**: Verifies that the join between TVL data and SCD2 dimensions works correctly
- **Method**: Uses DuckDB in-memory to test the exact join query from SCD2Manager
- **Verifies**: 
  - All TVL records have matching dimension records
  - Join preserves all TVL data points
  - Data integrity and schema consistency

### 2. `test_dune_upload.py`
- **Purpose**: Verifies that historical facts data is properly uploaded to Dune Analytics
- **Method**: Tests both existing data and sample data uploads
- **Verifies**:
  - Table creation works correctly
  - Data upload succeeds
  - Schema compatibility with Dune API

### 3. `test_pipeline_steps.py`
- **Purpose**: End-to-end testing of the complete pipeline
- **Method**: Executes each step sequentially and verifies outputs
- **Verifies**:
  - Step 1: TVL data fetch and storage
  - Step 2: SCD2 dimensions fetch and storage
  - Step 3: Data join between TVL and SCD2
  - Step 4: Dune upload of joined data

## Running Tests

### Run All Tests
```bash
cd tests/pipeline_integration
python run_tests.py --all
```

### Run Individual Tests
```bash
# Test DuckDB join verification
python run_tests.py --test duckdb_join

# Test Dune upload verification
python run_tests.py --test dune_upload

# Test full pipeline steps
python run_tests.py --test pipeline_steps
```

### Run Individual Test Files
```bash
# Test DuckDB join
python test_duckdb_join.py

# Test Dune upload
python test_dune_upload.py

# Test full pipeline
python test_pipeline_steps.py
```

## Test Data Requirements

The tests expect the following data files to exist:
- `output/tvl_data_{today}.parquet` - TVL data from Step 1
- `output/pool_dim_scd2.parquet` - SCD2 dimensions from Step 2
- `output/historical_facts_{today}.parquet` - Joined data from Step 3

If these files don't exist, run the main pipeline first:
```bash
python -m tests.test_scheduler_simple
```

## Test Output

Each test provides detailed logging showing:
- Data counts at each step
- Sample data for verification
- Join verification results
- Upload success/failure status
- Any errors or warnings

## Expected Results

- **TVL Data**: ~496,661 records (all historical data points)
- **SCD2 Dimensions**: ~1,347 records (one per pool)
- **Historical Facts**: ~496,661 records (one for each TVL data point)
- **Dune Upload**: All historical facts records uploaded successfully

## Troubleshooting

### Common Issues

1. **Missing Data Files**: Run the main pipeline first to generate test data
2. **Dune API Errors**: Check API key and table permissions
3. **Join Mismatches**: Verify that all TVL pool_ids have corresponding SCD2 records
4. **Schema Errors**: Ensure all required columns are present in the data

### Debug Mode

Add debug logging to see more detailed information:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```
