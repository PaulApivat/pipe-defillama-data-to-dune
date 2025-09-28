# 🚀 Production Deployment Checklist

## Pre-Deployment Testing

### ✅ Core Functionality Tests
- [ ] `python -m tests.test_pipeline` - Test pipeline with sample data
- [ ] `python debug/test_full_pipeline.py` - Test full production pipeline
- [ ] `python debug/test_github_actions.py` - Test GitHub Actions workflow
- [ ] Verify Dune table creation and data upload
- [ ] Verify incremental updates work correctly

### ✅ Data Quality Tests
- [ ] Verify `pool_old_clean` logic works for both patterns
- [ ] Verify data schema matches expectations
- [ ] Verify row counts are correct
- [ ] Verify no duplicate data in Dune

## GitHub Actions Setup

### ✅ Workflow Configuration
- [ ] Verify `.github/workflows/defillama_daily_pipeline.yml` exists
- [ ] Verify workflow runs daily at 6:00 AM UTC
- [ ] Verify `workflow_dispatch` is enabled for manual triggers
- [ ] Verify environment variables are set in GitHub Secrets

### ✅ Required GitHub Secrets
- [ ] `DUNE_API_KEY` - Dune Analytics API key
- [ ] Any other required environment variables

## Production Monitoring

### ✅ Initial Deployment
- [ ] Deploy to production branch
- [ ] Trigger manual workflow run
- [ ] Verify data appears in Dune table
- [ ] Check logs for any errors

### ✅ Daily Monitoring (First Week)
- [ ] Check daily workflow runs
- [ ] Verify data is being updated
- [ ] Monitor for any errors or failures
- [ ] Check Dune table for data quality

### ✅ Long-term Monitoring
- [ ] Set up alerts for workflow failures
- [ ] Monitor data freshness
- [ ] Check for any data quality issues
- [ ] Review logs periodically

## Rollback Plan

### ✅ If Issues Occur
- [ ] Disable GitHub Actions workflow
- [ ] Revert to previous working version
- [ ] Investigate and fix issues
- [ ] Re-enable workflow after fixes

## Success Criteria

### ✅ Pipeline is Production Ready When:
- [ ] All tests pass consistently
- [ ] Data uploads to Dune without errors
- [ ] Incremental updates work correctly
- [ ] GitHub Actions runs daily without failures
- [ ] Data quality is maintained over time

## Next Steps After Deployment

1. **Monitor for 1 week** - Ensure stable operation
2. **Set up alerts** - For workflow failures
3. **Documentation** - Update README with production info
4. **Optimization** - Consider performance improvements
5. **Scaling** - Add more data sources if needed
