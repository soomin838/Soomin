# STABLE Release Checklist

## Local Validation
- `python scripts/validate_stable_clickbait_sanitizer.py`
- `python scripts/run_stability_suite.py`
- `python scripts/smoke_check_ready.py`
- `python scripts/generate_ops_report.py --days 7`
- `python scripts/generate_tuning_plan.py --days 7`

## Runtime Files To Monitor
- `storage/logs/run_metrics.jsonl`
- `storage/reports/weekly_ops_report.md`
- `storage/reports/tuning_plan.md`
