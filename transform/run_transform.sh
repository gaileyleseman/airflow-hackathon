#!/bin/bash
# Legacy transform CronJob
# WARNING: exit codes are intentionally ignored for most steps.
# This means the job always "succeeds" from the scheduler's perspective
# even if dbt run or dbt test failed. This is one of the problems
# the Airflow migration addresses.

cd /app/dbt

echo "=== Installing dbt packages ==="
dbt deps || true

echo "=== dbt source freshness ==="
# Advisory only — exit code ignored
dbt source freshness || true

echo "=== dbt run ==="
# Exit code ignored — pipeline continues regardless
dbt run || true

echo "=== dbt test (full) ==="
# Advisory only — exit code ignored
dbt test || true

echo "=== dbt test --select tag:mart ==="
# This is the only test whose exit code is checked.
# If mart tests fail, export is skipped.
dbt test --select tag:mart
MART_TEST_EXIT=$?

if [ $MART_TEST_EXIT -eq 0 ]; then
    echo "=== Mart tests passed. Running export. ==="
    cd /app && python export.py
else
    echo "=== Mart tests FAILED (exit $MART_TEST_EXIT). Skipping export. ==="
fi

echo "=== Transform complete ==="
# Always exit 0 — legacy behaviour
# This masks failures from the scheduler, which is why Airflow migration
# will split this into observable tasks with proper exit code handling.
exit 0
