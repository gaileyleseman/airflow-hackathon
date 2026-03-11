-- Fails if either mart table is empty.
-- An empty mart means ingestion silently skipped missing source files,
-- so the pipeline should not export stale/missing data.
{{ config(tags=['mart']) }}

select 'mart_citizens' as mart, count(*) as row_count
from {{ ref('mart_citizens') }}
having count(*) = 0

union all

select 'mart_logins' as mart, count(*) as row_count
from {{ ref('mart_logins') }}
having count(*) = 0
