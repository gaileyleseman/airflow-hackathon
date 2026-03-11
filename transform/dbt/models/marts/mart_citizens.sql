with citizens as (
    select * from {{ ref('int_citizens_enriched') }}
)

select
    citizen_id,
    full_name,
    date_of_birth,
    age,
    age_group,
    municipality,
    registration_date,
    years_registered,
    status,
    is_active,
    service_tier,
    email_verified
from citizens
