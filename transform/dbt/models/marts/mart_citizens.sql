with citizens as (
    select * from {{ ref('int_citizens_enriched') }}
),

activity as (
    select * from {{ ref('int_login_activity') }}
),

final as (
    select
        c.citizen_id,
        c.full_name,
        c.date_of_birth,
        c.age,
        c.age_group,
        c.municipality,
        c.registration_date,
        c.years_registered,
        c.status,
        c.is_active,
        c.service_tier,
        c.email_verified,

        -- Login activity (null if citizen has no logins)
        coalesce(a.total_attempts, 0)       as total_login_attempts,
        coalesce(a.successful_logins, 0)    as successful_logins,
        coalesce(a.failed_logins, 0)        as failed_logins,
        a.success_rate_pct,
        a.first_login_at,
        a.last_login_at,
        a.avg_session_seconds,
        a.most_used_device,
        a.lockout_count,
        coalesce(a.is_at_risk, false)       as is_at_risk,

        -- Account health score
        case
            when a.citizen_id is null           then 'no_activity'
            when a.is_at_risk = true            then 'at_risk'
            when a.success_rate_pct < 50        then 'degraded'
            else                                     'healthy'
        end                                 as account_health

    from citizens c
    left join activity a using (citizen_id)
)

select * from final
