with logins as (
    select * from {{ ref('stg_logins') }}
),

activity as (
    select
        citizen_id,

        count(*)                                            as total_attempts,
        count(*) filter (where success = true)             as successful_logins,
        count(*) filter (where success = false)            as failed_logins,

        round(
            count(*) filter (where success = true)::numeric
            / nullif(count(*), 0) * 100, 2
        )                                                   as success_rate_pct,

        min(logged_in_at)                                   as first_login_at,
        max(logged_in_at)                                   as last_login_at,

        avg(session_duration_seconds)
            filter (where session_duration_seconds is not null)
            ::integer                                       as avg_session_seconds,

        mode() within group (order by device_type)         as most_used_device,

        count(*) filter (
            where success = false
            and failure_reason = 'account_locked'
        )                                                   as lockout_count,

        -- Flag accounts with 3+ consecutive recent failures
        count(*) filter (
            where success = false
            and logged_in_at >= now() - interval '7 days'
        ) >= 3                                              as is_at_risk

    from logins
    group by citizen_id
)

select * from activity
