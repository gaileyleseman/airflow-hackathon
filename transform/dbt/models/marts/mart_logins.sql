with logins as (
    select * from {{ ref('stg_logins') }}
),

citizens as (
    select * from {{ ref('int_citizens_enriched') }}
),

final as (
    select
        l.login_id,
        l.citizen_id,
        c.full_name,
        c.municipality,
        c.age_group,
        c.service_tier,
        l.logged_in_at,
        l.logged_out_at,
        l.session_duration_seconds,
        l.device_type,
        l.success,
        l.failure_reason,
        l.ip_region,
        date_part('hour', l.logged_in_at)::integer    as login_hour,
        to_char(l.logged_in_at, 'Dy')                 as login_day_of_week
    from logins l
    left join citizens c using (citizen_id)
)

select * from final
