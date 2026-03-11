-- NOTE: This model LEFT JOINs to citizens, so logins from unknown citizens
-- (i.e., citizen_id values not found in the citizens table) are preserved
-- with null citizen columns. The is_unknown_citizen flag identifies these rows.
--
-- Unknown citizens can occur due to the decoupled scraper design where the
-- logins scraper samples from citizen_ids.json independently. In production
-- this could also happen if a citizen is deleted but their login history
-- remains.

with logins as (
    select * from {{ ref('stg_logins') }}
),

citizens as (
    select
        citizen_id,
        full_name,
        municipality,
        service_tier,
        is_active
    from {{ ref('int_citizens_enriched') }}
),

final as (
    select
        l.login_id,
        l.citizen_id,
        c.full_name,
        c.municipality          as citizen_municipality,
        c.service_tier,
        c.is_active             as citizen_is_active,

        l.logged_in_at,
        l.logged_out_at,
        l.session_duration_seconds,
        l.device_type,
        l.success,
        l.failure_reason,
        l.ip_region,

        -- Flag if login region differs from citizen municipality
        l.ip_region != c.municipality   as is_region_mismatch,

        -- Flag logins for unknown citizens (FK not in citizens table)
        c.citizen_id is null            as is_unknown_citizen,

        date_part('hour', l.logged_in_at)::integer  as login_hour,
        to_char(l.logged_in_at, 'Dy')               as login_day_of_week

    from logins l
    left join citizens c using (citizen_id)
)

select * from final
