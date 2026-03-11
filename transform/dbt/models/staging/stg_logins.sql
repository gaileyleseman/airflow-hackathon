with source as (
    select
        elem as payload,
        ingested_at
    from {{ source('logins', 'logins') }},
         jsonb_array_elements(payload) as elem
),

unpacked as (
    select
        payload->>'login_id'                                        as login_id,
        payload->>'citizen_id'                                      as citizen_id,
        (payload->>'logged_in_at')::timestamp                       as logged_in_at,
        (payload->>'logged_out_at')::timestamp                      as logged_out_at,
        (payload->>'session_duration_seconds')::integer             as session_duration_seconds,
        payload->>'device_type'                                     as device_type,
        (payload->>'success')::boolean                              as success,
        payload->>'failure_reason'                                  as failure_reason,
        payload->>'ip_region'                                       as ip_region,
        ingested_at
    from source
)

select * from unpacked
