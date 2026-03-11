with source as (
    select
        elem as payload,
        ingested_at
    from {{ source('citizens', 'citizens') }},
         jsonb_array_elements(payload) as elem
),

unpacked as (
    select
        payload->>'citizen_id'                          as citizen_id,
        payload->>'full_name'                           as full_name,
        (payload->>'date_of_birth')::date               as date_of_birth,
        payload->>'municipality'                        as municipality,
        (payload->>'registration_date')::date           as registration_date,
        payload->>'status'                              as status,
        payload->>'service_tier'                        as service_tier,
        (payload->>'email_verified')::boolean           as email_verified,
        payload->>'national_id_hash'                    as national_id_hash,
        ingested_at
    from source
)

select * from unpacked
