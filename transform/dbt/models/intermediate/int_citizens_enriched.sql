with citizens as (
    select * from {{ ref('stg_citizens') }}
),

enriched as (
    select
        citizen_id,
        full_name,
        date_of_birth,
        municipality,
        registration_date,
        status,
        service_tier,
        email_verified,
        national_id_hash,
        ingested_at,

        -- Derived columns
        date_part('year', age(date_of_birth))::integer      as age,

        case
            when date_part('year', age(date_of_birth)) < 30 then 'young'
            when date_part('year', age(date_of_birth)) < 60 then 'middle_aged'
            else 'senior'
        end                                                  as age_group,

        date_part('year', age(registration_date))::integer  as years_registered,

        status = 'active'                                    as is_active

    from citizens
)

select * from enriched
