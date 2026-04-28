{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        trim(lower(email))                          as email,
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,
        upper(trim(country))                        as country,
        city,
        phone,
        registration_date::date                     as registration_date,
        case
            when lower(cast(is_active as varchar)) in ('true','1','yes') then true
            else false
        end                                         as is_active,
        created_at::timestamp                       as created_at,
        updated_at::timestamp                       as updated_at

    from source
    where customer_id is not null
      and email is not null
),

deduplicated as (
    select *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as _rn
    from renamed
)

select
    customer_id,
    email,
    first_name,
    last_name,
    country,
    city,
    phone,
    registration_date,
    is_active,
    created_at,
    updated_at
from deduplicated
where _rn = 1
