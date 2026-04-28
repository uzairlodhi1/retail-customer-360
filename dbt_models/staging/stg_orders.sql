{{
    config(
        materialized='incremental',
        schema='staging',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ source('raw', 'orders') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        quantity::integer                                       as quantity,
        unit_price::numeric(18,2)                              as unit_price,
        coalesce(discount_pct, 0)::numeric(5,4)                as discount_pct,
        total_amount::numeric(18,2)                            as total_amount,
        upper(coalesce(currency, 'USD'))                       as currency,
        lower(trim(status))                                    as status,
        lower(trim(channel))                                   as channel,
        order_date::date                                       as order_date,
        shipped_date::date                                     as shipped_date,
        upper(trim(country))                                   as country,
        created_at::timestamp                                  as created_at,
        updated_at::timestamp                                  as updated_at

    from source
    where order_id     is not null
      and customer_id  is not null
      and product_id   is not null
      and quantity     > 0
      and total_amount >= 0
)

select * from cleaned
