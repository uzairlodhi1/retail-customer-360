{{
    config(
        materialized='incremental',
        schema='marts',
        unique_key=['report_date', 'country', 'channel', 'category'],
        incremental_strategy='merge',
        cluster_by=['report_date', 'country']
    )
}}

with orders as (
    select
        o.*,
        p.category
    from {{ ref('stg_orders') }}   o
    left join {{ source('raw', 'products') }} p using (product_id)

    {% if is_incremental() %}
        where o.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

completed as (
    select * from orders
    where status = 'completed'
),

summary as (
    select
        order_date                                              as report_date,
        country,
        channel,
        coalesce(category, 'Unknown')                          as category,
        count(order_id)                                        as total_orders,
        sum(quantity)                                          as total_units_sold,
        round(sum(total_amount), 2)                            as gross_revenue,
        round(avg(total_amount), 2)                            as avg_order_value,
        round(avg(discount_pct) * 100, 2)                     as avg_discount_pct,
        count(distinct customer_id)                            as unique_customers,
        current_timestamp()                                    as last_refreshed_at

    from completed
    group by 1, 2, 3, 4
)

select * from summary
order by report_date desc, gross_revenue desc
