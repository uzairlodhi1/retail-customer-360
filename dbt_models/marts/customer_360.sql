{{
    config(
        materialized='table',
        schema='marts',
        cluster_by=['country', 'customer_segment'],
        post_hook="ALTER TABLE {{ this }} CLUSTER BY (country, customer_segment)"
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'completed'
),

order_metrics as (
    select
        customer_id,
        count(order_id)                                         as total_orders,
        sum(total_amount)                                       as total_spend,
        avg(total_amount)                                       as avg_order_value,
        min(order_date)                                         as first_order_date,
        max(order_date)                                         as last_order_date,
        datediff('day', max(order_date), current_date())        as days_since_last_order

    from orders
    group by 1
),

preferred_channel as (
    select distinct
        customer_id,
        first_value(channel) over (
            partition by customer_id
            order by count(*) over (partition by customer_id, channel) desc
        )                                                       as preferred_channel
    from orders
),

segmented as (
    select
        c.customer_id,
        c.first_name || ' ' || c.last_name                     as full_name,
        c.email,
        c.country,
        c.city,
        c.registration_date,
        c.is_active,
        coalesce(m.total_orders, 0)                            as total_orders,
        coalesce(m.total_spend, 0)                             as total_spend,
        coalesce(m.avg_order_value, 0)                         as avg_order_value,
        m.first_order_date,
        m.last_order_date,
        m.days_since_last_order,
        ch.preferred_channel,
        case
            when m.total_spend  >= 1000 and m.total_orders >= 5  then 'VIP'
            when m.days_since_last_order > 180                    then 'At-Risk'
            when m.total_orders = 1                               then 'New'
            when m.total_orders is null                           then 'No Orders'
            else 'Regular'
        end                                                     as customer_segment,
        current_timestamp()                                     as last_refreshed_at

    from customers c
    left join order_metrics  m  on c.customer_id = m.customer_id
    left join preferred_channel ch on c.customer_id = ch.customer_id
)

select * from segmented
