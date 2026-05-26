{{
    config(materialized="table")
}}

{#
    Model wide de KPIs de marketing por estado e cohort de aquisição.
    Inclui: cohort mensal de clientes, LTV estimado, taxa de recompra.
    Grão: 1 linha por (estado, cohort_month).
#}

with orders as (
    select
        customer_unique_id,
        customer_state,
        order_id,
        total_payment_value,
        cast(order_purchase_at as date) as order_date
    from {{ ref('int_orders_enriched') }}
    where customer_unique_id is not null
      and order_status not in ('canceled', 'unavailable')
),

customer_level as (
    select
        customer_unique_id,
        customer_state,
        -- cohort = mês da primeira compra
        date_trunc('MONTH', min(order_date)) as cohort_month,
        count(distinct order_id)             as orders_count,
        sum(total_payment_value)             as customer_revenue
    from orders
    group by customer_unique_id, customer_state
),

state_cohort as (
    select
        customer_state,
        cohort_month,
        count(distinct customer_unique_id)                         as customers,
        sum(orders_count)                                          as total_orders,
        sum(customer_revenue)                                      as total_revenue,
        -- clientes com mais de 1 pedido = recompraram
        count(distinct case when orders_count > 1 then customer_unique_id end)
            as repeat_customers
    from customer_level
    group by customer_state, cohort_month
)

select
    customer_state,
    cohort_month,
    customers,
    total_orders,
    repeat_customers,
    round(total_revenue, 2)                                   as total_revenue,
    -- ticket médio por pedido
    round(total_revenue / nullif(total_orders, 0), 2)         as avg_order_value,
    -- LTV estimado = receita média por cliente do cohort
    round(total_revenue / nullif(customers, 0), 2)            as estimated_ltv,
    -- taxa de recompra (%)
    round(100.0 * repeat_customers / nullif(customers, 0), 2) as repurchase_rate_pct
from state_cohort
order by customer_state, cohort_month
