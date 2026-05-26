{{
    config(materialized="view")
}}

{#
    Enriquece cada pedido com cliente, agregados de pagamento e avaliação.
    Grão: 1 linha por order_id. Serve de base para fct_orders e marts.
#}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

payments_agg as (
    select
        order_id,
        sum(payment_value)              as total_payment_value,
        max(payment_installments)       as max_installments,
        count(*)                        as payment_count,
        -- tipo de pagamento de maior valor no pedido
        max_by(payment_type, payment_value) as primary_payment_type
    from {{ ref('stg_payments') }}
    group by order_id
),

reviews_agg as (
    select
        order_id,
        avg(cast(review_score as double)) as avg_review_score,
        count(*)                          as review_count
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_at,
    o.order_approved_at,
    o.order_delivered_customer_at,
    o.order_estimated_delivery_at,
    coalesce(p.total_payment_value, 0.0)        as total_payment_value,
    coalesce(p.payment_count, 0)                as payment_count,
    p.max_installments,
    p.primary_payment_type,
    r.avg_review_score,
    coalesce(r.review_count, 0)                 as review_count,
    -- dias de entrega: do aprovado até a entrega ao cliente
    datediff(o.order_delivered_customer_at, o.order_approved_at) as delivery_days,
    -- atraso vs. estimativa (negativo = entregue antes do previsto)
    datediff(o.order_delivered_customer_at, o.order_estimated_delivery_at) as delivery_delay_days
from orders o
left join customers c on o.customer_id = c.customer_id
left join payments_agg p on o.order_id = p.order_id
left join reviews_agg r on o.order_id = r.order_id
