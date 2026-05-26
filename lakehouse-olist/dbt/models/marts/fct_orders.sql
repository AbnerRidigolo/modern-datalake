{{
    config(materialized="table")
}}

{#
    Tabela fato de pedidos. Grão: 1 linha por pedido.
    Inclui métricas calculadas: delivery_days, delivery_tier, revenue_bucket.
#}

with enriched as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_id,
    customer_id,
    customer_unique_id,
    customer_state,
    customer_city,
    order_status,
    order_purchase_at,
    order_delivered_customer_at,
    order_estimated_delivery_at,
    primary_payment_type,
    max_installments,
    payment_count,
    review_count,
    avg_review_score,
    total_payment_value,
    delivery_days,
    delivery_delay_days,

    -- classificação do tempo de entrega
    case
        when delivery_days is null then 'sem_entrega'
        when delivery_days <= 3 then 'rapido'
        when delivery_days <= 7 then 'medio'
        when delivery_days <= 15 then 'lento'
        else 'muito_lento'
    end as delivery_tier,

    -- faixa de receita do pedido
    case
        when total_payment_value <= 50 then 'baixo'
        when total_payment_value <= 150 then 'medio'
        when total_payment_value <= 500 then 'alto'
        else 'premium'
    end as revenue_bucket,

    -- entregue dentro do prazo estimado?
    case
        when delivery_delay_days is null then null
        when delivery_delay_days <= 0 then true
        else false
    end as delivered_on_time,

    cast(order_purchase_at as date) as order_date

from enriched
