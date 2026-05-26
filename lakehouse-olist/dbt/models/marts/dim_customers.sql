{{
    config(materialized="table")
}}

{#
    Dimensão de clientes com SCD Tipo 2, materializada a partir do snapshot
    snap_customers. Expõe as colunas de versionamento (valid_from/valid_to/
    is_current) e uma surrogate key estável por versão.
#}

with snapshotted as (
    select * from {{ ref('snap_customers') }}
),

orders_metrics as (
    select
        customer_unique_id,
        count(distinct order_id)        as total_orders,
        sum(total_payment_value)        as lifetime_value,
        min(order_purchase_at)          as first_order_at,
        max(order_purchase_at)          as last_order_at
    from {{ ref('int_orders_enriched') }}
    where customer_unique_id is not null
    group by customer_unique_id
)

select
    {{ dbt_utils.generate_surrogate_key(['s.customer_unique_id', 's.dbt_valid_from']) }}
        as customer_sk,
    s.customer_unique_id,
    s.customer_id,
    s.customer_city,
    s.customer_state,
    s.customer_zip_code_prefix,
    coalesce(m.total_orders, 0)             as total_orders,
    coalesce(m.lifetime_value, 0.0)         as lifetime_value,
    m.first_order_at,
    m.last_order_at,
    -- colunas SCD2 vindas do snapshot
    s.dbt_valid_from                        as valid_from,
    s.dbt_valid_to                          as valid_to,
    case when s.dbt_valid_to is null then true else false end as is_current
from snapshotted s
left join orders_metrics m
    on s.customer_unique_id = m.customer_unique_id
