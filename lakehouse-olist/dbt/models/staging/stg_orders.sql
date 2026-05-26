{{
    config(
        materialized="incremental",
        unique_key="order_id",
        incremental_strategy="merge",
    )
}}

with source as (

    select * from {{ source('bronze', 'orders') }}

    {% if is_incremental() %}
        -- só processa lotes recentes em execuções incrementais
        where _batch_date >= date_sub(current_date(), {{ var('lookback_days') }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id
            order by _ingested_at desc
        ) as _rn
    from source

),

final as (

    select
        cast(order_id as string)                          as order_id,
        cast(customer_id as string)                       as customer_id,
        lower(trim(order_status))                         as order_status,
        cast(order_purchase_timestamp as timestamp)       as order_purchase_at,
        cast(order_approved_at as timestamp)              as order_approved_at,
        cast(order_delivered_carrier_date as timestamp)   as order_delivered_carrier_at,
        cast(order_delivered_customer_date as timestamp)  as order_delivered_customer_at,
        cast(order_estimated_delivery_date as timestamp)  as order_estimated_delivery_at,
        _batch_date,
        _ingested_at
    from deduplicated
    where _rn = 1
      and order_id is not null

)

select * from final
