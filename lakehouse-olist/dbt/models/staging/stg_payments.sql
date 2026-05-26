{{
    config(
        materialized="incremental",
        unique_key="payment_key",
        incremental_strategy="merge",
    )
}}

with source as (

    select * from {{ source('bronze', 'payments') }}

    {% if is_incremental() %}
        where _batch_date >= date_sub(current_date(), {{ var('lookback_days') }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by order_id, payment_sequential
            order by _ingested_at desc
        ) as _rn
    from source

),

final as (

    select
        -- chave sintética: pagamento é único por pedido + sequencial
        concat_ws('-', order_id, cast(payment_sequential as string)) as payment_key,
        cast(order_id as string)                as order_id,
        cast(payment_sequential as int)         as payment_sequential,
        lower(trim(payment_type))               as payment_type,
        cast(payment_installments as int)       as payment_installments,
        cast(payment_value as double)           as payment_value,
        _batch_date,
        _ingested_at
    from deduplicated
    where _rn = 1
      and order_id is not null

)

select * from final
