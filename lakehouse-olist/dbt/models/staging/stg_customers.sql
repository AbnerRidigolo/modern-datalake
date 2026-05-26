{{
    config(
        materialized="incremental",
        unique_key="customer_id",
        incremental_strategy="merge",
    )
}}

with source as (

    select * from {{ source('bronze', 'customers') }}

    {% if is_incremental() %}
        where _batch_date >= date_sub(current_date(), {{ var('lookback_days') }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _ingested_at desc
        ) as _rn
    from source

),

final as (

    select
        cast(customer_id as string)             as customer_id,
        cast(customer_unique_id as string)      as customer_unique_id,
        cast(customer_zip_code_prefix as int)   as customer_zip_code_prefix,
        initcap(trim(customer_city))            as customer_city,
        upper(trim(customer_state))             as customer_state,
        _batch_date,
        _ingested_at
    from deduplicated
    where _rn = 1
      and customer_id is not null

)

select * from final
