{{
    config(
        materialized="incremental",
        unique_key="product_id",
        incremental_strategy="merge",
    )
}}

with source as (

    select * from {{ source('bronze', 'products') }}

    {% if is_incremental() %}
        where _batch_date >= date_sub(current_date(), {{ var('lookback_days') }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by product_id
            order by _ingested_at desc
        ) as _rn
    from source

),

final as (

    select
        cast(product_id as string)                                  as product_id,
        -- normaliza categoria nula para um rótulo explícito
        coalesce(nullif(trim(product_category_name), ''), 'sem_categoria')
            as product_category_name,
        cast(product_weight_g as int)                               as product_weight_g,
        cast(product_length_cm as int)                              as product_length_cm,
        cast(product_height_cm as int)                              as product_height_cm,
        cast(product_width_cm as int)                               as product_width_cm,
        cast(product_photos_qty as int)                             as product_photos_qty,
        _batch_date,
        _ingested_at
    from deduplicated
    where _rn = 1
      and product_id is not null

)

select * from final
