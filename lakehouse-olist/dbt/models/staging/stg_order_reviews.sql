{{
    config(
        materialized="incremental",
        unique_key="review_id",
        incremental_strategy="merge",
    )
}}

with source as (

    select * from {{ source('bronze', 'order_reviews') }}

    {% if is_incremental() %}
        where _batch_date >= date_sub(current_date(), {{ var('lookback_days') }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by review_id
            order by _ingested_at desc
        ) as _rn
    from source

),

final as (

    select
        cast(review_id as string)                       as review_id,
        cast(order_id as string)                        as order_id,
        cast(review_score as int)                       as review_score,
        nullif(trim(review_comment_title), '')          as review_comment_title,
        nullif(trim(review_comment_message), '')        as review_comment_message,
        cast(review_creation_date as timestamp)         as review_creation_at,
        cast(review_answer_timestamp as timestamp)      as review_answer_at,
        _batch_date,
        _ingested_at
    from deduplicated
    where _rn = 1
      and review_id is not null

)

select * from final
