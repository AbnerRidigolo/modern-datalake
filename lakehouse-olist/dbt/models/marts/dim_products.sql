{{
    config(materialized="table")
}}

{#
    Dimensão de produtos. Grão: 1 linha por product_id. Adiciona faixa de
    volume e flag de dimensões ausentes para análises de catálogo.
#}

with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,

    -- volume em cm3 (null se faltar alguma dimensão)
    (product_length_cm * product_height_cm * product_width_cm) as product_volume_cm3,

    case
        when product_weight_g is null then 'desconhecido'
        when product_weight_g <= 500 then 'leve'
        when product_weight_g <= 2000 then 'medio'
        else 'pesado'
    end as weight_tier,

    case
        when product_length_cm is null
          or product_height_cm is null
          or product_width_cm is null
        then true else false
    end as has_missing_dimensions

from products
