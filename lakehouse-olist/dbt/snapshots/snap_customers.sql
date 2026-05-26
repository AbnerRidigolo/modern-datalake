{#
    Snapshot SCD Tipo 2 dos clientes. O dbt mantém o histórico de mudanças
    de cidade/estado por customer_unique_id, gerando dbt_valid_from /
    dbt_valid_to automaticamente. Estratégia 'check' compara as colunas
    monitoradas a cada execução de `dbt snapshot`.
#}
{% snapshot snap_customers %}

{{
    config(
        target_schema='gold',
        unique_key='customer_unique_id',
        strategy='check',
        check_cols=['customer_city', 'customer_state'],
        invalidate_hard_deletes=True,
    )
}}

with deduped as (
    select
        customer_unique_id,
        -- um customer_unique_id pode ter vários customer_id (1 por pedido);
        -- consolidamos pegando o registro mais recente
        max(customer_id)            as customer_id,
        max(customer_city)          as customer_city,
        max(customer_state)         as customer_state,
        max(customer_zip_code_prefix) as customer_zip_code_prefix
    from {{ ref('stg_customers') }}
    where customer_unique_id is not null
    group by customer_unique_id
)

select * from deduped

{% endsnapshot %}
