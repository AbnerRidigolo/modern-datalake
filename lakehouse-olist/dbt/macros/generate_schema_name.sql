{#
    Sobrescreve o comportamento padrão de nomenclatura de schema do dbt.

    Por padrão o dbt concatena o schema do profile com o schema custom
    (target_schema_custom_schema). Aqui usamos o schema custom diretamente
    (ex.: silver, gold) quando definido, mantendo nomes de camada limpos.
    Em targets que não sejam de produção, prefixamos com o schema do profile
    para isolar ambientes de desenvolvimento.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
