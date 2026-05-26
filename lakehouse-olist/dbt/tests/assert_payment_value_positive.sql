-- Falha se algum pagamento tiver valor negativo.
-- Valor zero é permitido (ex.: pagamento via voucher integral).
select
    payment_key,
    order_id,
    payment_value
from {{ ref('stg_payments') }}
where payment_value < 0
