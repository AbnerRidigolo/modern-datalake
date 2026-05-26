-- Falha se algum pedido entregue tiver delivery_days negativo
-- (entrega não pode ocorrer antes da aprovação do pedido).
select
    order_id,
    delivery_days
from {{ ref('fct_orders') }}
where delivery_days is not null
  and delivery_days < 0
