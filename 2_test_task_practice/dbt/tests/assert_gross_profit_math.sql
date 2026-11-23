select
    order_id,
    revenue_eur,
    cogs_eur,
    gross_profit_eur
from
    {{ ref('mrt_revenue') }}
where
    gross_profit_eur != (revenue_eur - cogs_eur)
