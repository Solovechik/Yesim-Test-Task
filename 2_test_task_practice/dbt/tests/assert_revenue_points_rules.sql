select
    order_id,
    source,
    payment_uid,
    revenue_eur,
    month_since_first,
    purchase_number
from
    {{ ref('mrt_revenue') }}
where
    source = 'points'
    and (
        payment_uid is not null
        or revenue_eur != 0
        or month_since_first != 0
        or purchase_number is not null
    )
