select
    order_id,
    source,
    payment_uid,
    first_paid_at,
    first_cohort,
    month_since_first,
    purchase_number
from
    {{ ref('mrt_revenue') }}
where
    source = 'payment'
    and (
        payment_uid is null
        or first_paid_at is null
        or first_cohort is null
        or month_since_first < 1
        or purchase_number < 1
    )
