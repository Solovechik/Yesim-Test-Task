select
    order_id,
    user_id
from
    {{ ref('mrt_revenue') }}
join
    {{ ref('dim_users') }}
using
    user_id
where
    is_user_tester = 1
    or is_user_staff = 1
