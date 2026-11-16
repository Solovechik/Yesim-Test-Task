{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMM(pdate)',
        order_by='(pdate, user_id, order_id)',
        tags=['mart', 'gold']
    )
}}

with
-- input -----------------------------------------------------------------------
    users as (
        select * from {{ ref('stg_users') }}
    ),

    orders as (
        select * from {{ ref('stg_orders') }} final
    ),

    payments as (
        select * from {{ ref('stg_payments') }} final
    ),

    plans as (
        select * from {{ ref('stg_plans') }}
    ),

    activations as (
        select * from {{ ref('stg_activations') }} final
    ),

    usage_daily as (
        select * from {{ ref('stg_usage_daily') }} final
    ),

    refunds as (
        select * from {{ ref('stg_refunds') }} final
    ),

-- logic -----------------------------------------------------------------------
    clients as (
        select
            id                                                  as user_id,
            country                                             as user_country
        from
            users
        where
            is_tester = 0
            and is_staff = 0
    ),

    success_payment_last_state as (
        select
            payment_uid,
            created_at,
            amount_eur
        from
            payments
        where
            status = 'success'
        order by
            loaded_at desc
        limit 1 by
            payment_uid
    ),

    paid_orders as (
        select
            orders.id                                           as order_id,
            user_id,
            payment_uid,
            orders.created_at                                   as order_created_at,
            payments.created_at                                 as payment_created_at,
            plan_id,
            amount_eur
        from
            orders
        left join
            success_payment_last_state                          as payments
        using
            payment_uid
        where
            orders.payment_uid is null
            or payments.payment_uid is not null
    ),

    client_orders as (
        select
            *
        from
            paid_orders
        join
            clients
        using
            user_id
    ),

    client_orders_with_plans as (
        select
            *,
            if(
                plans.is_unlimited = 1,
                multiIf(
                    plans.days < 7, 'unlim_short',
                    plans.days = 7, 'unlim_mid',
                    'unlim_long'
                ),
                multiIf(
                    plans.size_gb <= 1, 'small',
                    plans.size_gb <= 10, 'mid',
                    'big'
                )
            )                                                   as plan_category
        from
            client_orders as orders
        left join
            plans
        on
            orders.plan_id = plans.id
    ),

    payment_refunds as (
        select
            payment_uid,
            sum(amount_eur)                                     as refund_amount_eur
        from
            refunds
        group by
            payment_uid
    ),

    client_orders_with_refunds as (
        select
            orders.*,
            coalesce(refunds.refund_amount_eur, 0)              as refund_eur
        from
            client_orders_with_plans                            as orders
        left join
            payment_refunds                                     as refunds
        using
            payment_uid
    ),

    activation_daily_usage as (
        select
            activations.id                                      as _activation_id,
            order_id,
            sum(usage.cost_eur)                                 as cogs_eur
        from
            activations
        left join
            usage_daily                                         as usage
        on
            activations.id = usage.activation_id
        group by
            _activation_id,
            order_id
    ),

    client_orders_with_usage as (
        select
            orders.*,
            cogs_eur
        from
            client_orders_with_refunds                          as orders
        left join
            activation_daily_usage
        using
            order_id
    ),

    final as (
        with
            payment_uid is null                                 as is_points,

            if(
                is_points,
                'points',
                'payment'
            )                                                   as source,

            if(
                is_points,
                order_created_at,
                payment_created_at
            )                                                   as paid_at,

            if(is_points, 0, amount_eur) as revenue_eur,

            min(
                if(
                    source = 'payment',
                    paid_at,
                    null
                )) over (partition by user_id)                  as first_paid_at,

            multiIf(
                source = 'payment'
                and first_paid_at is not null,
                date_diff(
                    'month',
                    toStartOfMonth(first_paid_at),
                    toStartOfMonth(paid_at)
                ) + 1,

                source = 'points',
                0,

                null
            )                                                   as month_since_first,

            if(
                source = 'payment',
                row_number() over (
                    partition by user_id, source
                    order by paid_at asc, order_id asc
                ),
                null
            )                                                   as purchase_number

        select
            order_id,
            user_id,
            payment_uid,
            source,
            paid_at,
            toDate(paid_at)                                     as pdate,
            plan_id,
            plan_category,
            user_country,
            revenue_eur,
            refund_eur,
            cogs_eur,
            revenue_eur - cogs_eur                              as gross_profit_eur,
            first_paid_at,
            toStartOfMonth(first_paid_at)                       as first_cohort,
            month_since_first,
            purchase_number
        from
            client_orders_with_usage
    )

select * from final
