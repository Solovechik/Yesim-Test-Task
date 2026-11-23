{{
    config(
        materialized    = 'table',
        engine          = 'MergeTree()',
        partition_by    = 'toYYYYMM(pdate)',
        order_by        = '(pdate, user_id, order_id)',
        tags            = ['mart', 'gold']
    )
}}

with
-- input -----------------------------------------------------------------------
    users as (
        select * from {{ ref('dim_users') }}
    ),

    orders as (
        select * from {{ ref('fct_orders') }}
    ),

    payments as (
        select * from {{ ref('fct_payments') }}
    ),

    plans as (
        select * from {{ ref('dim_plans') }}
    ),

    activations as (
        select * from {{ ref('fct_activations') }}
    ),

    usage_daily as (
        select * from {{ ref('fct_usage_daily') }}
    ),

    refunds as (
        select * from {{ ref('fct_refunds') }}
    ),

-- logic -----------------------------------------------------------------------
    clients as (
        select
            user_id,
            user_country
        from
            users
        where
            is_user_tester = 0
            and is_user_staff = 0
    ),

    success_payments as (
        select
            payment_uuid,
            payment_created_at,
            payment_amount_eur
        from
            payments
        where
            payment_status = 'success'
    ),

    paid_orders as (
        select
            orders.order_id                                     as order_id,
            user_id,
            payment_uuid,
            order_created_at,
            payment_created_at,
            plan_id,
            payment_amount_eur
        from
            orders
        left join
            success_payments                                    as payments
        using
            payment_uuid
        where
            orders.payment_uuid is null
            or payments.payment_uuid is not null
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
                is_plan_unlimited = 1,
                multiIf(
                    plan_days < 7, 'unlim_short',
                    plan_days = 7, 'unlim_mid',
                    'unlim_long'
                ),
                multiIf(
                    plan_size_gb <= 1, 'small',
                    plan_size_gb <= 10, 'mid',
                    'big'
                )
            )                                                   as plan_category
        from
            client_orders as orders
        left join
            plans
        using
            plan_id
    ),

    payment_refunds as (
        select
            payment_uuid,
            sum(refund_amount_eur)                              as refund_amount_eur
        from
            refunds
        group by
            payment_uuid
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
            payment_uuid
    ),

    activation_daily_usage as (
        select
            order_id,
            coalesce(
                sum(usage_cost_eur),
                toDecimal64(0, 2)
            )                                                   as cogs_eur
        from
            activations
        left join
            usage_daily                                         as usage
        using
            activation_id
        group by
            order_id
    ),

    client_orders_with_usage as (
        select
            orders.*,
            coalesce(
                cogs_eur,
                toDecimal64(0, 2)
            )                                                   as cogs_eur
        from
            client_orders_with_refunds                          as orders
        left join
            activation_daily_usage
        using
            order_id
    ),

    final as (
        with
            payment_uuid is null                                 as is_points,

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

            if(is_points, 0, payment_amount_eur)                as revenue_eur,

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
            payment_uuid                                        as payment_uid,
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
