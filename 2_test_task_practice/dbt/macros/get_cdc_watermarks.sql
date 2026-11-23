{% macro get_cdc_watermarks(relation, version_col='_version', batch_dt_col='order_created_at') %}
    {% set default_result = {'max_ver': 0, 'max_batch_dt': '1970-01-01 00:00:00'} %}

    {% if not execute %}
        {{ return(default_result) }}
    {% endif %}

    {% set query %}
        select
            coalesce(max({{ version_col }}), 0) as max_ver,
            toString(coalesce(maxIf({{ batch_dt_col }}, {{ version_col }} = 0), toDateTime('1970-01-01 00:00:00'))) as max_batch_dt
        from {{ relation }}
    {% endset %}

    {% set res = run_query(query) %}

    {% if res and (res.rows | length) > 0 %}
        {% set first_row = res.rows | first %}
        {% set max_ver_val = first_row['max_ver'] %}
        {% set max_batch_dt_val = first_row['max_batch_dt'] %}
        {{ return({
            'max_ver': max_ver_val if max_ver_val is not none else 0,
            'max_batch_dt': max_batch_dt_val if max_batch_dt_val else '1970-01-01 00:00:00'
        }) }}
    {% else %}
        {{ return(default_result) }}
    {% endif %}
{% endmacro %}
