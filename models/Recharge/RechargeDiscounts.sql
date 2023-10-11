
{% if var('RechargeDiscounts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%recharge%discounts')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

    select
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'NA') as id	,		
        {{extract_nested_value("applies_to","purchase_item_type","string")}} as applies_to_purchase_item_type,
        {{extract_nested_value("api","can_apply","bool")}} as channel_settings_api,
        {{extract_nested_value("checkout_page","can_apply","bool")}} as channel_settings_checkout_page,
        {{extract_nested_value("customer_portal","can_apply","bool")}} as channel_settings_customer_portal,
        {{extract_nested_value("merchant_portal","can_apply","bool")}} as channel_settings_merchant_portal,
        a.code	,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,			
        a.status	,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,				
        {{extract_nested_value("usage_limits","automatic_redemptions_per_customer","numeric")}} as usage_limits_automatic_redemptions_per_customer,
        {{extract_nested_value("usage_limits","first_time_customer_restriction","bool")}} as usage_limits_first_time_customer_restriction,
        {{extract_nested_value("usage_limits","max_subsequent_redemptions","numeric")}} as usage_limits_max_subsequent_redemptions,
        {{extract_nested_value("usage_limits","one_application_per_customer","bool")}} as usage_limits_one_application_per_customer,
        value	,		
        value_type	,								
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    from {{i}} a
        {{unnesting("applies_to")}}
        {{unnesting("channel_settings")}}
        {{multi_unnesting("channel_settings","api")}}
        {{multi_unnesting("channel_settings","checkout_page")}}
        {{multi_unnesting("channel_settings","customer_portal")}}
        {{multi_unnesting("channel_settings","merchant_portal")}}
        {{unnesting("usage_limits")}}

            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
    qualify dense_rank() over (partition by id order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
