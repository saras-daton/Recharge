{% if var('RechargeDiscounts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('recharge_discounts_tbl_ptrn'),
exclude=var('recharge_discounts_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

 {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}
    /*SELECT * {{exclude()}} (row_num)
    From (*/
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'NA') as id	,		
        {{extract_nested_value("applies_to","purchase_item_type","string")}} as applies_to_purchase_item_type,
        --a.channel_settings	,	
        {{extract_nested_value("api","can_apply","bool")}} as channel_settings_api,
        {{extract_nested_value("checkout_page","can_apply","bool")}} as channel_settings_checkout_page,
        {{extract_nested_value("customer_portal","can_apply","bool")}} as channel_settings_customer_portal,
        {{extract_nested_value("merchant_portal","can_apply","bool")}} as channel_settings_merchant_portal,
        a.code	,		
        -- a.created_at	,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,			

        a.status	,		
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,			
        --usage_limits	,	
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
        /*DENSE_RANK() OVER (PARTITION BY id order by {{daton_batch_runtime()}} desc) row_num*/
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
           
    where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_discounts_lookback') }},0) from {{ this }})
        {% endif %}
            qualify dense_rank() over (partition by id order by {{daton_batch_runtime()}} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
