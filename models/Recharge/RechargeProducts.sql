
{% if var('RechargeProducts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('recharge_products_tbl_ptrn'),
exclude=var('recharge_products_tbl_exclude_ptrn'),
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

   /* SELECT * {{exclude()}} (row_num)
    From (*/
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
		cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,	
        discount_amount,		
        discount_type,		
        handle,		
        coalesce(cast(id as string),'NA') as id,		
        --images.large as images_large,
        {{extract_nested_value("Images","large","string")}} as images_large,
        {{extract_nested_value("Images","medium","string")}} as images_medium,
        {{extract_nested_value("Images","original","string")}} as images_original,
        {{extract_nested_value("Images","small","string")}} as images_small,
        -- images.medium as images_medium,		
        -- images.original as images_original,		
        -- images.small as images_small,			
        cast(product_id as string) as product_id,		
        cast(shopify_product_id as string) as shopify_product_id,	
        -- subscription_defaults.charge_interval_frequency	as subscription_defaults_charge_interval_frequency,		
        -- subscription_defaults.order_interval_frequency_options as subscription_defaults_order_interval_frequency_options,		
        -- subscription_defaults.order_interval_unit as subscription_defaults_order_interval_unit,		
        -- subscription_defaults.storefront_purchase_options as subscription_defaults_storefront_purchase_options,
        -- subscription_defaults.apply_cutoff_date_to_checkout as subscription_defaults_apply_cutoff_date_to_checkout,	
        {{extract_nested_value("subscription_defaults","charge_interval_frequency","numeric")}} as subscription_defaults_charge_interval_frequency,
        {{extract_nested_value("subscription_defaults","order_interval_frequency_options","string")}} as subscription_defaults_order_interval_frequency_options,
        {{extract_nested_value("subscription_defaults","order_interval_unit","string")}} as subscription_defaults_order_interval_unit,
        {{extract_nested_value("subscription_defaults","storefront_purchase_options","string")}} as subscription_defaults_storefront_purchase_options,
        {{extract_nested_value("subscription_defaults","apply_cutoff_date_to_checkout","bool")}} as subscription_defaults_apply_cutoff_date_to_checkout,

        title,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,				
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        /*DENSE_RANK() OVER (PARTITION BY id order by {{daton_batch_runtime()}} desc) row_num*/
        from {{i}} a 
            {{unnesting("images")}}
            {{unnesting("subscription_defaults")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            
    where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_products_lookback') }},0) from {{ this }})
        {% endif %}
        qualify dense_rank() over (partition by id order by {{daton_batch_runtime()}} desc)=1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
