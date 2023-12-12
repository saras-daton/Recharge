
{% if var('RechargeProducts') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("recharge_products_tbl_ptrn",'%recharge%products',"recharge_products_tbl_exclude_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
		{{timezone_conversion("created_at")}} as created_at,
        discount_amount,		
        discount_type,		
        handle,		
        cast(id as string) as id,		
        {{extract_nested_value("Images","large","string")}} as images_large,
        {{extract_nested_value("Images","medium","string")}} as images_medium,
        {{extract_nested_value("Images","original","string")}} as images_original,
        {{extract_nested_value("Images","small","string")}} as images_small,			
        cast(product_id as string) as product_id,		
        cast(shopify_product_id as string) as shopify_product_id,		
        {{extract_nested_value("subscription_defaults","charge_interval_frequency","numeric")}} as subscription_defaults_charge_interval_frequency,
        {{extract_nested_value("subscription_defaults","order_interval_frequency_options","string")}} as subscription_defaults_order_interval_frequency_options,
        {{extract_nested_value("subscription_defaults","order_interval_unit","string")}} as subscription_defaults_order_interval_unit,
        {{extract_nested_value("subscription_defaults","storefront_purchase_options","string")}} as subscription_defaults_storefront_purchase_options,
        {{extract_nested_value("subscription_defaults","apply_cutoff_date_to_checkout","bool")}} as subscription_defaults_apply_cutoff_date_to_checkout,

        title,	
        {{timezone_conversion("updated_at")}} as updated_at,	
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
