
{% if var('RechargeOnetimes') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("recharge_onetimes_tbl_ptrn",'%recharge%onetimes',"recharge_onetimes_tbl_exclude_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,

        cast(id as string) as id	,		
        cast(address_id as string) as address_id	,		
        cast(customer_id as string) as customer_id	,	
        {{timezone_conversion("created_at")}} as created_at,
        {{extract_nested_value("external_product_id","ecommerce","string")}} as external_product_id_ecommerce,
        {{extract_nested_value("external_variant_id","ecommerce","string")}} as external_variant_id_ecommerce,
        next_charge_scheduled_at	,		
        price	,		
        product_title	,		
        cast(quantity as int64) as quantity	,		
        sku_override	,	
        {{timezone_conversion("updated_at")}} as updated_at,
        variant_title	,			
        is_cancelled	,		
        presentment_currency	,		
        sku	,					
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} 
            {{unnesting("external_product_id")}}
            {{unnesting("external_variant_id")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
  where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_onetimes_lookback') }},0) from {{ this }})
        {% endif %}
                qualify dense_rank() over (partition by id,customer_id,address_id order by {{daton_batch_runtime()}} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
